import boto3
import botocore
import json
import urllib.parse
import os
import io
import fitz  # PyMuPDF
import re
import time 
from datetime import datetime, timezone
from utils import execute_merge_logic, execute_validation


# --- Variáveis de Ambiente ---
MODEL_ID = os.environ.get('MODEL_ID')
REPORT_PREFIX = os.environ.get('REPORT_PREFIX')

try:
    s3_client = boto3.client('s3')
    bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
    print("Clientes Boto3 inicializados.")
except Exception as e:
    print(f"Erro ao inicializar clientes: {e}")
    bedrock_runtime = None
    s3_client = None

# --- Funções Helper ---
def get_text_from_pdf_bytes(pdf_bytes):
    full_text = ""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            for page in doc:
                full_text += page.get_text("text", sort=True) + "\n\n"
        return full_text
    except Exception as e:
        print(f"Erro ao extrair texto: {e}")
        raise e

def split_text_into_chunks(text, chunk_size=2000, chunk_overlap=200):
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start += (chunk_size - chunk_overlap)
    return chunks

def call_bedrock_llm(system_prompt, user_prompt, max_tokens=100, temperature=0.0):
    messages = [{"role": "user", "content": [{"text": user_prompt}]}]
    system_prompts = [{"text": system_prompt}]
    
    max_retries = 3
    base_delay_segundos = 2
    
    for attempt in range(max_retries):
        try:            
            response = bedrock_runtime.converse(
                modelId=MODEL_ID, 
                messages=messages,
                system=system_prompts,
                inferenceConfig={"maxTokens": max_tokens, "temperature": temperature}
            )
            return response["output"]["message"]["content"][0]["text"], 0, 0 

        except botocore.exceptions.ClientError as error:
            error_code = error.response['Error']['Code']
            if error_code in ['ModelErrorException', 'ThrottlingException', 'InternalServerException']:
                print(f"AVISO: Erro transiente ({error_code}). Tentativa {attempt + 1}.")
                if attempt + 1 == max_retries: raise error
                time.sleep(base_delay_segundos * (attempt + 1))
            else:
                raise error
        except Exception as e:
            if attempt + 1 == max_retries: raise e
            time.sleep(base_delay_segundos * (attempt + 1))
            
    raise Exception("Falha Bedrock.")

def _limpar_json(json_string):
    if "```json" in json_string:
        json_string = json_string.split("```json")[1].split("```")[0].strip()
    if not json_string.startswith("{"):
        start_index = json_string.find('{')
        if start_index != -1:
            json_string = json_string[start_index:]
    return json_string

def get_dados_extraidos_schema():
    # SCHEMA 
    schema = {
        "tipo_documento": "string (ex: 'Termo de Securitização')",
        "numero_emissao": "string (EXTRACT EXACTLY. Do not invent. ex: '522')",
        "isin": "string (Código ISIN, se disponível, ex: 'BRRBRACRIY12')",
        "numero_processo": "string (EXTRACT EXACTLY. CVM Process Number. ex: 'SRE/0001/2023')",
        
        "securitizadora": {
            "nome": "string (Nome da companhia)",
            "cnpj": "string (EXTRACT EXACTLY. XX.XXX.XXX/XXXX-XX)" 
        },
        "devedor": { 
            "nome": "string (Nome/Razão Social do devedor/cedente)",
            "cnpj": "string (XX.XXX.XXX/XXXX-XX)",
            "endereco": "string (Endereço completo, se disponível)",
            "cidade": "string",
            "estado": "string"
        },
        "agente_fiduciario": "string (Nome da instituição)",
        "auditor": "string (Nome do auditor da operação)",
        "agencia_rating": "string (Nome da agência de rating, ex: 'S&P')",
        "rating_emissao": "string (A nota/classificação de risco, ex: 'AAA(br)')",

        # --- Características da Emissão ---
        "volume_total": "number (Valor monetário total da emissão, ex: 20000000.0)",
        "destinacao_recursos": "string (Finalidade dos recursos)",
        "categoria_anbima": "string (ex: 'Residencial, corporativo')",
        "segmento_anbima": "string (ex: 'Apartamentos')",
        
        # --- Lastro e Garantias ---
        "natureza_creditos": "string (ex: 'Créditos Imobiliários', 'Créditos do Agronegócio')",
        "criterios_elegibilidade": "string (Resumo das regras que definem os créditos)",
        "garantias": "string (Descrição das garantias da operação, ex: Hipoteca, Alienação Fiduciária)",
        "mecanismos_reforco_credito": "string (Descrição de Fundo de Reserva, Sobrecolateralização, etc.)",
        "indice_subordinacao": "string (Se aplicável, ex: '10%')",
        
        # (Campos de texto livre)
        "estrutura_lastro_garantia": "string (Resumo da estrutura)",
        "estrutura_pagamentos_covenants": "string (Resumo da estrutura)",
        
        # --- Títulos (Séries) ---
        "amortizacao_resgate": "string (Resumo de como o principal será pago)",
        "eventos_vencimento_antecipado": "string (Resumo das situações que antecipam o vencimento)",
        
        "series": [ 
            {
                "nome": "string (Nome da série, ex: '1ª Série')",
                "volume": "number (Valor monetário da série)",
                "taxa_remuneracao": "number (Valor da taxa, ex: 32.25)",
                "indexador_taxa_remuneracao": "string (ex: 'Fixa', 'DI (d-4)')",
                "data_vencimento": "string (YYYY-MM-DD)",
                "data_emissao": "string (YYYY-MM-DD)"
            }
        ],
        
        # --- Regulatório ---
        "registro_cvm_autorizacoes": "string (O número de registro na CVM ou outra autoridade)",
        "legislacao_aplicavel": "string (ex: 'Lei nº 9.514/1997', 'Normas da CVM')"
    }
    return json.dumps(schema, indent=2, ensure_ascii=False)


# --- HANDLER PRINCIPAL ---
def lambda_handler(event, context):
    print("Iniciando Lambda...")
    
    if not bedrock_runtime or not s3_client:
        return {"status": "error", "reason": "Boto3 failed"}

    try:
        # 1. Obter Arquivo
        s3_record = event['Records'][0]['s3']
        bucket_name = s3_record['bucket']['name']
        object_key = urllib.parse.unquote_plus(s3_record['object']['key'], encoding='utf-8')
        file_name_only = os.path.basename(object_key)
        print(f"Processando: s3://{bucket_name}/{object_key}")
        
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        document_text = get_text_from_pdf_bytes(file_obj['Body'].read())
        chunks = split_text_into_chunks(document_text)

        # 4. Map
        print("Iniciando 'Map'...")
        summaries_list = []
        
        system_prompt_map = """
        You are a summarization assistant. Your task is to summarize the text chunk.
        
        MANDATORY - HUNT FOR IDENTIFIERS:
        Scan the text specifically for these values. If found, WRITE THEM EXACTLY in the summary:
        
        1. **CNPJ:** Look for format "XX.XXX.XXX/XXXX-XX".
        2. **Issuance Number:** Look for "Emissão nº X", "Xª Emissão".
        3. **Process Number (CRITICAL):** - Look for codes starting with **"SRE/"**, **"RJ/"**, **"SP/"**.
           - Look for labels like **"Processo"**, **"Protocolo"**, **"Registro"**, **"Autos"**, **"CVM"**.
           - Look for patterns like "CVM/SRE/..." or "SRE/XXXX/XXXX".
           - If found, COPY EXACTLY.
        
        For all other financial data (Series, Rates, Guarantees), provide a concise summary.
        If the chunk is irrelevant, return 'N/A'.
        """

        for i, chunk in enumerate(chunks):
            user_prompt_map = f"<contexto>\n{chunk}\n</contexto>\n\nResuma este trecho."
            
            # --- IMPRIME ANTES DE CHAMAR (Para debug de travamento) ---
            print(f"Log: Processando Chunk {i+1}/{len(chunks)}...", end="\r") 
            
            summary_text, _, _ = call_bedrock_llm(system_prompt_map, user_prompt_map, max_tokens=256)
            
            if 'N/A' not in summary_text:
                summaries_list.append(summary_text)
        
        print(f"\nSumarização concluída. {len(summaries_list)} sumários relevantes gerados.")
        super_summary_context = "\n\n--- SUMÁRIO ---\n\n".join(summaries_list)

        # 5. Reduce
        print("Iniciando 'Reduce'...")
        schema_str = get_dados_extraidos_schema()
        
        system_prompt_reduce = f"""
        You are a financial assistant. Extract data from the summary to fill the JSON schema.
        
        RULES:
        1. Try to fill ALL fields in the schema (Volume, Series, Dates, etc.) based on the summary.
        2. **CRITICAL IDs (CNPJ, Emissao, Processo):**
           - Look for them in the summary.
           - If found, extract exactly.
           - If NOT found, return `null`. **DO NOT INVENT.**
        
        Respond with ONLY the JSON object.
        <schema>{schema_str}</schema>
        """
        user_prompt_reduce = f"<contexto_sumarizado>\n{super_summary_context}\n</contexto_sumarizado>\n\nExtraia o JSON."
        
        json_string_answer, _, _ = call_bedrock_llm(system_prompt_reduce, user_prompt_reduce, max_tokens=8192)
        
        try:
            dados_extraidos_json = json.loads(_limpar_json(json_string_answer))
        except json.JSONDecodeError:
            print("Erro: JSON inválido do Bedrock.")
            return {"status": "error", "reason": "Invalid JSON"}

        # 6. Merge
        original_directory = os.path.dirname(object_key)
        output_directory = os.path.join(original_directory, "output")
        
        json_para_salvar, merged_with_key = execute_merge_logic(
            bedrock_runtime, MODEL_ID, s3_client, bucket_name, output_directory, dados_extraidos_json 
        )
        
        if not merged_with_key:
            json_para_salvar = dados_extraidos_json 

        # --- 7. VALIDAÇÃO CONDICIONAL ---
        print("Verificando pré-requisitos para Validação CVM...")
        
        numero_proc_valor = json_para_salvar.get("numero_processo")
        tem_processo = bool(numero_proc_valor and str(numero_proc_valor).strip())

        if tem_processo:
            print(f"Log: Processo '{numero_proc_valor}' encontrado. Validando...")
            resultado_validacao_completo = execute_validation(json_para_salvar)
            status_final = resultado_validacao_completo.get("status")
            print(f"Log: Validação concluída. Status: {status_final}")
        else:
            print("Log: Número do Processo ausente. Status definido como PENDENTE.")
            status_final = "PENDENTE"
            resultado_validacao_completo = {
                "status": "PENDENTE",
                "timestamp_validacao": datetime.now(timezone.utc).isoformat(),
                "motivo": "Aguardando número de registro CVM (presente em Anúncio ou Aditamento)."
            }

        # --- 8. SALVAMENTO ---
        data_extracao_obj = datetime.now(timezone.utc)
        timestamp_str = data_extracao_obj.strftime("%Y%m%d_%H%M%S")
        base_filename = os.path.splitext(file_name_only)[0]

        json_para_salvar['validacao_cvm'] = resultado_validacao_completo

        if status_final == "REPROVADA" and REPORT_PREFIX:
            try:
                report_key = os.path.join(REPORT_PREFIX, f"{base_filename}_divergencia_{timestamp_str}.json")
                report_data = {
                    "arquivo_origem": file_name_only,
                    "data_reprovacao": data_extracao_obj.isoformat(),
                    "relatorio_completo": resultado_validacao_completo
                }
                s3_client.put_object(
                    Bucket=bucket_name, Key=report_key,
                    Body=json.dumps(report_data, ensure_ascii=False, indent=2),
                    ContentType='application/json'
                )
                print(f"Relatório de divergência salvo: {report_key}")
            except Exception as e:
                print(f"Erro ao salvar relatório: {e}")

        tipo_doc = dados_extraidos_json.get("tipo_documento", "documento_desconhecido")
        final_json_data = {
            "arquivo_origem": file_name_only,
            "tipo_documento": tipo_doc,
            "data_extracao": data_extracao_obj.isoformat(),
            "dados_extraidos": json_para_salvar,
            "merge_info": {"merged_with_file": merged_with_key} if merged_with_key else None
        }

        json_filename = f"{base_filename}_{timestamp_str}.json"
        output_key = os.path.join(output_directory, json_filename)

        s3_client.put_object(
            Bucket=bucket_name, Key=output_key,
            Body=json.dumps(final_json_data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        
        print(f"JSON salvo: s3://{bucket_name}/{output_key}")
        return {
            "status": "success", 
            "output_key": output_key, 
            "merged": (merged_with_key is not None),
            "validation_status": status_final 
        }

    except Exception as e:
        print(f"Erro Handler: {e}")
        raise e