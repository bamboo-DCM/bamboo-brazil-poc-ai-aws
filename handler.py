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
    """Extrai o TEXTO PURO de um PDF usando PyMuPDF (fitz)."""
    print("Extraindo texto puro do PDF (bytes) com PyMuPDF...")
    full_text = ""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            print(f"PDF tem {len(doc)} páginas.")
            for page in doc:
                full_text += page.get_text("text", sort=True) + "\n\n"
        print(f"Extração de texto concluída. Total de {len(full_text)} caracteres.")
        return full_text
    except Exception as e:
        print(f"Erro ao extrair texto do PDF com PyMuPDF: {e}")
        raise e

def split_text_into_chunks(text, chunk_size=2000, chunk_overlap=200):
    """Divide um texto longo em chunks com sobreposição."""
    print(f"Dividindo texto em chunks de ~{chunk_size} caracteres...")
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start += (chunk_size - chunk_overlap)
    print(f"Texto dividido em {len(chunks)} chunks.")
    return chunks

def call_bedrock_llm(system_prompt, user_prompt, max_tokens=100, temperature=0.0):
    """
    Função genérica para chamar o Bedrock, usando client.converse()
    e com lógica de retentativa (retry) para erros transientes.
    """
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
            
            response_text = response["output"]["message"]["content"][0]["text"]
            
            usage = response.get('usage', {})
            in_tokens = usage.get('inputTokens', 0)
            out_tokens = usage.get('outputTokens', 0)
            
            return response_text, in_tokens, out_tokens

        except botocore.exceptions.ClientError as error:
            error_code = error.response['Error']['Code']
            if error_code == 'ModelErrorException' or error_code == 'ThrottlingException' or error_code == 'InternalServerException':
                print(f"AVISO: Erro transiente detectado ({error_code}). Tentativa {attempt + 1}/{max_retries}. Aguardando para tentar novamente...")
                if attempt + 1 == max_retries:
                    print(f"ERRO: Máximo de retentativas ({max_retries}) atingido. Desistindo.")
                    raise error
                
                time.sleep(base_delay_segundos * (attempt + 1))
            else:
                print(f"ERRO: Erro não transiente do Boto3: {error}")
                raise error
        except Exception as e:
            print(f"Erro inesperado ao invocar modelo: {e}")
            if attempt + 1 == max_retries:
                raise e
            time.sleep(base_delay_segundos * (attempt + 1))
            
    raise Exception("Falha ao chamar o Bedrock após múltiplas tentativas.")


def _limpar_json(json_string):
    """Tenta limpar a resposta do LLM para garantir que seja um JSON válido."""
    if "```json" in json_string:
        json_string = json_string.split("```json")[1].split("```")[0].strip()
    if not json_string.startswith("{"):
        start_index = json_string.find('{')
        if start_index != -1:
            end_index = json_string.rfind('}')
            if end_index > start_index:
                json_string = json_string[start_index:end_index+1]
            else:
                json_string = json_string[start_index:]
    return json_string

def get_dados_extraidos_schema():
    """
    Retorna o JSON-SCHEMA para o objeto 'dados_extraidos', 
    combinando o formato do cliente com os campos qualitativos.
    """
    schema = {
        "tipo_documento": "string (ex: 'Termo de Securitização')",
        "numero_emissao": "string (Número da emissão, ex: '522')",
        "isin": "string (Código ISIN, se disponível, ex: 'BRRBRACRIY12')",
        "numero_processo": "string (Número do processo CVM, ex: 'SRE/0001/2023')",
        
        # --- Partes Envolvidas ---
        "securitizadora": {
            "nome": "string (Nome da companhia)",
            "cnpj": "string (XX.XXX.XXX/XXXX-XX)"
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


# --- O HANDLER PRINCIPAL (Sumarização Hierárquica) ---
def lambda_handler(event, context):
    """
    Handler principal do Lambda.
    1. Executa o MapReduce no arquivo de entrada.
    2. Executa o Merge (se aplicável).
    3. Executa a Validação CVM.
    4. Salva o JSON principal (modificado) e, condicionalmente, 
       salva o relatório de divergência no mesmo bucket, em um prefixo diferente.
    """

    print("Iniciando a função Lambda")
    
    if not bedrock_runtime or not s3_client:
        print("Erro: Clientes Boto3 não foram inicializados.")
        return {"status": "error", "reason": "Boto3 clients failed to init"}

    try:
        # 1. Obter e Baixar o arquivo S3
        s3_record = event['Records'][0]['s3']
        bucket_name = s3_record['bucket']['name']
        object_key = urllib.parse.unquote_plus(s3_record['object']['key'], encoding='utf-8')
        file_name_only = os.path.basename(object_key)
        print(f"Processando arquivo: s3://{bucket_name}/{object_key}")
        
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_bytes = file_obj['Body'].read()
        
        # 2. (Load) Extrair texto
        document_text = get_text_from_pdf_bytes(file_bytes)
        
        # 3. (Split) Criar a lista de Chunks
        chunks = split_text_into_chunks(document_text)

        # 4. (Map) Iterar, Sumarizar e Concatenar
        print("Iniciando 'Map' para sumarizar chunks...")
        summaries_list = []
        system_prompt_map = """
        You are a summarization assistant. Your task is to read the text excerpt and
        summarize it into a few key points. Focus on:
        - Company names (Securitizadora, Debtor, Assignor, Fiduciary Agent, Auditor, Rating Agency)
        - Financial data (Values, rates, volumes)
        - Dates (Issuance, Maturity)
        - Identifiers (ISIN, CVM, Issuance Number)
        - Security characteristics (Series, guarantees, amortization, credit enhancement, covenants)
        - Rules (Eligibility criteria, legislation)
        If the excerpt is irrelevant (e.g., index, blank page), return 'N/A'.
        """

        total_tokens_usados = {"input": 0, "output": 0}
        
        for i, chunk in enumerate(chunks):
            user_prompt_map = f"<contexto>\n{chunk}\n</contexto>\n\nSumarize os pontos principais deste contexto."
            summary_text, in_tok, out_tok = call_bedrock_llm(
                system_prompt_map, user_prompt_map, max_tokens=256, temperature=0.0
            )
            print(f"Log: Passo='Map-Summarize', Chunk={i+1}/{len(chunks)}, InTokens={in_tok}, OutTokens={out_tok}")
            total_tokens_usados["input"] += in_tok
            total_tokens_usados["output"] += out_tok
            if 'N/A' not in summary_text:
                summaries_list.append(summary_text)
        
        print(f"Sumarização concluída. {len(summaries_list)} sumários relevantes gerados.")
        super_summary_context = "\n\n--- NOVO SUMÁRIO ---\n\n".join(summaries_list)

        # 5. (Reduce) Extrair o JSON do arquivo ATUAL
        print("Iniciando 'Reduce' para extrair JSON do arquivo atual...")
        schema_str = get_dados_extraidos_schema()
        system_prompt_reduce = f"""
            You are an assistant specialized in analyzing financial documents.
            Your task is to extract data from a document summary and format it
            as JSON, according to the provided schema. Respond with ONLY the JSON.
        <schema>{schema_str}</schema>
        """
        user_prompt_reduce = f"<contexto_sumarizado>\n{super_summary_context}\n</contexto_sumarizado>\n\nExtraia os dados deste contexto."
        
        json_string_answer, in_tok, out_tok = call_bedrock_llm(
            system_prompt_reduce, user_prompt_reduce, max_tokens=8192, temperature=0.0
        )
        
        print(f"Log: Passo='Reduce-Extract', InTokens={in_tok}, OutTokens={out_tok}")
        total_tokens_usados["input"] += in_tok
        total_tokens_usados["output"] += out_tok
        print("Extração MapReduce concluída.")

        try:
            dados_extraidos_json = json.loads(_limpar_json(json_string_answer))
        except json.JSONDecodeError:
            print("Erro: Bedrock retornou uma string que NÃO é um JSON válido (Fase Reduce).")
            print(f"--- Resposta Bruta Recebida --- \n{json_string_answer}\n--- Fim da Resposta ---")
            return {"status": "error", "reason": "Resposta do Reduce não é um JSON válido."}

        # --- 6. NOVA LÓGICA DE VERIFICAÇÃO E MERGE ---
        
        original_directory = os.path.dirname(object_key)
        output_directory = os.path.join(original_directory, "output")
        
        # Esta função retorna o JSON *final* e a key do arquivo com o qual fez o merge
        json_para_salvar, merged_with_key = execute_merge_logic(
            bedrock_runtime,
            MODEL_ID,
            s3_client,
            bucket_name,
            output_directory,
            dados_extraidos_json 
        )
        
        if merged_with_key:
            print(f"Log: Merge concluído. Usando dados mesclados.")
        else:
            print(f"Log: Salvamento direto. Usando dados extraídos.")
            json_para_salvar = dados_extraidos_json 
            

        # --- 7.LÓGICA DE VALIDAÇÃO (Pós-Merge) ---
        print("Iniciando Módulo de Validação CVM...")

        resultado_validacao_completo = execute_validation(json_para_salvar)
        
        status_final = resultado_validacao_completo.get("status")
        
        print(f"Log: Validação concluída. Status: {status_final}")


        # --- 8. LÓGICA DE SALVAMENTO (Condicional Atualizada) ---
        data_extracao_obj = datetime.now(timezone.utc)
        timestamp_str = data_extracao_obj.strftime("%Y%m%d_%H%M%S")
        base_filename = os.path.splitext(file_name_only)[0]

        # 8a. Modificar o JSON principal (adiciona o "carimbo" de status)
        json_para_salvar['validacao_cvm'] = {"status": status_final}

        # 8b. Salvar relatório de divergência (se REPROVADO)
        if status_final == "REPROVADA":
            if not REPORT_PREFIX:
                print("AVISO: Status REPROVADA, mas a variável REPORT_PREFIX não está definida. Relatório de divergência não será salvo.")
            else:
                try:
                    report_filename = f"{base_filename}_divergencia_{timestamp_str}.json"
                    report_output_key = os.path.join(REPORT_PREFIX, report_filename)
                    
                    report_data = {
                        "arquivo_origem": file_name_only,
                        "data_reprovacao": data_extracao_obj.isoformat(),
                        "relatorio_completo": resultado_validacao_completo
                    }
                    
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=report_output_key,
                        Body=json.dumps(report_data, ensure_ascii=False, indent=2),
                        ContentType='application/json'
                    )
                    print(f"Relatório de divergência salvo em: s3://{bucket_name}/{report_output_key}")
                except Exception as e:
                    print(f"ERRO CRÍTICO ao salvar relatório de divergência: {e}")


        # 8c. Salvar o JSON principal
      
        tipo_documento_novo_arquivo = dados_extraidos_json.get("tipo_documento", "documento_desconhecido")
        
        final_json_data = {
            "arquivo_origem": file_name_only,
            "tipo_documento": tipo_documento_novo_arquivo,
            "data_extracao": data_extracao_obj.isoformat(),
            "dados_extraidos": json_para_salvar, # 'json_para_salvar' é o JSON mesclado
            "merge_info": {
                "merged_with_file": merged_with_key
            } if merged_with_key else None
        }

        # Define o caminho de saída principal
        json_filename = f"{base_filename}_{timestamp_str}.json"
        output_key = os.path.join(output_directory, json_filename)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=output_key,
            Body=json.dumps(final_json_data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        
        print(f"JSON principal salvo com sucesso em: s3://{bucket_name}/{output_key}")
        
        return {
            "status": "success", 
            "output_key": output_key, 
            "merged": (merged_with_key is not None),
            "validation_status": status_final 
        }

    except Exception as e:
        print(f"Ocorreu um erro inesperado no handler: {type(e).__name__} - {e}")
        raise e

# --- Executor de Teste Local ---
if __name__ == "__main__":
    import sys
    import json
    import pprint

    print("--- INICIANDO TESTE LOCAL ---")
    
    if len(sys.argv) > 1:
        event_file_path = sys.argv[1]
    else:
        event_file_path = "mock_event.json"
    
    print(f"Carregando evento de: {event_file_path}")

    try:
        with open(event_file_path, 'r') as f:
            event_data = json.load(f)
        
        mock_context = {}
        
        print("Chamando o lambda_handler...")
        response = lambda_handler(event_data, mock_context)
        
        print("--- EXECUÇÃO CONCLUÍDA ---")
        print("\nResposta Final do Handler:")
        pprint.pprint(response)
        
        if response.get("status") == "success":
            print(f"\n✅ SUCESSO! Verifique o arquivo em s3://{event_data['Records'][0]['s3']['bucket']['name']}/{response['output_key']}")

    except FileNotFoundError:
        print(f"Erro: Arquivo de evento '{event_file_path}' não encontrado.")
    except json.JSONDecodeError:
        print(f"Erro: Arquivo de evento '{event_file_path}' não contém um JSON válido.")
    except botocore.exceptions.ClientError as e:
        print(f"\n--- ERRO BOTO3 (Após retentativas) ---")
        print(e)
    except Exception as e:
        print(f"Erro inesperado durante a execução local: {e}")
        import traceback
        traceback.print_exc()