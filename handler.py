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

# --- Configuração Global ---
MODELO_ID_MICRO = "amazon.nova-lite-v1:0" 

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
                modelId=MODELO_ID_MICRO, 
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

# --- FUNÇÃO DE SCHEMA ATUALIZADA ---
def get_dados_extraidos_schema():
    """
    Retorna o JSON-SCHEMA para o objeto 'dados_extraidos', 
    combinando o formato do cliente com os campos qualitativos.
    """
    schema = {
        "tipo_documento": "string (ex: 'Termo de Securitização')",
        "numero_emissao": "string (Número da emissão, ex: '522')",
        "isin": "string (Código ISIN, se disponível, ex: 'BRRBRACRIY12')",
        
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
        
        # --- Campos de texto livre  ---
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
    Handler principal do Lambda. Espera um evento de S3 (ObjectCreated).
    Implementa a lógica de Sumarização Hierárquica (Map-Reduce).
    """
    print("Iniciando a função Lambda (Sumarização Hierárquica)...")
    
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

        # (Debug de Chunks - mantido da sua solicitação anterior)
        print("Salvando os 5 primeiros chunks em 'debug_chunks.txt'...")
        try:
            with open("debug_chunks.txt", "w", encoding="utf-8") as f:
                for i, chunk in enumerate(chunks[:5]):
                    f.write(f"--- CHUNK {i+1} (Início) ---\n")
                    f.write(chunk)
                    f.write(f"\n--- CHUNK {i+1} (Fim) ---\n\n")
            print("Arquivo 'debug_chunks.txt' salvo com sucesso.")
        except Exception as e:
            print(f"AVISO: Falha ao salvar arquivo de debug: {e}")

        # 4. (Map) Iterar, Sumarizar e Concatenar
        print("Iniciando 'Map' para sumarizar chunks...")
        summaries_list = []
        
        # --- PROMPT DE SUMARIZAÇÃO (---
        system_prompt_map = """
        Você é um assistente de sumarização. Sua tarefa é ler o trecho de texto e
        sumarizá-lo em alguns pontos principais. Foque em:
        - Nomes de empresas (Securitizadora, Devedor, Cedente, Agente Fiduciário, Auditor, Agência de Rating)
        - Dados financeiros (Valores, taxas, volumes)
        - Datas (Emissão, Vencimento)
        - Identificadores (ISIN, CVM, Nº da Emissão)
        - Características dos títulos (Séries, garantias, amortização, reforço de crédito, covenants)
        - Regras (Critérios de elegibilidade, legislação)
        Se o trecho for irrelevante (ex: índice, página em branco), retorne 'N/A'.
        """
        
        total_tokens_usados = {"input": 0, "output": 0}
        
        for i, chunk in enumerate(chunks):
            user_prompt_map = f"<contexto>\n{chunk}\n</contexto>\n\nSumarize os pontos principais deste contexto."
            
            summary_text, in_tok, out_tok = call_bedrock_llm(
                system_prompt_map, 
                user_prompt_map, 
                max_tokens=256, 
                temperature=0.0
            )
            
            print(f"Log: Passo='Map-Summarize', Chunk={i+1}/{len(chunks)}, InTokens={in_tok}, OutTokens={out_tok}")
            total_tokens_usados["input"] += in_tok
            total_tokens_usados["output"] += out_tok

            if 'N/A' not in summary_text:
                summaries_list.append(summary_text)
        
        print(f"Sumarização concluída. {len(summaries_list)} sumários relevantes gerados.")
        
        super_summary_context = "\n\n--- NOVO SUMÁRIO ---\n\n".join(summaries_list)

        # 5. (Reduce) Extrair o JSON final 
        print("Iniciando 'Reduce' para extrair JSON do super-sumário...")
        
        schema_str = get_dados_extraidos_schema()
        
        system_prompt_reduce = f"""
        Você é um assistente especialista em análise de documentos financeiros.
        Sua tarefa é extrair dados de um *sumário* de documento e formatá-los 
        em JSON, de acordo com o schema fornecido.
        
        Formato de Saída (JSON Schema):
        <schema>
        {schema_str}
        </schema>
        
        Responda *APENAS* com o JSON preenchido (o objeto 'dados_extraidos'). 
        Não inclua '```json', introduções ou explicações.
        Como você está lendo um sumário, é normal que muitos campos fiquem 'null'.
        """
        
        user_prompt_reduce = f"<contexto_sumarizado>\n{super_summary_context}\n</contexto_sumarizado>\n\nExtraia os dados deste contexto."
        
        json_string_answer, in_tok, out_tok = call_bedrock_llm(
            system_prompt_reduce,
            user_prompt_reduce,
            max_tokens=8192, 
            temperature=0.0
        )
        
        print(f"Log: Passo='Reduce-Extract', InTokens={in_tok}, OutTokens={out_tok}")
        total_tokens_usados["input"] += in_tok
        total_tokens_usados["output"] += out_tok
        
        print(f"--- Extração Concluída ---")
        print(f"Observabilidade Total: InputTokens={total_tokens_usados['input']}, OutputTokens={total_tokens_usados['output']}")

        # 6. Salva o JSON final de volta no S3
        
        try:
            cleaned_json = _limpar_json(json_string_answer)
            dados_extraidos_json = json.loads(cleaned_json)
        except json.JSONDecodeError:
            print("Erro: Bedrock retornou uma string que NÃO é um JSON válido.")
            print(f"--- Resposta Bruta Recebida --- \n{json_string_answer}\n--- Fim da Resposta ---")
            return {"status": "error", "reason": "Resposta do Reduce não é um JSON válido."}

        # Cria o "wrapper" de metadata
        final_json_data = {
            "arquivo_origem": file_name_only,
            "tipo_documento": "termo_securitizacao",
            "data_extracao": datetime.now(timezone.utc).isoformat(),
            "dados_extraidos": dados_extraidos_json, # Aninha o resultado do LLM
            "merge_info": None # Define como nulo
        }

        # Constrói o caminho de saída
        output_key_json = object_key.replace(".pdf", ".json")
        file_name = os.path.basename(output_key_json)
        output_key = f"output/{file_name}"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=output_key,
            Body=json.dumps(final_json_data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        
        print(f"JSON salvo com sucesso em: s3://{bucket_name}/{output_key}")
        return {"status": "success", "output_key": output_key}

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