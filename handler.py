
import boto3
import botocore
import json
import urllib.parse
import os
import io
import fitz 
import re
import time 

MODELO_ID_MICRO = "amazon.nova-lite-v1:0" 

try:
    s3_client = boto3.client('s3')
    bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
    print("Clientes Boto3 inicializados.")
except Exception as e:
    print(f"Erro ao inicializar clientes: {e}")
    bedrock_runtime = None
    s3_client = None


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

def get_full_json_schema():
    """Retorna o JSON-SCHEMA COMPLETO, com todas as seções aninhadas."""
    full_schema = {
        "Identificação e Informações Gerais": {
            "Denominação da emissão": "string", "Data de emissão": "string (YYYY-MM-DD)",
            "Securitizadora": {"Nome": "string", "CNPJ": "string", "Sede": "string"},
            "Cedente(s)": "string", "Agente fiduciário": "string", "Auditor": "string",
            "Custodiante": "string", "Rating": "string"
        },
        "Descrição dos Créditos Securitizados": {
            "Natureza dos créditos": "string", "Origem dos créditos": "string",
            "Valor total da carteira": "number", "Fluxo esperado": "string",
            "Critérios de elegibilidade": "string", "Garantias associadas": "string",
            "Procedimento de substituição": "string"
        },
        "Características dos Títulos Emitidos": {
            "Quantidade e valor nominal": "string", "Forma e espécie": "string",
            "Prazo e data de vencimento": "string", "Remuneração": "string",
            "Forma e local de pagamento": "string", "Amortização e resgate": "string",
            "Eventos de vencimento antecipado": "string", "Ranking (waterfall)": "string"
        },
        "Obrigações e Direitos das Partes": {
            "Obrigações da secururitizadora": "string", "Direitos dos investidores": "string",
            "Função do agente fiduciário": "string", "Mecanismos de reforço": "string",
            "Destinação dos recursos": "string"
        },
        "Informações Regulatórias e Contábeis": {
            "Registros e autorizações": "string", "Tributação aplicável": "string",
            "Demonstrações financeiras": "string"
        },
        "Disposições Gerais": {
            "Legislação aplicável": "string", "Foro": "string", "Assinaturas": "string"
        }
    }
    return json.dumps(full_schema, indent=2, ensure_ascii=False)


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
        print(f"Processando arquivo: s3://{bucket_name}/{object_key}")
        
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_bytes = file_obj['Body'].read()
        
        # 2. (Load) Extrair texto
        document_text = get_text_from_pdf_bytes(file_bytes)
        
        # 3. (Split) Criar a lista de Chunks
        chunks = split_text_into_chunks(document_text)

        # --- INÍCIO DA MODIFICAÇÃO (DEBUG DE CHUNKS) ---
        print("Salvando os 5 primeiros chunks em 'debug_chunks.txt'...")
        try:
            with open("debug_chunks.txt", "w", encoding="utf-8") as f:
                for i, chunk in enumerate(chunks[:5]): # Pega os 5 primeiros
                    f.write(f"--- CHUNK {i+1} (Início) ---\n")
                    f.write(chunk)
                    f.write(f"\n--- CHUNK {i+1} (Fim) ---\n\n")
            print("Arquivo 'debug_chunks.txt' salvo com sucesso.")
        except Exception as e:
            print(f"AVISO: Falha ao salvar arquivo de debug: {e}")
        # --- FIM DA MODIFICAÇÃO ---

        # 4. (Map) Iterar, Sumarizar e Concatenar
        print("Iniciando 'Map' para sumarizar chunks...")
        summaries_list = []
        
        system_prompt_map = """
        Você é um assistente de sumarização. Sua tarefa é ler o trecho de texto e
        sumarizá-lo em alguns pontos principais. Foque em dados financeiros, 
        nomes de empresas (Securitizadora, Cedente, Agente Fiduciário), 
        datas, valores, obrigações e características dos títulos.
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

        # 5. (Reduce) Extrair o JSON final do "super-sumário"
        print("Iniciando 'Reduce' para extrair JSON do super-sumário...")
        
        full_schema_str = get_full_json_schema()
        
        system_prompt_reduce = f"""
        Você é um assistente especialista em análise de documentos financeiros.
        Sua tarefa é extrair dados de um *sumário* de documento e formatá-los em JSON.
        
        Formato de Saída (JSON Schema Completo):
        <schema>
        {full_schema_str}
        </schema>
        
        Responda *APENAS* com o JSON preenchido. Não inclua '```json', introduções ou explicações.
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
            final_json_data = json.loads(cleaned_json)
        except json.JSONDecodeError:
            print("Erro: Bedrock retornou uma string que NÃO é um JSON válido.")
            print(f"--- Resposta Bruta Recebida --- \n{json_string_answer}\n--- Fim da Resposta ---")
            return {"status": "error", "reason": "Resposta do Reduce não é um JSON válido."}

        # Constrói o caminho de saída
        output_key_json = object_key.replace(".pdf", ".json")
        
        # Correção do 'os.basename' para 'os.path.basename'
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