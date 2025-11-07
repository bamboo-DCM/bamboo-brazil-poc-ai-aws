# merge.py
import boto3
import botocore
import json
import time

def _limpar_json_merge(json_string):
    """Função de limpeza dedicada para a resposta do merge."""
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

def _call_bedrock_for_merge(bedrock_runtime, model_id, json_antigo_str, json_novo_str):
    """
    Executa a chamada de LLM específica para mesclar dois JSONs.
    """
    system_prompt = f"""
    You are an assistant specialized in consolidating contract data.

    The "Old JSON" represents the current, correct state of the contract.
    The "New JSON" represents a possible update (addendum).

    Your goal is to merge both into a *final consolidated JSON* following these strict rules:

    1. Start with an exact copy of the "Old JSON".

    2. Use the values from the "New JSON" to update the "Old JSON" ONLY IF:
    - The new value is **not null**, **not empty**, and **not a placeholder**
        such as "Não especificado", "N/A", "Não informado", or similar.

    3. If a field in the "New JSON" has one of these placeholder values,
    the old value MUST be preserved — it must NOT be overwritten.

    4. If a field in the "Old JSON" exists but is missing in the "New JSON",
    keep the old value as-is.

    5. When the field is a list (for example, "series"), perform a *deep merge*:
    - Match items by their "nome" (e.g., "1ª Série", "2ª Série").
    - Update only the subfields in that specific item that meet rule (2).
    - Keep all other fields and series unchanged.

    6. Never duplicate items in a list. If a series already exists, update it;
    if it does not exist, append it.

    7. Respond **only** with the final merged JSON object — no markdown, no comments, no explanations.
    """
 
    user_prompt = f"""
    <json_antigo>
    {json_antigo_str}
    </json_antigo>

    <json_novo_aditamento>
    {json_novo_str}
    </json_novo_aditamento>

    Por favor, retorne o JSON final mesclado.
    """
 
    messages = [{"role": "user", "content": [{"text": user_prompt}]}]
    system_prompts = [{"text": system_prompt}]
    
    max_retries = 3
    base_delay_segundos = 2
    
    for attempt in range(max_retries):
        try:
            response = bedrock_runtime.converse(
                modelId=model_id, 
                messages=messages,
                system=system_prompts,
                inferenceConfig={"maxTokens": 8192, "temperature": 0.0}
            )
            
            response_text = response["output"]["message"]["content"][0]["text"]
            return response_text # Retorna o texto JSON mesclado

        except botocore.exceptions.ClientError as error:
            error_code = error.response['Error']['Code']
            if error_code == 'ModelErrorException' or error_code == 'ThrottlingException' or error_code == 'InternalServerException':
                print(f"AVISO (Merge): Erro transiente detectado ({error_code}). Tentativa {attempt + 1}/{max_retries}.")
                if attempt + 1 == max_retries:
                    raise error
                time.sleep(base_delay_segundos * (attempt + 1))
            else:
                raise error
        except Exception as e:
            print(f"Erro inesperado no merge: {e}")
            if attempt + 1 == max_retries:
                raise e
            time.sleep(base_delay_segundos * (attempt + 1))
            
    raise Exception("Falha ao mesclar JSONs no Bedrock após múltiplas tentativas.")

def find_latest_json(s3_client, bucket, output_directory):
    """
    Encontra o arquivo .json mais recente (por data de modificação)
    em uma pasta 'output/' específica.
    """
    print(f"Verificando pasta de saída: s3://{bucket}/{output_directory}")
    try:
        # Adiciona a barra final se não tiver, para listar a "pasta"
        if not output_directory.endswith('/'):
            output_directory += '/'
            
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=output_directory)
        
        if 'Contents' not in response or not response['Contents']:
            print("Log: Pasta /output vazia.")
            return None # Pasta vazia

        # Filtra apenas por arquivos .json e encontra o mais recente
        json_files = [
            obj for obj in response['Contents'] 
            if obj['Key'].endswith('.json') and obj['Size'] > 0
        ]
        
        if not json_files:
            print("Log: Pasta /output não contém arquivos .json válidos.")
            return None

        latest_file = sorted(json_files, key=lambda obj: obj['LastModified'], reverse=True)[0]
        print(f"Log: Arquivo anterior mais recente encontrado: {latest_file['Key']}")
        return latest_file['Key']
        
    except Exception as e:
        print(f"Erro ao listar S3: {e}")
        return None

def download_json_from_s3(s3_client, bucket, key):
    """Baixa um JSON do S3 e o converte em objeto Python."""
    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        content = file_obj['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        print(f"Erro fatal ao baixar o JSON anterior ({key}): {e}")
        raise

# --- Esta é a função principal que o handler.py irá chamar ---
def execute_merge_logic(bedrock_runtime, model_id, s3_client, bucket, output_directory, dados_aditamento_novo):
    """
    Orquestra a lógica de merge. Encontra o JSON antigo, baixa, e chama o LLM para mesclar.
    """
    
    # 1. Encontra o JSON "pai"
    latest_json_key = find_latest_json(s3_client, bucket, output_directory)
    
    if latest_json_key is None:
        # Lógica de "apenas salvar": o handler.py fará isso se retornarmos None
        return None, None # Retorna (None, None) -> (JSON Mesclado, Key do Arquivo Antigo)

    # 2. Carrega o JSON "pai"
    json_anterior_obj = download_json_from_s3(s3_client, bucket, latest_json_key)
    
    json_anterior_dados = json_anterior_obj.get("dados_extraidos")
    if not json_anterior_dados:
        print("Erro: JSON anterior não continha a chave 'dados_extraidos'.")
        # Fallback: apenas salva o novo
        return None, None

    # 3. Executa o Merge com o LLM (A "segunda chamada")
    print("Iniciando 'Merge' com LLM...")
    json_antigo_str = json.dumps(json_anterior_dados, ensure_ascii=False)
    json_novo_str = json.dumps(dados_aditamento_novo, ensure_ascii=False)

    merged_json_string = _call_bedrock_for_merge(
        bedrock_runtime,
        model_id,
        json_antigo_str,
        json_novo_str
    )
    
    print("Merge com LLM concluído.")
    
    # 4. Limpa e retorna o resultado
    try:
        final_merged_json = json.loads(_limpar_json_merge(merged_json_string))
        return final_merged_json, latest_json_key
    except json.JSONDecodeError:
        print("Erro fatal: A resposta do MERGE não foi um JSON válido.")
        print(f"Resposta recebida: {merged_json_string}")
        return dados_aditamento_novo, f"ERRO_DE_MERGE_{latest_json_key}"