# utils/validacao.py

import boto3
import pandas as pd
import json
import os
import re
import io
from datetime import datetime, timezone

# --- Variáveis de Ambiente ---
CVM_BUCKET = os.environ.get('CVM_BUCKET') 
CVM_KEY = os.environ.get('CVM_KEY')

# --- Mapeamento de Colunas (JSON -> CSV) ---
MAPA_CAMPOS = {
    # Chaves para o HASH
    "securitizadora_cnpj": "CNPJ_Emissor",
    "numero_emissao": "Numero_Requerimento",
    "numero_processo": "Numero_Processo",
    
    # --- CHAVES PARA VALIDAÇÃO ---
    "volume_total": "Valor_Total_Registrado",
    "securitizadora_nome": "Nome_Emissor",
    "agente_fiduciario": "Agente_fiduciario",
    #"data_emissao_serie1": "Data_Registro" # Compara a Data_Registro da CVM com a 1ª série do JSON
}

s3_client = boto3.client('s3')
df_cvm_global = None # Cache global para o DataFrame

# --- Funções Helper de Normalização (EXPANDIDAS) ---

def normalizar_cnpj(cnpj):
    """Remove pontuação de CNPJs."""
    if not cnpj or not isinstance(cnpj, str):
        return None
    return re.sub(r'[^\d]', '', cnpj)

def normalizar_valor(valor):
    """Normaliza valores monetários para comparação."""
    if valor is None:
        return None
    if isinstance(valor, (int, float)):
        return round(float(valor), 2)
    s = str(valor).strip().upper().replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return round(float(s), 2)
    except ValueError:
        return None

def normalizar_int_string(valor):
    """Normaliza strings de número (ex: '1ª', '522') para int."""
    if valor is None:
        return None
    if isinstance(valor, int):
        return valor
    s = str(valor).strip()
    s = re.sub(r"[ªº]", "", s)
    s = s.split('.')[0] 
    try:
        return int(s)
    except ValueError:
        return None

def _criar_hash_id(cnpj, emissao, processo):
    """Cria a chave composta única a partir dos valores normalizados."""
    norm_cnpj = normalizar_cnpj(cnpj)
    norm_emissao = normalizar_int_string(emissao)
    norm_processo = str(processo).strip() if processo else None
    
    if not norm_cnpj or not norm_emissao or not norm_processo:
        return None
        
    return f"{norm_cnpj}_{norm_emissao}_{norm_processo}"

def normalizar_nome(nome):
    """Limpa e padroniza nomes de empresas para comparação."""
    if not nome or not isinstance(nome, str):
        return None
    
    s = nome.upper().strip()
    # Remove acentos (forma simples)
    s = re.sub(r'[ÁÀÂÃ]', 'A', s)
    s = re.sub(r'[ÉÈÊ]', 'E', s)
    s = re.sub(r'[ÍÌÎ]', 'I', s)
    s = re.sub(r'[ÓÒÔÕ]', 'O', s)
    s = re.sub(r'[ÚÙÛ]', 'U', s)
    s = re.sub(r'[Ç]', 'C', s)
    
    # Remove pontuação
    s = re.sub(r'[.,;!?()]', '', s)
    
    # Remove sufixos corporativos comuns
    sufixos = ['S.A.', 'S/A', 'SA', 'LTDA', 'EIRELI', 'ME', 'EPP']
    # Cria uma regex (ex: \bS\.A\.\b|\bLTDA\b)
    sufixos_regex = r'\b(' + '|'.join(re.escape(suf) for suf in sufixos) + r')\b'
    s = re.sub(sufixos_regex, '', s)
    
    # Remove espaços duplicados
    s = re.sub(r'\s+', ' ', s).strip()
    
    return s

def normalizar_data(data_str):
    """
    Tenta converter uma string de data (YYYY-MM-DD ou DD/MM/YYYY)
    para o formato ISO (YYYY-MM-DD).
    """
    if not data_str or not isinstance(data_str, str):
        return None
        
    data_str = data_str.strip()
    
    try:
        # Tenta o formato CVM (YYYY-MM-DD)
        if '-' in data_str:
            data_obj = datetime.strptime(data_str.split(' ')[0], '%Y-%m-%d')
        # Tenta o formato LLM (DD/MM/YYYY)
        elif '/' in data_str:
            data_obj = datetime.strptime(data_str.split(' ')[0], '%d/%m/%Y')
        else:
            return None # Formato não reconhecido
            
        return data_obj.strftime('%Y-%m-%d')
        
    except ValueError:

        return None


# --- Carregamento Otimizado do CSV ---
def carregar_cvm_global():
    """
    Carrega o CSV da CVM do S3 e cria a coluna 'hash_id' para indexação.
    """
    global df_cvm_global
    if df_cvm_global is not None:
        print("Validação: Reutilizando DataFrame CVM global.")
        return df_cvm_global
        
    print(f"Validação: Carregando DataFrame CVM de s3://{CVM_BUCKET}/{CVM_KEY}")
    try:
        obj = s3_client.get_object(Bucket=CVM_BUCKET, Key=CVM_KEY)
        
        df_cvm_global = pd.read_csv(
            io.BytesIO(obj['Body'].read()), 
            encoding='latin1', 
            sep=',', 
            low_memory=False 
        )
        print("Validação: DataFrame CVM carregado. Limpando nomes de colunas...")
        
        df_cvm_global.columns = df_cvm_global.columns.str.strip()

        col_cnpj = MAPA_CAMPOS["securitizadora_cnpj"]
        col_emissao = MAPA_CAMPOS["numero_emissao"]
        col_processo = MAPA_CAMPOS["numero_processo"]
        print(f"DA CVM: Colunas de hash mapeadas: CNPJ='{col_cnpj}', Emissão='{col_emissao}', Processo='{col_processo}'")

        if not all(col in df_cvm_global.columns for col in [col_cnpj, col_emissao, col_processo]):
            print(f"ERRO: Colunas de hash não encontradas no CSV. Esperado: {col_cnpj}, {col_emissao}, {col_processo}")
            df_cvm_global = None
            return None

        # Cria a coluna de hash_id no DataFrame da CVM
        df_cvm_global['hash_id'] = df_cvm_global.apply(
            lambda row: _criar_hash_id(
                row[col_cnpj], 
                row[col_emissao], 
                row[col_processo]
            ),
            axis=1
        )
        
        df_cvm_global = df_cvm_global.set_index('hash_id', drop=False)
        print("Validação: DataFrame CVM indexado por 'hash_id'.")
        return df_cvm_global
        
    except Exception as e:
        print(f"Validação ERRO FATAL: Falha ao carregar ou indexar CSV da CVM: {e}")
        df_cvm_global = None
        raise e

# --- Lógica de Comparação ---

def _comparar_campos(dados_llm, linha_cvm):
    """Compara os campos mapeados e retorna a lista de divergências."""
    divergencias = []

    # 1. Comparação de Volume
    chave_llm_vol = "volume_total"
    chave_cvm_vol = MAPA_CAMPOS.get(chave_llm_vol)
    
    if chave_cvm_vol:
        val_llm = normalizar_valor(dados_llm.get(chave_llm_vol))
        val_cvm = normalizar_valor(linha_cvm.get(chave_cvm_vol))
        
        if (val_llm is not None and val_cvm is not None) and (val_llm != val_cvm):
            divergencias.append({
                "campo": "Volume Total",
                "valor_llm": dados_llm.get(chave_llm_vol),
                "valor_cvm": linha_cvm.get(chave_cvm_vol),
                "detalhe": f"Normalizado LLM: {val_llm} vs Normalizado CVM: {val_cvm}"
            })

    # --- Nome da Securitizadora ---
    chave_llm_nome = "securitizadora" # Objeto
    chave_cvm_nome = MAPA_CAMPOS.get("securitizadora_nome")
    
    if chave_cvm_nome:
        nome_llm_original = dados_llm.get(chave_llm_nome, {}).get("nome")
        nome_llm = normalizar_nome(nome_llm_original)
        nome_cvm = normalizar_nome(linha_cvm.get(chave_cvm_nome))

        if (nome_llm is not None and nome_cvm is not None) and (nome_llm != nome_cvm):
            divergencias.append({
                "campo": "Nome da Securitizadora",
                "valor_llm": nome_llm_original,
                "valor_cvm": linha_cvm.get(chave_cvm_nome),
                "detalhe": f"Normalizado LLM: {nome_llm} vs Normalizado CVM: {nome_cvm}"
            })

    # --- Agente Fiduciário ---
    chave_llm_af = "agente_fiduciario"
    chave_cvm_af = MAPA_CAMPOS.get(chave_llm_af)
    
    if chave_cvm_af:
        nome_llm_original = dados_llm.get(chave_llm_af)
        nome_llm = normalizar_nome(nome_llm_original)
        nome_cvm = normalizar_nome(linha_cvm.get(chave_cvm_af))

        if (nome_llm is not None and nome_cvm is not None) and (nome_llm != nome_cvm):
            divergencias.append({
                "campo": "Agente Fiduciário",
                "valor_llm": nome_llm_original,
                "valor_cvm": linha_cvm.get(chave_cvm_af),
                "detalhe": f"Normalizado LLM: {nome_llm} vs Normalizado CVM: {nome_cvm}"
            })
    
    return divergencias

def execute_validation(dados_llm_json):
    """
    Ponto de entrada principal para o módulo de validação.
    """
    print("Validação: Módulo execute_validation iniciado.")
    
    if not CVM_BUCKET or not CVM_KEY:
        print("Validação ERRO: Variáveis de ambiente CVM_BUCKET/CVM_KEY não definidas.")
        return {"status": "ERRO", "motivo_falha": "Configuração de CVM ausente no Lambda."}

    try:
        # 1. Carregar/Reutilizar o DataFrame da CVM (com índice de hash)
        df_cvm = carregar_cvm_global()
        if df_cvm is None:
            raise Exception("DataFrame CVM não pôde ser carregado ou indexado.")

        # --- LÓGICA DE MATCH (HASH) ---
        
        # 2. Obter as chaves do JSON extraído
        cnpj_llm = dados_llm_json.get("securitizadora", {}).get("cnpj")
        emissao_llm = dados_llm_json.get("numero_emissao")
        processo_llm = dados_llm_json.get("numero_processo")
        
        # 3. Criar o hash_id com os dados do LLM
        hash_id_llm = _criar_hash_id(cnpj_llm, emissao_llm, processo_llm)
        
        if not hash_id_llm:
            return {
                "status": "REPROVADA",
                "timestamp_validacao": datetime.now(timezone.utc).isoformat(),
                "chave_match": f"HASH_LIDO: {cnpj_llm}, {emissao_llm}, {processo_llm}",
                "motivo_falha": "Campos-chave (CNPJ, Emissão, Processo) ausentes ou inválidos no JSON do LLM.",
                "divergencias": []
            }

        # 4. Localizar a linha na CVM
        try:
            linha_cvm = df_cvm.loc[hash_id_llm] 
            if isinstance(linha_cvm, pd.DataFrame):
                linha_cvm = linha_cvm.iloc[0]
                
        except KeyError:
            return {
                "status": "REPROVADA",
                "timestamp_validacao": datetime.now(timezone.utc).isoformat(),
                "chave_match": f"HASH_BUSCADO: {hash_id_llm}",
                "motivo_falha": "Hash (CNPJ+Emissão+Processo) não localizado na base de dados da CVM.",
                "divergencias": []
            }
        
        # 5. Comparar os campos de validação 
        divergencias = _comparar_campos(dados_llm_json, linha_cvm)
        
        # 6. Gerar Relatório
        status_final = "REPROVADA" if divergencias else "APROVADA"
        
        return {
            "status": status_final,
            "timestamp_validacao": datetime.now(timezone.utc).isoformat(),
            "fonte_dados_cvm": CVM_KEY,
            "chave_match": f"HASH_CVM: {hash_id_llm}",
            "divergencias": divergencias
        }

    except Exception as e:
        print(f"Validação ERRO INESPERADO: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "ERRO",
            "motivo_falha": f"Erro interno no módulo de validação: {e}"
        }