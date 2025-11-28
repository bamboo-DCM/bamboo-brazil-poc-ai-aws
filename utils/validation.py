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
    "securitizadora_cnpj": "CNPJ_Emissor",
    "numero_emissao": "Numero_Requerimento", 
    "numero_processo": "Numero_Processo",
    
    "volume_total": "Valor_Total_Registrado",
    "securitizadora_nome": "Nome_Emissor",
    "agente_fiduciario": "Agente_fiduciario"
}

s3_client = boto3.client('s3')
df_cvm_global = None 

# --- Funções Helper de Normalização ---

def normalizar_cnpj(cnpj):
    if not cnpj or not isinstance(cnpj, str): return None
    return re.sub(r'[^\d]', '', cnpj)

def normalizar_valor(valor):
    if valor is None: return None
    if isinstance(valor, (int, float)): return round(float(valor), 2)
    s = str(valor).strip().upper().replace("R$", "").replace(".", "").replace(",", ".").strip()
    try: return round(float(s), 2)
    except ValueError: return None

def normalizar_int_string(valor):
    if valor is None: return None
    if isinstance(valor, int): return valor
    s = str(valor).strip()
    s = re.sub(r"[ªº]", "", s)
    s = s.split('.')[0] 
    try: return int(s)
    except ValueError: return None


def normalizar_processo(proc_str):
    """
    Transforma formatos complexos no padrão CVM: SRE/NNNN/AAAA.
    Entrada: CVM/SRE/AUT/CRI/PRI/2025/590
    Saída:   SRE/0590/2025 (Com 4 dígitos e zeros à esquerda)
    """
    if not proc_str or not isinstance(proc_str, str):
        return None
    
    s = proc_str.strip().upper()
    
    # 1. Tenta extrair ANO e NUMERO do formato longo (.../ANO/NUMERO)
    match_longo = re.search(r'/(\d{4})/(\d+)$', s)
    if match_longo:
        ano = match_longo.group(1)
        numero = match_longo.group(2)
        
        # Garante 4 dígitos com zeros à esquerda
        numero_padronizado = numero.zfill(4)
        
        return f"SRE/{numero_padronizado}/{ano}"

    # 2. Tenta limpar formatos que já parecem SRE (ex: SRE/1/2023 -> SRE/0001/2023)
    match_curto = re.match(r'^([A-Z]{2,3})/(\d+)/(\d{4})$', s)
    if match_curto:
        prefixo = match_curto.group(1)
        numero = match_curto.group(2)
        ano = match_curto.group(3)
        
        numero_padronizado = numero.zfill(4)
        return f"{prefixo}/{numero_padronizado}/{ano}"

    return s
# ----------------------------------------------------

def _criar_hash_id(cnpj, emissao, processo):
    norm_cnpj = normalizar_cnpj(cnpj)
    norm_emissao = normalizar_int_string(emissao)
    
    # Usa a nova função de normalização
    norm_processo = normalizar_processo(processo)
    
    if not norm_cnpj or not norm_emissao or not norm_processo:
        return None
        
    return f"{norm_cnpj}_{norm_emissao}_{norm_processo}"


def normalizar_nome(nome):
    if not nome or not isinstance(nome, str): return None
    s = nome.upper().strip()
    s = re.sub(r'[ÁÀÂÃ]', 'A', s)
    s = re.sub(r'[ÉÈÊ]', 'E', s)
    s = re.sub(r'[ÍÌÎ]', 'I', s)
    s = re.sub(r'[ÓÒÔÕ]', 'O', s)
    s = re.sub(r'[ÚÙÛ]', 'U', s)
    s = re.sub(r'[Ç]', 'C', s)
    s = re.sub(r'[.,;!?()]', '', s)
    sufixos = ['SA', 'LTDA', 'EIRELI', 'ME', 'EPP']
    sufixos_regex = r'\b(' + '|'.join(re.escape(suf) for suf in sufixos) + r')\b'
    s = re.sub(sufixos_regex, '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalizar_data(data_str):
    if not data_str or not isinstance(data_str, str): return None
    data_str = data_str.strip()
    try:
        if '-' in data_str: return datetime.strptime(data_str.split(' ')[0], '%Y-%m-%d').strftime('%Y-%m-%d')
        elif '/' in data_str: return datetime.strptime(data_str.split(' ')[0], '%d/%m/%Y').strftime('%Y-%m-%d')
        else: return None
    except ValueError: return None


# --- Carregamento Otimizado do CSV ---
def carregar_cvm_global():
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
        
        df_cvm_global.columns = df_cvm_global.columns.str.strip()

        col_cnpj = MAPA_CAMPOS["securitizadora_cnpj"]
        col_emissao = MAPA_CAMPOS["numero_emissao"]
        col_processo = MAPA_CAMPOS["numero_processo"]

        if col_processo in df_cvm_global.columns:
             df_cvm_global[col_processo] = df_cvm_global[col_processo].astype(str).apply(normalizar_processo)

        if not all(col in df_cvm_global.columns for col in [col_cnpj, col_emissao, col_processo]):
            print(f"ERRO: Colunas de hash não encontradas. Esperado: {col_cnpj}, {col_emissao}, {col_processo}")
            return None

        df_cvm_global['hash_id'] = df_cvm_global.apply(
            lambda row: _criar_hash_id(row[col_cnpj], row[col_emissao], row[col_processo]),
            axis=1
        )
        
        df_cvm_global = df_cvm_global.set_index('hash_id', drop=False)
        return df_cvm_global
        
    except Exception as e:
        print(f"Validação ERRO FATAL: {e}")
        df_cvm_global = None
        raise e

def _comparar_campos(dados_llm, linha_cvm):

    divergencias = []
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
    return divergencias

def execute_validation(dados_llm_json):

    print("Validação: Módulo execute_validation iniciado.")
    if not CVM_BUCKET or not CVM_KEY: return {"status": "ERRO", "motivo_falha": "Configuração ausente."}

    try:
        df_cvm = carregar_cvm_global()
        if df_cvm is None: raise Exception("DataFrame CVM falhou.")

        cnpj_llm = dados_llm_json.get("securitizadora", {}).get("cnpj")
        emissao_llm = dados_llm_json.get("numero_emissao")
        processo_raw = dados_llm_json.get("numero_processo", "")
        
        if not processo_raw: processos_lista = [None]
        else: processos_lista = [p.strip() for p in re.split(r'[;,]', str(processo_raw)) if p.strip()]

        linha_cvm = None
        hash_id_encontrado = None
        
        for proc in processos_lista:

            tentativa_hash = _criar_hash_id(cnpj_llm, emissao_llm, proc)
            
            if not tentativa_hash: continue
                
            try:
                print(f"Validação: Tentando match com hash: {tentativa_hash}")
                resultado = df_cvm.loc[tentativa_hash]
                if isinstance(resultado, pd.DataFrame): linha_cvm = resultado.iloc[0]
                else: linha_cvm = resultado
                hash_id_encontrado = tentativa_hash
                print(f"Validação: SUCESSO! Match encontrado.")
                break 
            except KeyError: continue

        if linha_cvm is None:
            return {
                "status": "REPROVADA",
                "timestamp_validacao": datetime.now(timezone.utc).isoformat(),
                "chave_match": f"Tentativas falharam para processos: {processos_lista}",
                "motivo_falha": "Hash não localizado na CVM.",
                "divergencias": []
            }
        
        divergencias = _comparar_campos(dados_llm_json, linha_cvm)
        status_final = "REPROVADA" if divergencias else "APROVADA"
        
        return {
            "status": status_final,
            "timestamp_validacao": datetime.now(timezone.utc).isoformat(),
            "fonte_dados_cvm": CVM_KEY,
            "chave_match": hash_id_encontrado,
            "divergencias": divergencias
        }

    except Exception as e:
        print(f"Validação ERRO: {e}")
        return {"status": "ERRO", "motivo_falha": str(e)}