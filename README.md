# POC Bamboo

Este projeto contém uma função AWS Lambda projetada para processar documentos PDF extensos (como Termos de Securitização), extrair dados estruturados usando um modelo de IA Generativa (Amazon Bedrock) e salvar o resultado como um arquivo JSON.

## Arquitetura

O fluxo da solução funciona da seguinte maneira:

1.  **Input:** Usuário faz upload de um arquivo PDF no Bucket S3.
2.  **Trigger:** O evento `s3:ObjectCreated` aciona a função Lambda.
3.  **Processamento:**
    * A Lambda roda em um container Docker (imagem ECR).
    * Lê o arquivo de referência `.csv` na pasta `CVM/`.
     ( Aqui uma observação importante, a pasta CVM deve conter o arquivo 'oferta_resolucao_160.csv', que será utilizado na parte da validação, sem esse o 'CVM/oferta_resolucao_160' a lambda retornará erro nessa etapa. )
    * Processa o PDF e invoca o modelo no **Amazon Bedrock**.
4.  **Output:** O resultado é gerado e salvo na pasta de output da pasta do projeto.
 


## Estrutura do Bucket S3 

Para que a aplicação funcione corretamente, o Bucket S3 deve seguir estritamente esta estrutura:

```
bucket-s3/
├── CVM/
│   └── oferta_resolucao_160.csv  <-- ARQUIVO OBRIGATÓRIO DE REFERÊNCIA
├── pasta individual do projeto/cliente 1
│   ├── (arquivos .pdf aqui)  <-- Upload dos arquivos para processamento
│   ├── output/
│     └── arquivos json com os resultados
├── pasta individual do projeto/cliente 2
│   ├──(arquivos .pdf aqui)        
│   ├──output/
│     └── arquivos json com os resultados
└── 
```

## Funcionalidades Principais

O sistema é acionado via S3 e executa um fluxo em 4 etapas:

1.  **Extração (MapReduce):**
    * Processa PDFs extensos dividindo-os em *chunks*.
    * Utiliza prompts de "Caça" (*Hunting*) para garantir a captura exata de identificadores (CNPJ, Processo, Emissão).
    * Gera um JSON estruturado inicial.

2.  **Consolidação (Merge):**
    * Verifica se já existe um arquivo processado anteriormente para a mesma operação.
    * Se existir (ex: subindo um Aditamento após o Termo original), o sistema funde os dados.
    * **Regra:** Preserva a identidade do documento original e atualiza apenas os dados novos.

3.  **Validação Condicional (CVM 160):**
    * O sistema verifica se o documento possui o **Número do Processo CVM**.
    * **Sem Número (Termo Original):** Define status como `PENDENTE` e suspende a validação (evita falsos negativos).
    * **Com Número (Anúncio/Aditamento):** Normaliza o formato do processo (ex: `CVM/SRE/...` para `SRE/0000/0000`), cria um **Hash Composto** e valida contra o CSV da CVM. 
      
4.  **Saída (Output):**
    * Salva o JSON final em `output/`.
    * Se houver divergência na validação (status `REPROVADA`), gera um **Relatório de Erro** separado na pasta configurada.
## Método Utilizado
Para lidar com documentos que excedem a janela de contexto dos LLMs, este código implementa uma estratégia de `Sumarização Hierárquica` (MapReduce). Isso garante que documentos de qualquer tamanho possam ser processados, embora com um trade-off entre velocidade e a precisão da sumarização.

O código inclui logs de observabilidade (contagem de tokens de entrada e saída) para monitoramento de custos.

## Exemplo de Output (JSON)

Para cada documento processado, a solução gera um arquivo `.json` contendo os metadados, o status da validação na CVM e os dados estruturados extraídos pelo LLM.

Abaixo, um exemplo:

```json
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
```

## Como Testar Localmente
É possível executar o handler localmente para depuração (isto irá gerar custos reais de S3 e Bedrock).

- Instale as dependências:

```Bash
pip install -r requirements.txt
```
- Configure o Ambiente:

  - Certifique-se de que suas credenciais AWS (aws configure) estão ativas e têm permissão para S3 (GetObject, PutObject) e Bedrock (Converse).
  - Crie um arquivo .env na raiz do projeto. 

```sh
  # ID do modelo Bedrock a ser usado
MODEL_ID="modelo-do-bedrock"

# Localização do arquivo CSV da CVM
CVM_BUCKET="nome-do-bucket"
CVM_KEY="path-para-o-arquivo-cvm"

# Prefixo (pasta) para salvar relatórios de reprovação
REPORT_PREFIX="
path-para-onde-salvar-os-relatorios "

# a quantidade de Workers para o paralelismo no Lambda
MAX_WORKERS = x
```


- Faça o upload de um PDF de teste pequeno (2-3 páginas) para seu bucket S3.

- Configure o Evento Mock:

  -  Edite o arquivo mock_event.json.

  - Altere bucket.name e object.key para apontar para o seu PDF de teste no S3.

- Execute o Handler:

```Bash
python handler.py
```
A execução local imprimirá os logs (incluindo a contagem de tokens) e, se bem-sucedida, salvará o JSON resultante na pasta output/ do seu bucket.
