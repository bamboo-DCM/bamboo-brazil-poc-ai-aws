# POC Bamboo

Este projeto contém uma função AWS Lambda projetada para processar documentos PDF extensos (como Termos de Securitização), extrair dados estruturados usando um modelo de IA Generativa (Amazon Bedrock) e salvar o resultado como um arquivo JSON.

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


