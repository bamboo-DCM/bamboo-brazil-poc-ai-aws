# POC Bamboo

Este projeto contém uma função AWS Lambda projetada para processar documentos PDF extensos (como Termos de Securitização), extrair dados estruturados usando um modelo de IA Generativa (Amazon Bedrock) e salvar o resultado como um arquivo JSON.

## Funcionalidade Principal

O sistema é acionado quando um novo arquivo PDF é enviado para um bucket S3. A função Lambda então:

- Lê o PDF: Extrai o texto completo do documento.

- Divide (Split): Quebra o texto em centenas de chunks (pedaços) gerenciáveis.

- Mapeia (Map): Itera sobre cada chunk e usa um modelo "Micro" (ex: amazon.nova-lite-v1:0) para criar um pequeno sumário focado em dados relevantes.

- Reduz (Reduce): Concatena todos os sumários em um "super-sumário" e faz uma chamada final ao modelo para extrair o JSON completo deste contexto.

- Salva: Salva o JSON estruturado final em uma pasta output/ no mesmo bucket S3.

- Validação: A validação é feita usando uma chave de hash composta (baseada em CNPJ_Emissor + Numero_Requerimento + Numero_Processo) para encontrar a linha correta no CSV. Em seguida, o script compara campos-chave (ex: Valor_Total_Registrado, Nome_Emissor, Data_Registro) para identificar divergências.

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


