## Pipeline de Extração de Dados
Este repositório contém o código para o pipeline de extração de dados, que coleta dados da Bovespa e armazena em no bucket 'fiap-challenge-bovespa-data-raw'.

O pipeline consiste em 4 etapas:
Extração: 
* Um script Python (scraper.py) é responsável por acessar o site e baixar os arquivos ZIP.
* O script é executado localmente e salva os arquivos ZIP em um diretório específico.

Envio para AWS:
* Os arquivos ZIP são enviados para o bucket descrito S3 na AWS.

Processamento:
* Uma função Lambda é configurada para ser disparada quando um novo arquivo ZIP é adicionado ao bucket S3.
* A função Lambda extrai o conteúdo dos arquivos ZIP e salva os dados em um novo bucket S3.

ETL e Carga:
* Um job Glue é configurado para processar os dados extraídos do bucket S3, realizando a transformação e carregamento dos dados em um banco de dados no Lake Formation.

### Diagrama do Pipeline

!["Pipeline ingestão de dados Bovespa"](/fluxo-pipeline.png)