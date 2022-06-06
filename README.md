# Desafio - Dev Python

Este repositório possui um teste que visa avaliar sua curiosidade, seus conhecimentos em Python, análise e limpeza de dados, Storytelling e conceitos relacionados a processos ETL/ELT. O teste possui seu próprio conjunto de arquivos, parâmetros, instruções e estratégias para ser resolvido. Portanto, estude cada detalhe com sabedoria.

# Script

Utilizou-se o airflow como ferramenta para o ETL e controle de fluxo de dados. 

- Existe uma função para extrair dados adult.data
- Existe uma função para extrarir dados adult.test
- Existe uma função para limpeza e transformação dos dados
- Existe uma função para popular o banco PostgreSQL

Utilizou-se Task Group para agrupar as funções extract na DAG

# Decisões de Limpeza

No tratamento dos dados optou-se por não excluir nenhum dado, deixando alternativas para os usuarios para tratamento do dados missing. Para facilitar a localização dos dados missing, os dados strings foram substituídos por nulos e os numericos por -1. Vale ressaltar que em casos reais existem regras definidas pela organização de tratamento de missings values. Porém, neste caso decidiu-se mantêlos para a visualização.
