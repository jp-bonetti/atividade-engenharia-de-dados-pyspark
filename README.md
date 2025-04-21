## Projeto Apache Spark com Apache Iceberg e Apache Delta Lake

Projeto desenvolvido para demonstração do Apache Spark Local (pyspark) gravando arquivos no formato Apache Iceberg e Apache Delta Lake, também de forma local.

Necessário possuir Python na versão 3.12, e Java na versão 8. Recomendado possuir sistema Linux para execução do projeto.

Projeto Python inicializado com o [Poetry](https://github.com/python-poetry/poetry).

Comandos utilizados para setup do ambiente:

```bash copy
poetry init
poetry add pyspark=3.4.2 delta-spark=2.4.0 jupyterlab
poetry shell
jupyter-lab
```

Os exemplos de código pyspark/python para instanciar o Spark, bem como criar e manipular uma tabela Apache Iceberg e Apache Delta Lake, estão no arquivo `iceberg.ipynb` e `delta-lake.ipynb`, respectivamente.

A cada nova execução dos notebooks, apague a pasta data.

Documentação MKDocs disponível em: https://jp-bonetti.github.io/atividade-engenharia-de-dados-pyspark/