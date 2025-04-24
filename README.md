## Projeto Apache Spark com Apache Iceberg e Apache Delta Lake

Projeto desenvolvido para demonstração do Apache Spark Local (pyspark) gravando arquivos no formato Apache Iceberg e Apache Delta Lake, também de forma local.

Necessário possuir Python na versão 3.12, e Java na versão 8. Recomendado possuir sistema Linux para execução do projeto.

Projeto Python inicializado com o [Poetry](https://github.com/python-poetry/poetry).

## Instalação do projeto:

```bash copy
git clone https://github.com/jp-bonetti/atividade-engenharia-de-dados-pyspark.git

cd atividade-engenharia-de-dados-pyspark

poetry install

poetry shell

jupyter-lab
```

Dentro da pasta notebook, estão os exemplos de código Pyspark/Python para instanciar o Spark, bem como criar e manipular uma tabela Apache Iceberg e Apache Delta Lake, no arquivo `iceberg.ipynb` e `delta-lake.ipynb`, respectivamente.

A cada nova execução dos notebooks, apague a pasta data.

Documentação MKDocs disponível em: https://jp-bonetti.github.io/atividade-engenharia-de-dados-pyspark/

Autor: [João Paulo Sigieski Bonetti](https://github.com/jp-bonetti)