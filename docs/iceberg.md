## Importando as dependências

Para realizar as operações, precisamos importar a seguinte biblioteca.

```python
from pyspark.sql import SparkSession
```

## Criação da classe Spark

Após importar, já podemos inicializar nossa classe Spark, que vai ser responsável por toda a manipulação dos nossos dados.


```python
spark = SparkSession.builder \
  .appName("IcebergLocalDevelopment") \
  .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0') \
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.local.type", "hadoop") \
  .config("spark.sql.catalog.local.warehouse", "data/spark-warehouse/iceberg") \
  .getOrCreate()
```

Conforme será mostrado, toda a manipulação é feita através de comandos SQL.

## Criação da base de dados

```python
spark.sql(
  """
  CREATE TABLE local.PRODUTOS (ID_PRODUTO INT, NOME_PRODUTO STRING, MARCA_PRODUTO STRING, CATEGORIA_PRODUTO STRING, PRECO_PRODUTO DOUBLE, QUANTIDADE_PRODUTO INT) USING iceberg
  """
)
```

## INSERT

```python
spark.sql('''INSERT INTO local.PRODUTOS VALUES 
    (1, "PRODUTO_X","MARCA_X", "CATEGORIA_X" ,800.00, 50),
    (2, "PRODUTO_Y", "MARCA_Y", "CATEGORIA_Y", 500.00, 100),
    (3, "PRODUTO_Z","MARCA_Z", "CATEGORIA_Z", 150.00, 150)''')
```

## DELETE

Operação de deletar com base no ID do produto.

```python
spark.sql('delete from local.PRODUTOS where ID_PRODUTO = 3')
```

## UPDATE

Operação de atualizar o preço do produto com base no seu ID.

```python
spark.sql(
    """
    update local.PRODUTOS set ANO_FABRICACAO_PRODUTO = '2023' where ID_PRODUTO = 3
    """
)
```

## SELECT

```python
spark.sql('select * from local.PRODUTOS').show()
```
