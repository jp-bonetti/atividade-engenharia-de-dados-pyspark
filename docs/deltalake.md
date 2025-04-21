## Importando as dependências

Para realizar as operações, precisamos importar as seguintes bibliotecas, incluindo os tipos específicos para criação das tabelas.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

from delta import *
```

## Criação da classe Spark

Após importar, já podemos inicializar nossa classe Spark, que vai ser responsável por toda a manipulação dos dados.


```python
spark = ( 
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate() 
)
```

## Criação da base de dados

Nessa etapa, em **data** estamos criando a injeção inicial de dados para facilizar a manipulação futuramente.

Em **Schema**, declaramos a estrutura que vai compor o nosso projeto.

```python
data = [
    ("1", "PRODUTO_X","MARCA_X", "CATEGORIA_X" ,800.00, 50),
    ("2", "PRODUTO_Y", "MARCA_Y", "CATEGORIA_Y", 500.00, 100),
    ("3", "PRODUTO_Z","MARCA_Z", "CATEGORIA_Z", 150.00, 150)
]

schema = (
    StructType([
        StructField("ID_PRODUTO", StringType(),True),
        StructField("NOME_PRODUTO", StringType(),True),
        StructField("MARCA_PRODUTO", StringType(),True),
        StructField("CATEGORIA_PRODUTO", StringType(),True),
        StructField("PRECO_PRODUTO", FloatType(), True),
        StructField("QUANTIDADE_PRODUTO", IntegerType(), True)
    ])
)

df = spark.createDataFrame(data=data,schema=schema)

df.show(truncate=False)
```

## Salvando o DataFrame no formato Delta

Após definir a estrutura e os dados inicias, vamos salvar o DataFrame no formato Delta, configurar para que os dados sejam sobrescritos, e informar onde os dados devem ser salvos.

```python
( 
    df
    .write
    .format("delta")
    .mode('overwrite')
    .save("./data/PRODUTOS")
)
```

## INSERT

Definição dos novos dados a serem inseridos.

```python
new_data = [
    ("4","PRODUTO_A", "MARCA_A", "CATEGORIA_A", 50.00, 60),
    ("5","PRODUTO_B", "MARCA_B", "CATEGORIA_B", 150.00, 90),
    ("6","PRODUTO_C", "MARCA_C", "CATEGORIA_C", 300.00, 20)
]

df_new = spark.createDataFrame(data=new_data, schema=schema)

deltaTable = DeltaTable.forPath(spark, "./data/PRODUTOS")
```

Operação de marge entra o DataFrame já existente, e o criado acima.

```python
(
    deltaTable.alias("dados_atuais")
    .merge(
        df_new.alias("novos_dados"),
        "dados_atuais.ID_PRODUTO = novos_dados.ID_PRODUTO"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

## DELETE

Operação de deletar com base no ID do produto.

```python
deltaTable.delete("ID_PRODUTO = 4")
```

## UPDATE

Operação de atualizar o preço do produto com base no seu ID.

```python
deltaTable.update("ID_PRODUTO = 2", set = { "PRECO_PRODUTO": "400.00" })
```

## Exibir o estado atual da tabela

```python
(
    spark
    .read
    .format('delta')
    .load("./data/PRODUTOS")
    .show()
)
```

## Histórico de alterações 

```python
(
    deltaTable
    .history()
    .select("version", "timestamp", "operation", "operationMetrics")
    .show()
)
```