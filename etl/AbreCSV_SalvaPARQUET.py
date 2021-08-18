from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max

# Cria objeto da Spark Session
spark = (SparkSession.builder.appName("parquetExercise")
    .getOrCreate()
)


# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/docentes_co.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/docentes_co/")
)


# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/docentes_nordeste.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/docentes_nordeste/")
)

# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/docentes_norte.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/docentes_norte/")
)

# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/docentes_sudeste.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/docentes_sudeste/")
)

# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/docentes_sul.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/docentes_sul/")
)


# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/escolas.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/escolas/")
)


# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/gestor.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/gestor/")
)



# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/matricula_co.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/matricula_co/")
)



# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/matricula_nordeste.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/matricula_nordeste/")
)



# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/matricula_norte.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/matricula_norte/")
)


# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/matricula_sudeste.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/matricula_sudeste/")
)

# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/matricula_sul.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/matricula_sul/")
)



# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", "|")
    .load("s3://igti-bootcamp-ed-2021-468906988086/raw/turmas.CSV")
)

# Escreve a tabela em staging em formato parquet
print("Writing parquet table...")
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO_CENSO")
    .save("s3://datalake-bruno-468906988086-igti-edc-tf/staging-zone/censo/turmas/")
)