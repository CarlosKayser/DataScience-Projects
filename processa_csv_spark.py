from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

spark = SparkSession \
        .builder \
        .appName("Processa CSV") \
        .getOrCreate()

to_value = lambda v : float(v.replace(",", "."))
udf_to_value = F.udf(to_value, types.FloatType())

df = spark.read.csv("data/2019_Viagem_UTF8.csv", header=True, sep=';')

df2 = df.withColumn("ValorPassagens", udf_to_value(df["Valor passagens"])) \
        .withColumn("PeriodoDataInicio", F.to_date(df["Período - Data de início"], format="dd/MM/yyyy")) \
        .withColumn("PeriodoDataFim", F.to_date(df["Período - Data de fim"], format="dd/MM/yyyy"))

df3 = df2.select(df2["Nome do órgão superior"].alias("Orgao"), \
                 df2["Nome órgão solicitante"].alias("OrgaoSolicitante"), \
                 df2["Identificador do processo de viagem"].alias("ProcessoViagem"), 
                 df2["ValorPassagens"], df2["PeriodoDataInicio"], df2["PeriodoDataFim"])

df3.write.parquet("viagens_2.parquet")

# Pra rodar esse arquivo como um Job no Spark, usamos o seguinte comando:
# spark-submit --master local[2] processa_csv_spark.py

# Mais informações --> https://spark.apache.org/docs/latest/submitting-applications.html