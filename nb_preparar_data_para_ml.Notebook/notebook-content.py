# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8197d5f6-e14a-46b9-8bf7-97e66f671a08",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "fb0905fd-76e1-4862-81bf-a8ad86ea00d6",
# META       "known_lakehouses": [
# META         {
# META           "id": "8197d5f6-e14a-46b9-8bf7-97e66f671a08"
# META         },
# META         {
# META           "id": "dc09f737-fad5-437e-acbd-4645f677d903"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Preparar df para entrenar

df_para_entrenar = spark.read.table("dbo.ft_vientos")
df_para_entrenar.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Celda 2 - Agregación diaria de viento promedio por estación
from pyspark.sql import functions as F

df_agregado = (
    df_para_entrenar
    .withColumn("fecha", F.to_date("time"))
    .withColumn("anio",  F.year("time"))
    .withColumn("mes",   F.month("time"))
    .withColumn("dia",   F.dayofmonth("time"))
    .groupBy("CodigoNacional", "anio", "mes", "dia", "fecha")
    .agg(
        F.round(F.avg("ff_valor"), 2).alias("vviento_promedio")
    )
    .orderBy("CodigoNacional", "anio", "mes", "dia")
)

df_agregado.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#guardar en Gold

df_agregado.write.format("delta").mode("overwrite")\
.saveAsTable("lh_gold_walter.dbo.ml_train_data_viento_promedio")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
