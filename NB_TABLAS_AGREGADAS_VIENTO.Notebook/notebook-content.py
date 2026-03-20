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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## TABLA VIENTO AÑO MES DIA CON SPARK

# CELL ********************

# PASO 1:  LEER DESDE EL ORIGEN y CAMINO LARGO
from pyspark.sql.functions import max, min, avg


df_original = spark.sql(
"""
SELECT * FROM dbo.ft_vientos

"""
)
display(df_original)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PASO 2, REALIZAR LAS AGREGACIONES CON SPARK

from pyspark.sql.functions import max, min, avg, year, month, dayofmonth

df_agregado = df_original \
    .withColumn("año", year("time")) \
    .withColumn("mes", month("time")) \
    .withColumn("dia", dayofmonth("time")) \
    .groupBy("año", "mes", "dia") \
    .agg(
        max("dd_valor").alias("dir_racha1"),
        min("dd_valor").alias("dir_racha2"),
        avg("ff_valor").alias("vv_viento_promedio")
    ) \
    .orderBy("año", "mes", "dia")

display(df_agregado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#guardamos esta tabla

df_agregado.write.format("delta").mode("overwrite").saveAsTable("dbo.ft_viento_anno_mes_dia")
df_check = spark.sql("""select * from dbo.ft_viento_anno_mes_dia""")
display(df_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## TABLA AÑO MES CON SQL DIRECTO

# CELL ********************

#creacion directa

df_inicio =  spark.sql (
"""
SELECT
    year(time)  as anno,
    month(time) as mes, 
    MAX(dd_valor) as Dir_racha1,
    MAX(dd_valor) as Dir_racha2,           
    round( AVG(ff_valor) , 2)  as vv_viento_promedio ,  
    CodigoNacional
FROM dbo.ft_vientos
GROUP BY  
    year(time), 
    month(time), 
    CodigoNacional

"""
)
df_inicio.write.format("delta").mode("overwrite").saveAsTable("dbo.ft_viento_anno_mes")


#verificamos visualmente
df_review= spark.sql("""select * from  dbo.ft_viento_anno_mes """)
display(df_review)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
