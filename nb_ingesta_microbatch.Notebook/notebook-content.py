# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fef4e400-36b2-4048-95d2-c1eb2197b4c8",
# META       "default_lakehouse_name": "lh_bronze_wcl",
# META       "default_lakehouse_workspace_id": "fb0905fd-76e1-4862-81bf-a8ad86ea00d6",
# META       "known_lakehouses": [
# META         {
# META           "id": "fef4e400-36b2-4048-95d2-c1eb2197b4c8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#importar librerias
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import pyspark.sql.functions as F

#definir rutas de archivo

ruta_origen = "abfss://WS2603_WALTER@onelake.dfs.fabric.microsoft.com/lh_bronze_wcl.Lakehouse/Files/sensor_simulado"
ruta_chequeo = "abfss://WS2603_WALTER@onelake.dfs.fabric.microsoft.com/lh_bronze_wcl.Lakehouse/Files/msg_chequeo"
nombre_tabla = "dbo.sensores_simulados"

esquema_archivo = StructType()\
    .add("id" ,        StringType())\
    .add("temp" ,      DoubleType())\
    .add("humedad" ,   DoubleType() )\
    .add("vviento" ,   DoubleType())\
    .add("timestamp" , TimestampType())

spark.sql(
f"""
CREATE TABLE IF NOT EXISTS {nombre_tabla}(
    id  STRING ,
    temp DOUBLE,
    humedad DOUBLE,
    vviento DOUBLE,
    ts_registro TIMESTAMP, 
    ts_process TIMESTAMP
) USING DELTA

"""
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DELTA STREAMING o Microbatching

archivo_raiz = spark.readStream\
    .schema(esquema_archivo)\
    .option("maxFilesPerTrigger" , 1)\
    .json(ruta_origen)

#agrego al dataframe el campo "ts_process"
archivo_df = archivo_raiz.withColumn("ts_process", F.current_timestamp() )

deltastream = archivo_df\
    .writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("mergeSchema", True)\
    .option("checkpointLocation", ruta_chequeo)\
    .toTable(nombre_tabla)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Monitoero de Streaming
print(deltastream.status)
print(deltastream.lastProgress)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Detener 
deltastream.stop()

print(deltastream.status)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# reiniciar 
# volver a ejecutar celda 2

print(deltastream.status)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
