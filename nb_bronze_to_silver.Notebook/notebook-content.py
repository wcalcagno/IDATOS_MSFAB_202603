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
# META         },
# META         {
# META           "id": "8197d5f6-e14a-46b9-8bf7-97e66f671a08"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_bronze = spark.read.table("lh_bronze_wcl.dbo.viento_dmc")
display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ft_viento = spark.sql(
"""
SELECT 
time, dd_valor, ff_valor, VRB_Valor, CodigoNacional
FROM lh_bronze_wcl.dbo.viento_dmc
Where dd_valor is not null OR ff_valor is not NULL or vrb_valor is not NULL
"""
)

df_dim_estaciones = spark.sql(
"""
SELECT 
distinct (CodigoNacional) ,
latitud, 
longitud,
nombreEstacion
FROM lh_bronze_wcl.dbo.viento_dmc


"""
)


display(df_ft_viento)
display(df_dim_estaciones)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_estaciones.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.dim_estaciones")
df_ft_viento.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.ft_vientos")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
