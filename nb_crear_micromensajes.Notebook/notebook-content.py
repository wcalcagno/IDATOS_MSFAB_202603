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

#Proposito del notebook es crear mensajes en formato JSON simulando un dispositivo IoT.

#Importar las librerias que utilizaremos

import os 
import json
import uuid
import random
import time 
from datetime import datetime
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import pyspark.sql.functions as F

#configuracion

ruta_destino = "/lakehouse/default/Files/sensor_simulado"
num_mensajes = 12
seg_espera = 3

# verificacion
os.makedirs(ruta_destino, exist_ok=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in range(num_mensajes):

    # variables generales
    ahora = datetime.utcnow()
    ts_str = ahora.strftime("%Y%m%dT%H%M%S")


    #estructura mensaje

    registro = {
        "id": str(uuid.uuid4())  ,
        "temp": round(random.uniform(-2.0,38.0),2) ,
        "humedad": round(random.uniform(0.0,1.0),2),
        "vviento": round(random.uniform(0.0,10.0),0),
        "timestamp": ahora.isoformat() 
    }

    #construyamos el Archivo / Fichero JSON

    filename = f"sensor_{ts_str}.json"
    filepath = os.path.join(ruta_destino, filename)

    #materializar el fichero /archivo
    with open(filepath,"w") as f:
        json.dump(registro, f)

    #mensaje de OK
    print(f"OK-[{i+1} / {num_mensajes}] generado : {filename}")
    time.sleep(seg_espera)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
