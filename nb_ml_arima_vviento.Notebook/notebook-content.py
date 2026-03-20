# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dc09f737-fad5-437e-acbd-4645f677d903",
# META       "default_lakehouse_name": "lh_gold_walter",
# META       "default_lakehouse_workspace_id": "fb0905fd-76e1-4862-81bf-a8ad86ea00d6",
# META       "known_lakehouses": [
# META         {
# META           "id": "dc09f737-fad5-437e-acbd-4645f677d903"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Importar librerias necesarias

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Librerías para análisis temporal
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

# Librerías para ARIMA
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.seasonal import seasonal_decompose

# Métricas
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

import warnings
warnings.filterwarnings('ignore')

print("✓ Librerías importadas correctamente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# paso 2 importamos la data para el modelo

df_base = spark.read.table("dbo.ml_train_data_viento_promedio")

#muestra informacion basica de lo importado
print(f"Total de registros: {df_base.count():,} ")
print(f"Total de colummnas: {df_base.columns} ")
df_base.printSchema()

#muestra vista previa de los datos

df_base.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Estadísticas descriptivas
print("=" * 80)
print("ESTADÍSTICAS DESCRIPTIVAS")
print("=" * 80)
display(df_base.select("vviento_promedio").describe())

# Verificar valores nulos
print("\n" + "=" * 80)
print("VALORES NULOS")
print("=" * 80)
null_counts = df_base.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in df_base.columns]
)
display(null_counts)

# Distribución por estación
print("\n" + "=" * 80)
print("DISTRIBUCIÓN POR ESTACIÓN")
print("=" * 80)
display(df_base.groupBy("CodigoNacional").count().orderBy(desc("count")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transformamos de Spark a Pandas y aseguramos tipos correctos
df_pandas = df_base.toPandas()

# Forzar tipo datetime desde el origen
df_pandas['fecha'] = pd.to_datetime(df_pandas['fecha'])
df_pandas['CodigoNacional'] = df_pandas['CodigoNacional'].astype(int)

print(f"Tipo de columna fecha : {df_pandas['fecha'].dtype}")
print(f"Rango de fechas       : {df_pandas['fecha'].min()} → {df_pandas['fecha'].max()}")
print(f"\nPrimeros registros:")
display(df_pandas.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtrar por una estación
df_estacion = df_pandas[df_pandas['CodigoNacional'] == 400009].copy()
df_estacion = df_estacion.set_index('fecha')
df_estacion = df_estacion.sort_index()
df_estacion = df_estacion[['vviento_promedio']]
df_estacion = df_estacion[~df_estacion.index.duplicated(keep='first')]

print(f"Rango  : {df_estacion.index.min()} → {df_estacion.index.max()}")
print(f"Filas  : {df_estacion.shape[0]}")
print(f"Nulos  : {df_estacion['vviento_promedio'].isna().sum()}")
print(df_estacion.head(10).to_string())  # ← reemplaza display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Crear gráfico de serie temporal - Viento Promedio
fig, ax = plt.subplots(1, 1, figsize=(15, 5))

ax.plot(df_estacion.index, df_estacion['vviento_promedio'],
        color='steelblue', linewidth=1, label='Viento Promedio')
ax.set_title('Serie Temporal - Velocidad de Viento Promedio (Estación 400009)', 
             fontsize=14, fontweight='bold')
ax.set_xlabel('Fecha')
ax.set_ylabel('Velocidad (m/s)')
ax.legend()
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 6: Test de Estacionariedad (Augmented Dickey-Fuller)
# ============================================================================
def test_estacionariedad(serie, nombre):
    """Ejecuta test ADF para verificar estacionariedad"""
    print(f"\n{'=' * 80}")
    print(f"TEST DE ESTACIONARIEDAD - {nombre}")
    print('=' * 80)
    
    resultado = adfuller(serie.dropna())
    
    print(f'Estadístico ADF: {resultado[0]:.6f}')
    print(f'p-value:         {resultado[1]:.6f}')
    print(f'Valores críticos:')
    for key, value in resultado[4].items():
        print(f'\t{key}: {value:.3f}')
    
    if resultado[1] <= 0.05:
        print(f"\n✓ La serie ES ESTACIONARIA (p-value <= 0.05)")
        return True
    else:
        print(f"\n✗ La serie NO ES ESTACIONARIA (p-value > 0.05)")
        print("  → Se requiere diferenciación")
        return False

# Test para Viento Promedio
es_estacionaria_viento = test_estacionariedad(
    df_estacion['vviento_promedio'], 'VIENTO PROMEDIO'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 7: Análisis ACF y PACF para determinar parámetros ARIMA
# ============================================================================
def plot_acf_pacf(serie, nombre, lags=40):
    """Grafica ACF y PACF para identificar parámetros p y q"""
    fig, axes = plt.subplots(2, 1, figsize=(15, 8))
    
    plot_acf(serie.dropna(), lags=lags, ax=axes[0])
    axes[0].set_title(f'Autocorrelación (ACF) - {nombre}', 
                      fontsize=12, fontweight='bold')
    
    plot_pacf(serie.dropna(), lags=lags, ax=axes[1])
    axes[1].set_title(f'Autocorrelación Parcial (PACF) - {nombre}', 
                      fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.show()

print("ANÁLISIS ACF/PACF - VELOCIDAD DE VIENTO PROMEDIO")
plot_acf_pacf(df_estacion['vviento_promedio'], 'Viento Promedio')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 8: Descomposición estacional de la serie
# ============================================================================
# Período 365 para datos diarios (anual), o 30 si prefieres mensual
decomposition_viento = seasonal_decompose(
    df_estacion['vviento_promedio'].dropna(),
    model='additive',
    period=365
)

fig = decomposition_viento.plot()
fig.set_size_inches(15, 10)
fig.suptitle('Descomposición Estacional - Viento Promedio (Estación 400009)', 
             fontsize=16, fontweight='bold', y=1.02)
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 9: División de datos en entrenamiento y prueba
# ============================================================================
train_size = int(len(df_estacion) * 0.8)

# Series de entrenamiento y prueba
train_viento = df_estacion['vviento_promedio'].iloc[:train_size]
test_viento  = df_estacion['vviento_promedio'].iloc[train_size:]

print("=" * 80)
print("DIVISIÓN DE DATOS")
print("=" * 80)
print(f"Total de registros  : {len(df_estacion)}")
print(f"Entrenamiento       : {len(train_viento)} ({len(train_viento)/len(df_estacion)*100:.1f}%)")
print(f"Prueba              : {len(test_viento)}  ({len(test_viento)/len(df_estacion)*100:.1f}%)")
print(f"\nPeriodo entrenamiento: {train_viento.index[0].date()} → {train_viento.index[-1].date()}")
print(f"Periodo prueba       : {test_viento.index[0].date()} → {test_viento.index[-1].date()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 10: Búsqueda de mejores parámetros ARIMA (Grid Search)
# ============================================================================
def buscar_mejor_arima(serie_train, serie_test, nombre, p_range, d_range, q_range):
    """Búsqueda de mejores parámetros ARIMA usando grid search"""
    print(f"\n{'=' * 80}")
    print(f"BÚSQUEDA DE PARÁMETROS ÓPTIMOS - {nombre}")
    print('=' * 80)
    
    mejor_aic       = np.inf
    mejor_orden     = None
    mejor_modelo    = None
    resultados      = []
    
    total_combinaciones  = len(p_range) * len(d_range) * len(q_range)
    combinacion_actual   = 0
    
    for p in p_range:
        for d in d_range:
            for q in q_range:
                combinacion_actual += 1
                try:
                    modelo     = ARIMA(serie_train, order=(p, d, q))
                    modelo_fit = modelo.fit()
                    
                    if modelo_fit.aic < mejor_aic:
                        mejor_aic    = modelo_fit.aic
                        mejor_orden  = (p, d, q)
                        mejor_modelo = modelo_fit
                    
                    resultados.append({
                        'orden': (p, d, q),
                        'aic'  : modelo_fit.aic,
                        'bic'  : modelo_fit.bic
                    })
                    
                    if combinacion_actual % 10 == 0:
                        print(f"Progreso: {combinacion_actual}/{total_combinaciones} "
                              f"- Mejor AIC: {mejor_aic:.2f} {mejor_orden}")
                    
                except Exception:
                    continue
    
    print(f"\n✓ Búsqueda completada")
    print(f"Mejor orden ARIMA : {mejor_orden}")
    print(f"Mejor AIC         : {mejor_aic:.2f}")
    
    df_resultados = pd.DataFrame(resultados).sort_values('aic').head(10)
    print(f"\nTop 10 mejores modelos:")
    print(df_resultados.to_string(index=False))
    
    return mejor_modelo, mejor_orden

# Rangos de parámetros
p_values = range(0, 4)   # AR
d_values = range(0, 3)   # Diferenciación
q_values = range(0, 4)   # MA

modelo_viento, orden_viento = buscar_mejor_arima(
    train_viento, test_viento, 'VIENTO PROMEDIO',
    p_values, d_values, q_values
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 11: Entrenar modelo ARIMA final
# ============================================================================
print("=" * 80)
print("ENTRENAMIENTO MODELO ARIMA FINAL - VIENTO PROMEDIO")
print("=" * 80)

print(f"\n→ Entrenando ARIMA{orden_viento} para Viento Promedio...")
modelo_final_viento  = ARIMA(train_viento, order=orden_viento)
modelo_fit_viento    = modelo_final_viento.fit()

print("\nResumen del modelo:")
print(modelo_fit_viento.summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 12: Realizar predicciones
# ============================================================================
print("=" * 80)
print("GENERANDO PREDICCIONES")
print("=" * 80)

predicciones_viento = modelo_fit_viento.forecast(steps=len(test_viento))

print(f"✓ Predicciones generadas para {len(test_viento)} periodos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 13: Evaluación del modelo
# ============================================================================
def evaluar_modelo(y_true, y_pred, nombre):
    """Calcula métricas de evaluación"""
    mse  = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    mae  = mean_absolute_error(y_true, y_pred)
    r2   = r2_score(y_true, y_pred)
    mape = np.mean(np.abs((y_true - y_pred) / y_true.replace(0, np.nan))) * 100

    print(f"\n{'=' * 80}")
    print(f"MÉTRICAS DE EVALUACIÓN - {nombre}")
    print('=' * 80)
    print(f"MSE  (Error Cuadrático Medio)    : {mse:.4f}")
    print(f"RMSE (Raíz del MSE)              : {rmse:.4f}")
    print(f"MAE  (Error Absoluto Medio)      : {mae:.4f}")
    print(f"R²   (Coeficiente Determinación) : {r2:.4f}")
    print(f"MAPE (Error Porcentual Abs.)      : {mape:.2f}%")
    
    return {'MSE': mse, 'RMSE': rmse, 'MAE': mae, 'R2': r2, 'MAPE': mape}

# Nota: reemplaza división directa por replace(0, nan) para evitar
# división por cero en registros con viento = 0 m/s
metricas_viento = evaluar_modelo(test_viento, predicciones_viento, 'VIENTO PROMEDIO')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 14: Visualización predicciones vs valores reales
# ============================================================================
fig, ax = plt.subplots(1, 1, figsize=(16, 6))

ax.plot(train_viento.index, train_viento,
        label='Entrenamiento', color='steelblue', linewidth=1)
ax.plot(test_viento.index, test_viento,
        label='Real (Test)', color='green', linewidth=1.5)
ax.plot(test_viento.index, predicciones_viento,
        label='Predicción', color='red', linewidth=1.5, linestyle='--')
ax.set_title(f'Predicción ARIMA{orden_viento} - Velocidad de Viento Promedio (Estación 400009)',
             fontsize=14, fontweight='bold')
ax.set_xlabel('Fecha')
ax.set_ylabel('Velocidad (m/s)')
ax.legend(loc='best')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 15: Análisis de residuales
# ============================================================================
fig, axes = plt.subplots(1, 2, figsize=(16, 5))

residuales_viento = test_viento.values - predicciones_viento.values

axes[0].plot(residuales_viento, color='steelblue', linewidth=1)
axes[0].axhline(y=0, color='black', linestyle='--', linewidth=1)
axes[0].set_title('Residuales - Viento Promedio', fontweight='bold')
axes[0].set_xlabel('Observación')
axes[0].set_ylabel('Residual (m/s)')
axes[0].grid(True, alpha=0.3)

axes[1].hist(residuales_viento, bins=30, color='steelblue', alpha=0.7, edgecolor='black')
axes[1].set_title('Distribución de Residuales - Viento Promedio', fontweight='bold')
axes[1].set_xlabel('Residual (m/s)')
axes[1].set_ylabel('Frecuencia')
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 16: Predicción futura (próximos 365 días)
# ============================================================================
print("=" * 80)
print("PREDICCIÓN FUTURA - PRÓXIMOS 365 DÍAS")
print("=" * 80)

# Reentrenar con todos los datos
modelo_completo_viento = ARIMA(df_estacion['vviento_promedio'], order=orden_viento)
fit_completo_viento    = modelo_completo_viento.fit()

periodos_futuros = 365
forecast_viento  = fit_completo_viento.forecast(steps=periodos_futuros)

# Fechas futuras diarias
ultima_fecha   = df_estacion.index[-1]
fechas_futuras = pd.date_range(
    start=ultima_fecha + pd.DateOffset(days=1),
    periods=periodos_futuros,
    freq='D'
)

df_forecast = pd.DataFrame({
    'fecha'               : fechas_futuras,
    'vviento_pred_ms'     : forecast_viento.values
})

print(f"\nPredicciones para los próximos {periodos_futuros} días:")
print(df_forecast.head(15).to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 17: Visualización de predicciones futuras
# ============================================================================
fig, ax = plt.subplots(1, 1, figsize=(16, 6))

# Mostrar solo los últimos 2 años del histórico para no saturar el gráfico
historico_reciente = df_estacion['vviento_promedio'].last('730D')

ax.plot(historico_reciente.index, historico_reciente,
        label='Histórico (últimos 2 años)', color='steelblue', linewidth=1.5)
ax.plot(fechas_futuras, forecast_viento,
        label='Predicción Futura (365 días)', color='red',
        linewidth=1.5, linestyle='--')
ax.set_title('Predicción Futura - Velocidad de Viento Promedio (Estación 400009)',
             fontsize=14, fontweight='bold')
ax.set_xlabel('Fecha')
ax.set_ylabel('Velocidad (m/s)')
ax.legend(loc='best')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 18: Guardar resultados en Delta Lake
# ============================================================================
from pyspark.sql.functions import lit, current_timestamp

print("=" * 80)
print("GUARDANDO RESULTADOS EN DELTA LAKE")
print("=" * 80)

df_forecast_spark = spark.createDataFrame(df_forecast)

df_forecast_final = (
    df_forecast_spark
    .withColumn("CodigoNacional",   lit(400009))
    .withColumn("modelo_viento",    lit(f"ARIMA{orden_viento}"))
    .withColumn("fecha_generacion", current_timestamp())
    .withColumn("rmse_viento",      lit(metricas_viento['RMSE']))
    .withColumn("r2_viento",        lit(metricas_viento['R2']))
    .withColumn("mape_viento",      lit(metricas_viento['MAPE']))
)

tabla_salida = "gold_wcl.dbo.predicciones_viento_arima"

df_forecast_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tabla_salida)

print(f"✓ Predicciones guardadas en: {tabla_salida}")
print(df_forecast_final.toPandas().head(10).to_string(index=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELDA 19: Resumen final
# ============================================================================
resumen = f"""
╔════════════════════════════════════════════════════════════════════════════╗
║              RESUMEN DE MODELO ARIMA - VELOCIDAD DE VIENTO                 ║
╚════════════════════════════════════════════════════════════════════════════╝

📊 DATOS PROCESADOS:
   • Estación         : 400009
   • Total registros  : {len(df_estacion)}
   • Periodo          : {df_estacion.index[0].strftime('%Y-%m-%d')} → {df_estacion.index[-1].strftime('%Y-%m-%d')}
   • Entrenamiento    : {len(train_viento)} registros ({len(train_viento)/len(df_estacion)*100:.1f}%)
   • Prueba           : {len(test_viento)} registros  ({len(test_viento)/len(df_estacion)*100:.1f}%)

💨 VIENTO PROMEDIO:
   • Modelo           : ARIMA{orden_viento}
   • RMSE             : {metricas_viento['RMSE']:.4f} m/s
   • MAE              : {metricas_viento['MAE']:.4f} m/s
   • R²               : {metricas_viento['R2']:.4f}
   • MAPE             : {metricas_viento['MAPE']:.2f}%

🔮 PREDICCIONES FUTURAS:
   • Periodos         : {periodos_futuros} días
   • Desde            : {fechas_futuras[0].strftime('%Y-%m-%d')}
   • Hasta            : {fechas_futuras[-1].strftime('%Y-%m-%d')}

💾 ALMACENAMIENTO:
   • Lakehouse        : gold_wcl
   • Tabla            : {tabla_salida}
   • Formato          : Delta Lake

✅ MODELO COMPLETADO EXITOSAMENTE
"""
print(resumen)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
