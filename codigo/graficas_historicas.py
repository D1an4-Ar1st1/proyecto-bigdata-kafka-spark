
import glob
import os
import pandas as pd
import matplotlib.pyplot as plt

carpeta_csv = "/home/hadoop/proyecto_bigdata/resultados/resumen_diario"
carpeta_salida = "/home/hadoop/proyecto_bigdata/resultados/graficas"

os.makedirs(carpeta_salida, exist_ok=True)

archivos = glob.glob(os.path.join(carpeta_csv, "part-*.csv"))

if not archivos:
    raise FileNotFoundError(
        f"No se encontraron archivos CSV en {carpeta_csv}"
    )

df = pd.concat(
    [pd.read_csv(archivo) for archivo in archivos],
    ignore_index=True
)

df["fecha"] = pd.to_datetime(df["fecha"])
df = df.sort_values("fecha")

# Gráfica 1: temperatura promedio diaria
plt.figure(figsize=(12, 5))
plt.plot(
    df["fecha"],
    df["temperatura_promedio"],
    marker=".",
    linewidth=1
)
plt.title("Temperatura promedio diaria - Sensores históricos")
plt.xlabel("Fecha")
plt.ylabel("Temperatura (°C)")
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(
    os.path.join(carpeta_salida, "temperatura_promedio_diaria.png"),
    dpi=150
)
plt.close()

# Gráfica 2: humedad promedio diaria
plt.figure(figsize=(12, 5))
plt.plot(
    df["fecha"],
    df["humedad_promedio"],
    marker=".",
    linewidth=1
)
plt.title("Humedad relativa promedio diaria - Sensores históricos")
plt.xlabel("Fecha")
plt.ylabel("Humedad relativa (%)")
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(
    os.path.join(carpeta_salida, "humedad_promedio_diaria.png"),
    dpi=150
)
plt.close()

print(f"Fechas graficadas: {df['fecha'].min().date()} a {df['fecha'].max().date()}")
print(f"Cantidad de días con datos: {len(df)}")
print(f"Gráficas guardadas en: {carpeta_salida}")
print(" - temperatura_promedio_diaria.png")
print(" - humedad_promedio_diaria.png")
