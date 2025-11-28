import os
import pandas as pd

# ============================
# CONFIGURACIÓN DE RUTAS
# ============================
RUTA_BRONZE = "./bronze/ventas/ventas_bronze.csv"
RUTA_SILVER_DIR = "./silver/ventas"
os.makedirs(RUTA_SILVER_DIR, exist_ok=True)
RUTA_SILVER = os.path.join(RUTA_SILVER_DIR, "ventas_silver.csv")

# ============================
# 1. CARGAR BRONZE
# ============================
df = pd.read_csv(RUTA_BRONZE)

print("Shape Bronze:", df.shape)   # (filas, columnas)

# ============================
# 2. NORMALIZAR TEXTO
#    (sin tocar números)
# ============================
columnas_obj = df.select_dtypes(include="object").columns

for col in columnas_obj:
    df[col] = df[col].astype(str).str.strip().str.lower()
    df[col] = (
        df[col]
        .str.replace("á", "a")
        .str.replace("é", "e")
        .str.replace("í", "i")
        .str.replace("ó", "o")
        .str.replace("ú", "u")
    )

# ============================
# 3. ESTANDARIZAR FECHAS
#    (si hay columnas que contengan "fecha")
# ============================
for col in df.columns:
    if "fecha" in col:   # ej: fecha_compra, fecha_registro, etc.
        df[col] = pd.to_datetime(df[col], errors="coerce")

# ============================
# 4. MANEJO DE NULOS SUAVE
#    (solo rellenar, NO borrar filas)
# ============================
# Ejemplo: si existe promedio_compras y tiene nulos, rellenar con 0
if "promedio_compras" in df.columns:
    df["promedio_compras"] = df["promedio_compras"].fillna(0)

# Si existe codigo_cliente y tiene nulos, rellenar con -1
if "codigo_cliente" in df.columns:
    df["codigo_cliente"] = df["codigo_cliente"].fillna(-1)

# NO usamos dropna para no borrar registros

# ============================
# 5. GUARDAR EN SILVER
# ============================
df.to_csv(RUTA_SILVER, index=False, encoding="utf-8")

print("Shape Silver:", df.shape)
print("✅ Limpieza completada y guardada en:", RUTA_SILVER)
