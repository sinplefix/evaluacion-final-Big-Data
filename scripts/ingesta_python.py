import os
import sqlite3
import pandas as pd

# ============================
# CONFIGURACIÓN DE RUTAS
# ============================
# Carpeta raíz del proyecto (LIDL_PROJECT)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RUTA_DATA       = os.path.join(BASE_DIR, "data")
RUTA_BRONZE_DIR = os.path.join(BASE_DIR, "bronze", "ventas")

RUTA_CSV_INFO  = os.path.join(RUTA_DATA, "clientes_info.csv")
RUTA_TXT_EXTRA = os.path.join(RUTA_DATA, "clientes_extra.txt")
RUTA_SQL       = os.path.join(RUTA_DATA, "clientes.sql")

os.makedirs(RUTA_BRONZE_DIR, exist_ok=True)
RUTA_SALIDA_BRONZE = os.path.join(RUTA_BRONZE_DIR, "ventas_bronze.csv")

# ============================
# 1. CARGA DE CADA ARCHIVO
# ============================

def cargar_csv_info():
    print(f"Cargando CSV principal: {RUTA_CSV_INFO}")
    df = pd.read_csv(RUTA_CSV_INFO)
    return df

def cargar_txt_extra():
    """
    Lee el TXT de clientes_extra.
    Si da error, cambia sep=";" por sep=",".
    """
    print(f"Cargando TXT extra: {RUTA_TXT_EXTRA}")
    df = pd.read_csv(RUTA_TXT_EXTRA, sep=";")
    return df

def cargar_desde_sql():
    """
    Carga datos desde clientes.sql usando SQLite en memoria.

    Se asume que el archivo crea una tabla llamada 'clientes'.
    Si es otro nombre, cambia TABLE_NAME_SQL.
    """
    TABLE_NAME_SQL = "clientes"

    print(f"Cargando datos desde SQL: {RUTA_SQL}")
    with open(RUTA_SQL, "r", encoding="utf-8") as f:
        script_sql = f.read()

    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.executescript(script_sql)
    conn.commit()

    try:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME_SQL}", conn)
    except Exception as e:
        print(f"Error al leer la tabla '{TABLE_NAME_SQL}' desde SQLite: {e}")
        df = pd.DataFrame()

    conn.close()
    return df

# ============================
# 2. VALIDACIÓN BÁSICA
# ============================

def validar_campos(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # limpiar nombres de columnas
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # eliminar filas completamente vacías
    df = df.dropna(how="all")

    # eliminar duplicados
    df = df.drop_duplicates()

    # ejemplo: parsear fecha si existe columna 'fecha'
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    return df

# ============================
# 3. PIPELINE COMPLETO
# ============================

def main():
    df_csv = cargar_csv_info()
    df_txt = cargar_txt_extra()
    df_sql = cargar_desde_sql()

    # unir fuentes que no estén vacías
    dfs = [df for df in [df_csv, df_txt, df_sql] if not df.empty]
    if not dfs:
        print("No se cargaron datos desde ninguna fuente.")
        return

    df_total = pd.concat(dfs, ignore_index=True)

    # tomar solo 500 registros
    df_total = df_total.head(500)

    # validar
    df_validado = validar_campos(df_total)

    # guardar en BRONZE
    df_validado.to_csv(RUTA_SALIDA_BRONZE, index=False, encoding="utf-8")
    print("\n✅ Ingesta completada.")
    print(f"   Registros finales (Bronze): {len(df_validado)}")
    print(f"   Archivo generado: {RUTA_SALIDA_BRONZE}")

if __name__ == "__main__":
    main()
