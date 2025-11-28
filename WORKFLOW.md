# WORKFLOW DEL PIPELINE – EQUIPO A (BRONZE → SILVER)

## 1. Ingesta de Datos (Capa Bronze)

### 1.1 Obtención de fuentes
Las fuentes provienen del repositorio del profesor e incluyen:
- `clientes_info.csv`
- `clientes_extra.txt`
- `clientes.sql`

### 1.2 Proceso de ingesta (`ingesta_python.py`)
El script realiza:
1. Lectura de los 3 archivos con Python/pandas.
2. Normalización de nombres de columnas.
3. Eliminación de duplicados y filas vacías.
4. Unión de datos con `pd.concat()`.
5. Selección de 500 registros.
6. Exportación a `/bronze/ventas/ventas_bronze.csv`.

---

## 2. Limpieza y Transformación (Capa Silver)

### 2.1 Proceso de limpieza (`limpieza_silver.py`)
El script realiza:
1. Normalización de texto (minúsculas, sin tildes, sin espacios).
2. Estandarización de fechas.
3. Manejo de nulos (sin eliminar filas).
4. Exportación a `/silver/ventas/ventas_silver.csv`.

---

## 3. Estructura Final del Proyecto

```
/bronze/
    /ventas/
        ventas_bronze.csv

/data/
    clientes_extra.txt
    clientes_info.csv
    clientes.sql


/silver/
    /ventas/
        ventas_silver.csv

/scripts/
    ingesta_python.py
    limpieza_silver.py

WORKFLOW.md
```

---

## 4. Flujo Completo del Pipeline

```
FUENTES (CSV + TXT + SQL)
          |
          v
   ingesta_python.py
          |
          v
  BRONZE → ventas_bronze.csv
          |
          v
  limpieza_silver.py
          |
          v
  SILVER → ventas_silver.csv
```

---

## 5. Entregables del Equipo A

- Capa Bronze (archivo generado)
- Capa Silver (archivo generado)
- Script de ingesta
- Script de limpieza
- Workflow (este archivo)
- Arquitectura / diagrama (opcional)
