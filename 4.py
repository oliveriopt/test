import pandas as pd
import argparse
import re

def extract_casted_columns_from_xlsx(xlsx_path, database, table_name):
    df = pd.read_excel(xlsx_path)
    df.columns = [col.strip().lower() for col in df.columns]

    if "." not in table_name:
        raise ValueError("table_name debe tener formato schema.table (e.g., locale.Address)")

    schema, table = table_name.lower().split(".")

    filtered = df[
        (df["table_catalog"].str.lower() == database.lower()) &
        (df["table_schema"].str.lower() == schema) &
        (df["table_name"].str.lower() == table)
    ]

    if filtered.empty:
        raise ValueError(f"No se encontraron columnas para {database}.{table_name}")

    casted = [f"CAST([{col}] AS NVARCHAR(MAX)) AS [{col}]" for col in filtered["column_name"]]
    return ",\n       ".join(casted)

def replace_generate_batch_queries(py_path, casted_select, output_py_path):
    with open(py_path, "r") as f:
        code = f.read()

    pattern = r"def generate_batch_queries\(.*?\):.*?\n(?=def|class|if __name__|$)"

    replacement = f"""def generate_batch_queries(table_name, batch_size, order_column, connection_string):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {{table_name}}")
    total_rows = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    queries = []
    for start in range(1, total_rows + 1, batch_size):
        end = start + batch_size - 1
        query = f\"\"\"
            SELECT * FROM (
                SELECT {casted_select},
                       ROW_NUMBER() OVER (ORDER BY {{order_column}}) AS rn
                FROM {{table_name}}
            ) AS numbered
            WHERE rn BETWEEN {{start}} AND {{end}}
        \"\"\"
        queries.append(query.strip())
    return queries

"""

    new_code = re.sub(pattern, replacement, code, flags=re.DOTALL)

    with open(output_py_path, "w") as f:
        f.write(new_code)

    print(f"Archivo modificado guardado en: {output_py_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--py", required=True, help="Ruta al archivo .py original")
    parser.add_argument("--xlsx", required=True, help="Ruta al archivo info_schema.xlsx")
    parser.add_argument("--database", required=True, help="Nombre de la base de datos (table_catalog)")
    parser.add_argument("--table", required=True, help="Nombre de la tabla en formato schema.table")
    parser.add_argument("--output", required=True, help="Ruta de salida para el nuevo archivo .py")
    args = parser.parse_args()

    casted_select = extract_casted_columns_from_xlsx(args.xlsx, args.database, args.table)
    replace_generate_batch_queries(args.py, casted_select, args.output)

if __name__ == "__main__":
    main()



python update_py_with_dynamic_query.py \
  --py sql_batch_pipeline_fixed.py \
  --xlsx info_schema.xlsx \
  --database XpoMaster \
  --table locale.Address \
  --output sql_batch_pipeline_locale_address.py
