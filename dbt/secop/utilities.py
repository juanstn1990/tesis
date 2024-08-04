import duckdb

# Conectar a DuckDB
duck = duckdb.connect(database='/home/juanstn/git/duck_dbt/duck_dbt/duckdb_ddbb/finkargo.duckdb')

# Listar todas las tablas
tables = duck.execute("SHOW TABLES").fetchall()

# Imprimir la lista de tablas
print("Tablas en la base de datos:")
for table in tables:
    print(table[0])

# Verificar si la tabla DIM_PROVIDERS_CO existe
if ('DIM_PROVIDERS_CO',) in tables:
    # Ejecutar la consulta
    df = duck.execute("SELECT * FROM DIM_PROVIDERS_CO LIMIT 5").fetchdf()
    # Imprimir los resultados
    print(df)
else:
    print("La tabla DIM_PROVIDERS_CO no existe en la base de datos.")
