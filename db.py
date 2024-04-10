import csv, sys
import MySQLdb

# Conexión a la base de datos
try:
    db = MySQLdb.connect('localhost', 'root', '', 'provincias_argentinas')
except MySQLdb.Error as error:
    print('Error al intentar conectar la base de datos', error)
    sys.exit()
print('Conexión exitosa')


# Creación de la tabla
cursor = db.cursor()

tabla_loc = 'localidades'
colums = [
    'provincia VARCHAR(255)',
    'id INT',
    'localidad VARCHAR(255)',
    'cp INT',
    'id_prov_mstr INT'
]


def create_table(tabla_loc, colums):
    cursor.execute(f"DROP TABLE IF EXISTS {tabla_loc}")
    cursor.execute(f"CREATE TABLE {tabla_loc} ({', '.join(colums)})")
    if cursor:
        print(f"Se ha creado la tabla {tabla_loc}")

create_table(tabla_loc, colums)

# Insertar datos
def insert_date():
    with open('localidades.csv', 
          newline='', 
          mode='r', 
          encoding='utf-8') as csv_file:
        lectura_csv = csv.reader(csv_file,                 
                                delimiter=',', 
                                quotechar='"')
        next(lectura_csv)
        try:
            for row in lectura_csv:
                cursor.execute(f"INSERT INTO {tabla_loc} (provincia, id, localidad, cp, id_prov_mstr) VALUES ( %s, %s, %s, %s, %s )", row[0:5])
        except csv.Error as e:
            sys.exit('file {}, line {}: {}'.format(csv_file, lectura_csv.line_num, e)) 
    try:
        db.commit()
        print('Datos insertados correctamente.')
    except:
        db.rollback()

insert_date()

# Consulta de la cantidad de filas
consulta = "SELECT * FROM localidades"
cursor.execute(consulta)
print(cursor.rowcount, "filas insertadas.")


# Consulta para agrupar localidades por provincia
def group_by_provincia():
    cursor.execute("""
        SELECT provincia, COUNT(*) AS total_localidades,
            ROW_NUMBER() OVER (ORDER BY provincia) AS numero_provincia
        FROM localidades
        GROUP BY provincia
        ORDER BY provincia
    """)
    results = cursor.fetchall()
    for provincia, total_localidades, numero_provincia in results:
        print(f"Provincia {numero_provincia}: {provincia} - Cantidad de localidades: {total_localidades}")

group_by_provincia()

db.close()