import csv, sys, os
import MySQLdb


# Conexión a la base de datos
try:
    db = MySQLdb.connect('localhost', 'root', '', 'provincias_argentinas')
except MySQLdb.Error as error:
    print('Error al intentar conectar la base de datos', error)
    sys.exit()
print('Conexión exitosa')


cursor = db.cursor()

# Creación de la tabla
tabla_loc = 'localidades'
colums = [
    'provincia VARCHAR(255)',
    'id INT',
    'localidad VARCHAR(255)',
    'cp INT',
    'id_prov_mstr INT'
]


def create_table(tabla_loc, colums):
    cursor.execute(f"DROP TABLE IF EXISTS {tabla_loc}") # Elimina la tabla si ya existe
    cursor.execute(f"CREATE TABLE {tabla_loc} ({', '.join(colums)})") # Crea la tabla
    if cursor:
        print(f"Se ha creado la tabla {tabla_loc}")

create_table(tabla_loc, colums)


# Inserta datos a la tabla
def insert_date():
    with open('localidades.csv', newline='', mode='r', encoding='utf-8') as csv_file:
        lectura_csv = csv.reader(csv_file, delimiter=',', quotechar='"')
        next(lectura_csv)
        try:
            for row in lectura_csv:
                cursor.execute(f"INSERT INTO {tabla_loc} (provincia, id, localidad, cp, id_prov_mstr) VALUES ( %s, %s, %s, %s, %s )", row[0:5])
        except csv.Error:
            sys.exit()
    try:
        db.commit()
        print('Datos insertados correctamente.')
        print("filas insertadas: ", cursor.execute("SELECT * FROM localidades"))
    except:
        db.rollback()

insert_date()


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


def create_csvs():
    consulta = "SELECT provincia, localidad FROM localidades ORDER BY provincia" # Obtener localidades agrupadas por provincias
    cursor.execute(consulta)
    results = cursor.fetchall()

    folder = 'localidades_por_provincia'
    if not os.path.exists(folder):
        os.makedirs(folder) # Si el directorio no existe, lo crea

    for provincia in results:
        with open(f'{folder}/{provincia[0]}.csv', 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['localidad'])

            cursor.execute(f"SELECT localidad FROM localidades WHERE provincia = '{provincia[0]}'")
            localidades = cursor.fetchall()

            for localidad in localidades:
                csv_writer.writerow([localidad[0]])

    print('Archivos CSV creados correctamente.')
    
create_csvs()


db.close()