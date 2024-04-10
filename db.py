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
                cursor.execute(f"INSERT INTO {tabla_loc} (provincia, id, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s)", row[0:5])
        except csv.Error as e:
            sys.exit('file {}, line {}: {}'.format(csv_file, lectura_csv.line_num, e)) 

    db.commit()
    print('Datos insertados correctamente.')

insert_date()

# Contar filas insertadas
cursor.execute(f"SELECT COUNT(*) FROM {tabla_loc}")
num_filas = cursor.fetchone()[0]
print(f"Total de filas insertadas: {num_filas}")

db.close()