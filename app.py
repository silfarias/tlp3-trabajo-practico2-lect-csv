import csv, sys


with open('localidades.csv', 
          newline='', 
          mode='r', 
          encoding='utf-8') as csv_file:
    lectura_csv = csv.reader(csv_file, 
                             
                             delimiter=',', 
                             quotechar='"')
    try:
        for row in lectura_csv:
            print(row)
    except csv.Error as e:
        sys.exit('file {}, line {}: {}'.format(csv_file, lectura_csv.line_num, e))   