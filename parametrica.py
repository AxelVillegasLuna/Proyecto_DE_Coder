import psycopg2
from dotenv import load_dotenv
import os

# Función para realizar la conexión con la base de datos en RedShift
def get_conection(host_RS, db_name_RS, user_RS, passw_RS, port_RS):
    try:
        conn = psycopg2.connect(
            host = host_RS,
            dbname = db_name_RS,
            user = user_RS,
            password = passw_RS,
            port = port_RS
        )
        return conn
    except Exception:
        return False

# Cargamos los datos para la conexión desde las variables de entorno
load_dotenv()
host_RS = os.getenv("HOST_RS")
db_name_RS = os.getenv("DB_NAME_RS")
user_RS = os.getenv("USER_RS")
passw_RS = os.getenv("PASS_RS")
port_RS = os.getenv("PORT_RS")

# Realizamos la conexión a la base de datos en RedShift
conn = get_conection(host_RS, db_name_RS, user_RS, passw_RS, port_RS)
cur = conn.cursor()

# Creamos la tabla en nuestro esquema
create_table_query = '''
CREATE TABLE IF NOT EXISTS villegas_axel_coderhouse.parametrica(
    proceso_nombre varchar(45),
    fecha date
)
'''
cur.execute(create_table_query)
conn.commit()

#Insertamos el registro del proceso con su fecha
insert_table_query = '''
INSERT INTO villegas_axel_coderhouse.parametrica VALUES (
    'PROCESO_COVID',
    to_date('2020-07-25','yyyy-mm-dd')
)
'''
cur.execute(insert_table_query)
conn.commit()

# Cerramos el cursor y la conexión a la base de datos
cur.close()
conn.close()