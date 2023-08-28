import requests
import psycopg2
from dotenv import load_dotenv
import os

# Función para obtener la información de los casos de COVID-19 de la api covid-api
def get_casos(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanza una excepción si la respuesta no es exitosa (código de estado >= 400)
        data = response.json()
        return data
    except requests.exceptions.RequestException:
        None

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

"""
Los parámetros de fecha y paginación son temporales y sirven para demostrar el correcto funcionamiento del script,
tengo pensado implementar un dag en airflow que corra por día y traiga los datos de los casos de acuerdo a la fecha puesta en una tabla paramétrica
"""
fecha = '2021-10-25'
paginacion = '20'
url = f'https://covid-api.com/api/reports?date={fecha}&per_page={paginacion}'
respuesta = get_casos(url)

# Realizamos la conexión a la base de datos en RedShift
load_dotenv()
host_RS = os.getenv("HOST_RS")
db_name_RS = os.getenv("DB_NAME_RS")
user_RS = os.getenv("USER_RS")
passw_RS = os.getenv("PASS_RS")
port_RS = os.getenv("PORT_RS")

conn = get_conection(host_RS, db_name_RS, user_RS, passw_RS, port_RS)
cur = conn.cursor()
create_table_query = '''
CREATE TABLE IF NOT EXISTS villegas_axel_coderhouse.casos(
    fecha date,
    confirmados int,
    dif_confirm_ant int,
    muertes int,
    dif_muertes_ant int,
    recuperados int,
    dif_recup_ant int,
    activos int,
    dif_activos_ant int,
    tasa_mortalidad float,
    region varchar(25)
)
'''
cur.execute(create_table_query)
conn.commit()
cur.close()
conn.close()
