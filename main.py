import requests
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd

# Función para obtener la información de los casos de COVID-19 de la api covid-api
def get_casos(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanza una excepción si la respuesta no es exitosa (código de estado >= 400)
        data = response.json()
        return data
    except requests.exceptions.RequestException:
        None

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

# Seteo de ciertas estructuras necesarias para obtener la información requerida
fecha = '2020-07-25'
paises = ['BRA', 'COL', 'ECU', 'ARG', 'CHL', 'PER', 'PRY', 'BOL', 'URY', 'VEN']
datos_final = {'fecha': [],
               'confirmados': [],
               'dif_confirmados': [],
               'muertes': [],
               'dif_muertes_ant': [],
               'recuperados': [],
               'dif_recup_ant': [],
               'activos': [],
               'dif_activos_ant': [],
               'tasa_mortalidad': [],
               'region': []}

# Traemos la información de la API
for pais in paises:
    url = f'https://covid-api.com/api/reports?date={fecha}&iso={pais}&per_page=1'
    respuesta = get_casos(url)
    datos_final['fecha'].append(respuesta['data'][0]['date'])
    datos_final['confirmados'].append(respuesta['data'][0]['confirmed'])
    datos_final['dif_confirmados'].append(respuesta['data'][0]['confirmed_diff'])
    datos_final['muertes'].append(respuesta['data'][0]['deaths'])
    datos_final['dif_muertes_ant'].append(respuesta['data'][0]['deaths_diff'])
    datos_final['recuperados'].append(respuesta['data'][0]['recovered'])
    datos_final['dif_recup_ant'].append(respuesta['data'][0]['recovered_diff'])
    datos_final['activos'].append(respuesta['data'][0]['active'])
    datos_final['dif_activos_ant'].append(respuesta['data'][0]['active_diff'])
    datos_final['tasa_mortalidad'].append(respuesta['data'][0]['fatality_rate'])
    datos_final['region'].append(respuesta['data'][0]['region']['name'])

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

# Borramos los registros existentes en nuestra tabla para la fecha procesada
delete_table_query = f"""DELETE FROM villegas_axel_coderhouse.casos 
                        WHERE fecha = to_date('{fecha}', 'yyyy-mm-dd')"""
cur.execute(delete_table_query)
conn.commit()

# Creamos el DataFrame con pandas
df = pd.DataFrame(datos_final)

# Iteramos las filas del DataFrame para insertarlas en la tabla en RedShift
for index, row in df.iterrows():
    insert_table_query = f"""INSERT INTO villegas_axel_coderhouse.casos (fecha, confirmados, dif_confirm_ant, muertes, dif_muertes_ant, recuperados,
                            dif_recup_ant, activos, dif_activos_ant, tasa_mortalidad, region) VALUES (to_date('{row['fecha']}', 'yyyy-mm-dd'), {row['confirmados']}, {row['dif_confirmados']}, {row['muertes']},{row['dif_muertes_ant']}, {row['recuperados']}, {row['dif_recup_ant']}, {row['activos']}, {row['dif_activos_ant']}, {row['tasa_mortalidad']}, '{row['region']}')"""
    cur.execute(insert_table_query)
    conn.commit()

# Cerramos el cursor y la conexión a la base de datos
cur.close()
conn.close()