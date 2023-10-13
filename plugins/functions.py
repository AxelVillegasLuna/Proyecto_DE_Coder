from datetime import datetime, timedelta
import os
import smtplib
import pytz
import requests
import psycopg2
import pandas as pd

# Cargamos los datos para la conexión desde las variables de entorno
host_RS = os.environ.get("HOST_RS")
db_name_RS = os.environ.get("DB_NAME_RS")
user_RS = os.environ.get("USER_RS")
passw_RS = os.environ.get("PASS_RS")
port_RS = os.environ.get("PORT_RS")

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
    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        return None  # Devuelve None en caso de error
    
# Función para obtener la fecha de la tabla paramétrica
def get_fecha():
    conn = get_conection(host_RS, db_name_RS, user_RS, passw_RS, port_RS)
    cur = conn.cursor()
    select_table_query = """SELECT fecha FROM villegas_axel_coderhouse.parametrica
                            WHERE proceso_nombre = 'PROCESO_COVID'"""
    cur.execute(select_table_query)
    fecha = cur.fetchone()[0]
    fecha_format = fecha.strftime('%Y-%m-%d')
    cur.close()
    conn.close()
    return fecha_format

def enviar_mail(mensaje, error=0):
  try:
      x=smtplib.SMTP('smtp.gmail.com',587)
      x.starttls()
      x.login('datawarehouse.axel@gmail.com','bbwm gpxc anxj drzk')
      subject=f'Consulta a la api de COVID - {datetime.now(pytz.timezone("America/Argentina/Buenos_Aires")).strftime("%Y-%m-%d %H:%M:%S")}'
      if error == 0:
        body_text=f'El proceso corrio de forma exitosa\nLos datos recolectados son:\n\n{mensaje}'
      else:
          body_text=f'El proceso tuvo un error:\n{mensaje}'
      message='Subject: {}\n\n{}'.format(subject,body_text)
      x.sendmail('datawarehouse.axel@gmail.com','villegaslunaaxel@gmail.com',message)
      print('Exito')
  except Exception as exception:
      print(exception)
      print('Error al enviar el mail')

# Seteo de ciertas estructuras necesarias para obtener la información requerida
fecha = get_fecha()
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

# Función para obtener la información de los casos de COVID-19 de la api covid-api
def get_casos(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanza una excepción si la respuesta no es exitosa (código de estado >= 400)
        data = response.json()
        return data
    except requests.exceptions.RequestException:
        None
# Creamos la tabla en nuestro esquema
def crear_tabla():
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

def cargar_informacion():
    try:
        mail = ''
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

            conn = get_conection(host_RS, db_name_RS, user_RS, passw_RS, port_RS)
            cur = conn.cursor()
            delete_table_query = f"""DELETE FROM villegas_axel_coderhouse.casos 
                                    WHERE fecha = to_date('{fecha}', 'yyyy-mm-dd')"""
            cur.execute(delete_table_query)
            conn.commit()

        # Creamos el DataFrame con pandas
        df = pd.DataFrame(datos_final)

        # Iteramos las filas del DataFrame para insertarlas en la tabla en RedShift
        for index, row in df.iterrows():
            insert_table_query = f"""INSERT INTO villegas_axel_coderhouse.casos (fecha, confirmados, dif_confirm_ant, muertes, dif_muertes_ant, recuperados,
                                    dif_recup_ant, activos, dif_activos_ant, tasa_mortalidad, region) VALUES (to_date('{row['fecha']}', 'yyyy-mm-dd'), {row['confirmados']}, {row['dif_confirmados']}, {row['muertes']}, {row['dif_muertes_ant']}, {row['recuperados']}, {row['dif_recup_ant']}, {row['activos']}, {row['dif_activos_ant']}, {row['tasa_mortalidad']}, '{row['region']}')"""
            cur.execute(insert_table_query)
            conn.commit()
            mail = mail + f"Fecha: {row['fecha']} - Pais: {row['region']} - Confirmados: {row['confirmados']} - Muertes: {row['muertes']} - Recuperados: {row['recuperados']}\n"
        cur.close()
        conn.close()
        enviar_mail(mail)
    except Exception as e:
        enviar_mail(e, 1)

def update_parametrica():
    conn = get_conection(host_RS, db_name_RS, user_RS, passw_RS, port_RS)
    cur = conn.cursor()
    # Una vez que insertados los registros, procedemos a actualizar la fecha de la tabla paramétrica
    update_table_query = f"""UPDATE villegas_axel_coderhouse.parametrica
                            SET fecha = fecha + INTERVAL '1 day'
                            WHERE proceso_nombre = 'PROCESO_COVID'"""
    cur.execute(update_table_query)
    conn.commit()
    cur.close()
    conn.close()



