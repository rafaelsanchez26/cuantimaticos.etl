from binance.um_futures import UMFutures
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os, pytz, time
import polars as pl

def date_string_to_timestamp( date: str, time_zone: str ) -> int:

    '''
    Convierte una cadena de texto de fecha y hora en formato 'YYYY-MM-DD HH:MM:SS' a una marca de tiempo en milisegundos,
    ajustado a una zona horaria específica.

    Parámetros:
        date (str): Fecha y hora como cadena de texto, con formato 'YYYY-MM-DD HH:MM:SS'.
        time_zone (str): Zona horaria (por ejemplo, 'America/New_York') según la base de datos de zonas horarias de 'pytz'.

    Retorna:
        int: Marca de tiempo en milisegundos (desde el 1 de enero de 1970 UTC), ajustado a la zona horaria proporcionada.

    Ejemplo:
        date_string_to_timestamp( '2025-04-21 12:00:00', 'America/New_York' )
    '''
    
    # Convierte una cadena de texto de fecha y hora en una marca de tiempo en milisegundos.
    timestamp = int( pytz.timezone( time_zone ).localize( datetime.strptime( date, '%Y-%m-%d %H:%M:%S' ) ).timestamp() * 1000 )

    # Retorna una marca de timepo en milisegundos.
    return timestamp

def get_yesterday_date_string() -> str:

    '''
    Obtiene una cadena de texto con la fecha y hora de ayer, usando la zona horaria de Nueva York,
    y fija la hora a las 23:30:00 (11:30 PM).

    Retorna:
        str: Fecha y hora en formato 'YYYY-MM-DD HH:MM:SS'.
    
    Ejemplo de salida:
        '2025-04-20 23:30:00'
    '''

    # Obtiene una cadena de texto con la fecha y hora de ayer.
    yesterday_date = datetime.now( pytz.timezone( 'America/New_York' ) ) - timedelta( days=1 )

    # Fija la fecha y hora a las 23:30:00 (11:30 PM) en formato 'YYYY-MM-DD HH:MM:SS'.
    yesterday_date_string = yesterday_date.replace( hour=23, minute=30, second=0, microsecond=0 ).strftime('%Y-%m-%d %H:%M:%S')
    
    # Retorna una cadena de texto con la fecha y hora de ayer.
    return yesterday_date_string

################################################## EXTRAER

# Carga las variables de entorno desde el archivo .env (contiene información ultrasecreta que nadie debe saber).
load_dotenv()

# Inicializa un cliente para interactuar con la API de 'Binance Futures', utilizando las claves API de de las variables de entorno.
client = UMFutures( key=os.getenv( 'API_KEY' ), secret=os.getenv( 'SECRET_KEY' ) )

ticker = 'BTCUSDT'
timeframe = '30m'
limit = 1000
start_date = date_string_to_timestamp( '2019-9-8 0:0:0', 'America/New_York' )
end_date = date_string_to_timestamp( get_yesterday_date_string(), 'America/New_York' )

# Obtener respuesta de la API.
response = list( client.klines( symbol=ticker, interval=timeframe, startTime=start_date, endTime=end_date, limit=limit ) )

time.sleep( 1 )

while response[-1][0] < end_date:

    start_date = response[-1][0] + 1000
    response.extend( client.klines( symbol=ticker, interval=timeframe, startTime=start_date, endTime=end_date, limit=limit ) )
    time.sleep( 1 )

# Crear un dataframe con la respuesta de la API.
dataframe = pl.DataFrame( data=response, schema=[ 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time', 'Quote_asset_volume', 'Number_of_trades', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore' ], orient="row" )

################################################## TRANSFORM

# Eliminar las columnas que no son necesarias
dataframe = dataframe.drop( [ 'Close_time', 'Quote_asset_volume', 'Number_of_trades', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore' ] )

# Convertir la columna de fecha de timestamp a datetime.
dataframe = dataframe.with_columns(
    pl.col( 'Date' ).cast( pl.Datetime( 'ms' ) ).dt.convert_time_zone( 'America/New_York' ).alias( 'Date' )
)

# Convertir la columna de fecha de datetime a string.
dataframe = dataframe.with_columns(
    pl.col( 'Date' ).dt.strftime( '%d/%m/%Y %H:%M:%S' )
)

# Filtrar por fecha
#filtered_dataframe = dataframe.filter(
    #( pl.col('Date') >= pl.datetime( 2024, 11, 7, 8, 30, 0 ) ) & ( pl.col('Date') <= pl.datetime( 2024, 11, 7, 15, 0, 0 ) )
#).select( 'Open' )[-1]

##################################################

print( dataframe )
dataframe.write_csv( 'binancefutures_btcusdtp.csv' )