from binance.um_futures import UMFutures
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
import polars as pl
import pytz, os, time

def date_string_to_timestamp(date: str, time_zone: str) -> int:
    return int(
        pytz.timezone(time_zone)
        .localize(datetime.strptime(date, '%Y-%m-%d %H:%M:%S'))
        .timestamp() * 1000
    )

def get_yesterday_date_string() -> str:
    yesterday_date = datetime.now(pytz.timezone('America/New_York')) - timedelta(days=1)
    return yesterday_date.replace(hour=23, minute=30, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

# Cargar variables de entorno
load_dotenv()

# Credenciales API Binance
client = UMFutures(
    key=os.getenv('API_KEY'),
    secret=os.getenv('SECRET_KEY')
)

# Parámetros
ticker = 'BTCUSDT'
timeframe = '30m'
limit = 1000
start_date = date_string_to_timestamp('2019-9-8 0:0:0', 'America/New_York')
end_date = date_string_to_timestamp(get_yesterday_date_string(), 'America/New_York')

# Descargar datos
response = list(client.klines(symbol=ticker, interval=timeframe, startTime=start_date, endTime=end_date, limit=limit))
time.sleep(1)

while response[-1][0] < end_date:
    start_date = response[-1][0] + 1000
    response.extend(client.klines(symbol=ticker, interval=timeframe, startTime=start_date, endTime=end_date, limit=limit))
    time.sleep(1)

# Crear dataframe
dataframe = pl.DataFrame(
    data=response,
    schema=[
        'Date', 'Open', 'High', 'Low', 'Close', 'Volume',
        'Close_time', 'Quote_asset_volume', 'Number_of_trades',
        'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore'
    ],
    orient="row"
)

# Transformar datos
dataframe = dataframe.drop([
    'Close_time', 'Quote_asset_volume', 'Number_of_trades',
    'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore'
])

dataframe = dataframe.with_columns(
    pl.col('Date').cast(pl.Datetime('ms')).dt.convert_time_zone('America/New_York').alias('Date')
)

dataframe = dataframe.with_columns(
    pl.col('Date').dt.strftime('%d/%m/%Y %H:%M:%S')
)

# Convertir tipos a los del modelo
dataframe = dataframe.with_columns([
    pl.col("Open").cast(pl.Decimal(precision=10, scale=2)),
    pl.col("High").cast(pl.Decimal(precision=10, scale=2)),
    pl.col("Low").cast(pl.Decimal(precision=10, scale=2)),
    pl.col("Close").cast(pl.Decimal(precision=10, scale=2)),
    pl.col("Volume").cast(pl.Decimal(precision=15, scale=2)),
    pl.col("Date").str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S")
])

# Guardar CSV
dataframe.write_csv("binancefutures_btcusdtp.csv")

# Configurar conexión a PostgreSQL
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

connection_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_url)

# Guardar en la base de datos
dataframe.write_database(
    table_name="market_data",
    connection=engine,
    if_table_exists="replace"  # usa 'append' si no quieres borrar lo anterior
)

print("✅ Datos guardados en PostgreSQL y archivo CSV.")
