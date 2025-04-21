import polars as pl

dataframe = pl.read_csv( 'binancefutures_btcusdtp.csv' )

# Convertir la columna 'Date' de string a datetime.
dataframe = dataframe.with_columns(
    pl.col( 'Date' ).str.strptime( pl.Datetime, format='%d/%m/%Y %H:%M:%S' ).alias( 'Date' )
)

################################################## regular_trading_hours

# Crear una serie de polars con las fechas de la columna 'Date'.
regular_trading_hours = dataframe.select(
    pl.col( 'Date' ).dt.strftime( '%d/%m/%Y' ).alias( 'Date' )
).unique( maintain_order=True )

# Convertir la serie 'Date' del nuevo dataframe de string a datetime.
regular_trading_hours = regular_trading_hours.with_columns(
    pl.col( 'Date' ).str.strptime( pl.Datetime, format='%d/%m/%Y' ).alias( 'Date' )
)

# Extraer la columna 'Date' como una lista de objetos datetime
dates_list = regular_trading_hours[ 'Date' ].to_list()

# Crear la columna para los resultados de los calculos para rth.
results = []

# Iterar sobre las fechas (datetime) en la lista
for date in dates_list:

    open = dataframe.filter(
        ( pl.col( 'Date' ) >= pl.datetime( date.year, date.month, date.day, 8, 30, 0 ) ) & ( pl.col( 'Date' ) <= pl.datetime( date.year, date.month, date.day, 15, 30, 0 ) )
    ).get_column( 'Open' )[ 0 ]

    high = dataframe.filter(
        ( pl.col( 'Date' ) >= pl.datetime( date.year, date.month, date.day, 8, 30, 0 ) ) & ( pl.col( 'Date' ) <= pl.datetime( date.year, date.month, date.day, 15, 30, 0 ) )
    ).get_column( 'High' ).max()

    low = dataframe.filter(
        ( pl.col( 'Date' ) >= pl.datetime( date.year, date.month, date.day, 8, 30, 0 ) ) & ( pl.col( 'Date' ) <= pl.datetime( date.year, date.month, date.day, 15, 30, 0 ) )
    ).get_column( 'Low' ).min()

    close = dataframe.filter(
        ( pl.col( 'Date' ) >= pl.datetime( date.year, date.month, date.day, 8, 30, 0 ) ) & ( pl.col( 'Date' ) <= pl.datetime( date.year, date.month, date.day, 15, 30, 0 ) )
    ).get_column( 'Close' )[ -1 ]

    volume = dataframe.filter(
        ( pl.col( 'Date' ) >= pl.datetime( date.year, date.month, date.day, 8, 30, 0 ) ) & ( pl.col( 'Date' ) <= pl.datetime( date.year, date.month, date.day, 15, 30, 0 ) )
    ).get_column( 'Volume' ).sum()

    results.append( [ open, high, low, close, volume ] )

    #print( f'{date.year}/{date.month}/{date.day} { open } { high } { low } { close } { volume }')

# Crear un dataframe con la respuesta de la API.
aux = pl.DataFrame( data=results, schema=[ 'Open', 'High', 'Low', 'Close', 'Volume' ], orient="row" )

regular_trading_hours = regular_trading_hours.with_columns( aux )

# Convertir la columna de fecha de datetime a string.
regular_trading_hours = regular_trading_hours.with_columns(
    pl.col( 'Date' ).dt.strftime( '%d/%m/%Y' )
)

# Filtrar por fecha
#filtered_dataframe = dataframe.filter(
    #( pl.col('Date') >= pl.datetime( 2024, 11, 7, 8, 30, 0 ) ) & ( pl.col('Date') <= pl.datetime( 2024, 11, 7, 15, 0, 0 ) )
#).select( 'Open' )[-1]

print( regular_trading_hours )
regular_trading_hours.write_csv( 'binancefutures_btcusdtp_rth.csv' )