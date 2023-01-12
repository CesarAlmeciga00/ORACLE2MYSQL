from importsAndFunctions import *


def repRead(query, conn):
    df_lastDate = queryRead(query, conn)
    lastDate = df_lastDate['fecha'].values[0]
    return lastDate

def pentahoRead(query, connection):
    df_oracle = queryRead(query, connection)
    return df_oracle

