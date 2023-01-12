from oracle2mysql import *

ip = "172.17.8.68"
port = "3306"
user = "cesaralmeciga5850"
password = "U@SXQFNiEeShz66wxgzt"
bbdd = "bbdd_cs_bog_movistar_ciclos"
sqlEngine = sql_connection(ip, port, user, password, bbdd)
mysql_conn = sqlEngine.connect()



ip = '192.168.1.12'
port = 1521
SID = 'csdb'
user = 'data_cesardiaz'
password = 'NuGO1vhRmeID'

oracle_conn = oracle_connection(ip,port,user,password,SID)

df_oracle = pd.DataFrame()

mysql_query = """SELECT 
            MAX(FECHA_GESTION) as fecha
        FROM
            bbdd_cs_bog_movistar_ciclos.tb_pentaho_gestion_movistar_ciclos;"""

lastdate = repRead(mysql_query, mysql_conn)



mysql_queryDelete = """DELETE FROM `bbdd_cs_bog_movistar_ciclos`.`tb_pentaho_gestion_movistar_ciclos`
WHERE FECHA_GESTION = '""" + str(lastdate) + """';"""

executeQuery(mysql_queryDelete, mysql_conn)


oracle_query = """SELECT 
    FECHA_GESTION,
    HORA_GESTION,
    IDENTIFICACION,
    GESTION,
    CONSECUENCIA,
    CONTACTO,
    OBSERVACIONES,
    USUARIO,
    TELEFONO,
    MOTIVO_NO_PAGO,
    SUBMOTIVO_NO_PAGO,
    COD_CUENTA,
    FECHA_PAGO,
    MONTO_PROMESA,
    FECHA_FIN_GESTION,
    HORA_FIN_GESTION,
    DURACION,
    FIDELIZACION,
    FIDELIZACION_BENEFICIO
     FROM(
    SELECT 
    T.FECHA_HORA_GESTION,
    T.FECHA_GESTION,
    T.HORA_GESTION,
    T.IDENTIFICACION,
    T.GESTION,
    T.CONSECUENCIA,
    T.CONTACTO,
    T.OBSERVACIONES,
    T.USUARIO,
    T.TELEFONO,
    T.MOTIVO_NO_PAGO,
    T.SUBMOTIVO_NO_PAGO,
    T.COD_CUENTA,
    p.FECHA_PAGO,
    p.MONTO_PROMESA,
    T.FECHA_HORA_FIN_GESTION,
    T.FECHA_FIN_GESTION,
    T.HORA_FIN_GESTION,
    TRUNC(MOD((T.FECHA_HORA_FIN_GESTION - T.FECHA_HORA_GESTION)*(60*24),60)) || ' MIN ' ||
    TRUNC((T.FECHA_HORA_FIN_GESTION - T.FECHA_HORA_GESTION) * (60 * 60 * 24)) || ' SEG'  AS  DURACION,
    T.FIDELIZACION,
    T.FIDELIZACION_BENEFICIO FROM(
    SELECT 
    GESTION.FECHA_GESTION AS FECHA_HORA_GESTION,
    TO_CHAR(GESTION.FECHA_GESTION,'YYYY-MM-DD') AS FECHA_GESTION ,
    TO_CHAR(GESTION.FECHA_GESTION,'HH24:MI:SS') AS HORA_GESTION,
    GESTION.IDENTIFICACION,
    GESTION,
    CONSECUENCIA,
    CONTACTO,
    OBSERVACIONES,
    GESTION.USUARIO,
    TELEFONO,
    MOTIVO_NO_PAGO,
    NULL AS SUBMOTIVO_NO_PAGO,
    gc.COD_CUENTA,
    FECHA_FIN_GESTION AS FECHA_HORA_FIN_GESTION,
    TO_CHAR(FECHA_FIN_GESTION, 'YYYY-MM-DD') AS FECHA_FIN_GESTION,
    TO_CHAR(FECHA_FIN_GESTION, 'HH24:MI:SS') AS HORA_FIN_GESTION,
    FIDELIZACION,
    FIDELIZACION_BENEFICIO
    FROM MOVISTAR_INBOUND.GESTION
    INNER JOIN MOVISTAR_INBOUND.GESTION_CUENTA gc ON 
    gc.FECHA_GESTION = GESTION.FECHA_GESTION AND gc.IDENTIFICACION = GESTION.IDENTIFICACION 
    AND gc.USUARIO = GESTION.USUARIO) T
    LEFT JOIN MOVISTAR_INBOUND.PROMESAS p ON
    p.NUMERO_CUENTA = T.COD_CUENTA AND p.IDENTIFICACION = T.IDENTIFICACION AND p.FECHA_GESTION = T.FECHA_HORA_GESTION) A
    WHERE A.FECHA_GESTION >= '""" + str(lastdate) + """'"""

df_oracle = pentahoRead(oracle_query, oracle_conn)


to_sql(df_oracle, "tb_pentaho_gestion_movistar_ciclos", mysql_conn, 'append', None, 10000)

mysql_conn.close()