from oracle2mysql import *

ip = "172.17.8.68"
port = "3306"
user = "cesaralmeciga5850"
password = "U@SXQFNiEeShz66wxgzt"
bbdd = "bbdd_cs_bog_movistar_mexico"
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
            bbdd_cs_bog_movistar_mexico.tb_tipificacion_pentaho_b2c;"""

lastdate = repRead(mysql_query, mysql_conn)



mysql_queryDelete = """DELETE FROM `bbdd_cs_bog_movistar_mexico`.`tb_tipificacion_pentaho_b2c`
WHERE FECHA_GESTION = '""" + str(lastdate) + """';"""

executeQuery(mysql_queryDelete, mysql_conn)


oracle_query = """SELECT 
                
                T.FECHA_GESTION,
                HORA_GESTION,
                T.IDENTIFICACION,
                GESTION,
                CONSECUENCIA,
                CONTACTO,
                OBSERVACIONES,
                USUARIO,
                TELEFONO,
                MOTIVO_NO_PAGO,
                SUBMOTIVO_NO_PAGO,
                T.NUMERO_CUENTA,
                pr.FECHA_PAGO,
                pr.MONTO_PROMESA,
                
                FECHA_FIN_GESTION,
                HORA_FIN_GESTION,
                FIDELIZACION,
                FIDELIZACION_BENEFICIO,
                DESCUENTO_FACTURA,
                MOTIVO_NO_PAGO2,
                MOTIVO_NO_PAGO3,
                OPERADOR,
                TIENE_SERVICIOS,
                CONTINUA_SERVICIO FROM(
                SELECT 
                GESTION.FECHA_GESTION AS FECHA_HORA_GESTION,
                TO_CHAR(GESTION.FECHA_GESTION,'YYYY-MM-DD') AS FECHA_GESTION,
                TO_CHAR(GESTION.FECHA_GESTION,'HH24:MI:SS') AS HORA_GESTION,
                GESTION.IDENTIFICACION,
                GESTION,
                CONSECUENCIA,
                CONTACTO,
                OBSERVACIONES,
                GESTION.USUARIO,
                TELEFONO,
                MOTIVO_NO_PAGO,
                SUBMOTIVO_NO_PAGO,
                gc.NUMERO_CUENTA,
                FECHA_FIN_GESTION AS FECHA_HORA_FIN_GESTION,
                TO_CHAR(FECHA_FIN_GESTION, 'YYYY-MM-DD') AS FECHA_FIN_GESTION,
                TO_CHAR(FECHA_FIN_GESTION, 'HH24:MI:SS') AS HORA_FIN_GESTION,
                FIDELIZACION,
                FIDELIZACION_BENEFICIO,
                DESCUENTO_FACTURA,
                MOTIVO_NO_PAGO2,
                MOTIVO_NO_PAGO3,
                OPERADOR,
                TIENE_SERVICIOS,
                CONTINUA_SERVICIO
                FROM MOVISTAR_COBRANZA_MEX_PDTI.GESTION
                INNER JOIN MOVISTAR_COBRANZA_MEX_PDTI.GESTION_CUENTA gc 
                ON gc.FECHA_GESTION = GESTION.FECHA_GESTION
                AND gc.IDENTIFICACION = GESTION.IDENTIFICACION
                AND gc.USUARIO = GESTION.USUARIO
                WHERE TO_CHAR(GESTION.FECHA_GESTION,'YYYY-MM-DD') >= '""" + str(lastdate) + """') T
                LEFT JOIN 
                MOVISTAR_COBRANZA_MEX_PDTI.PROMESA pr ON pr.IDENTIFICACION  = T.IDENTIFICACION
                AND pr.NUMERO_CUENTA = T.NUMERO_CUENTA
                AND pr.FECHA_GESTION = T.FECHA_HORA_GESTION"""

df_oracle = pentahoRead(oracle_query, oracle_conn)


to_sql(df_oracle, "tb_tipificacion_pentaho_b2c", mysql_conn, 'append', None, 10000)

mysql_conn.close()