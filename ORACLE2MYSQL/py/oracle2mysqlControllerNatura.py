from oracle2mysql import *

ip = "172.17.8.68"
port = "3306"
user = "cesaralmeciga5850"
password = "U@SXQFNiEeShz66wxgzt"
bbdd = "bbdd_cs_bog_natura"
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
            bbdd_cs_bog_natura.tb_pentaho_gestion_natura;"""

lastdate = repRead(mysql_query, mysql_conn)



mysql_queryDelete = """DELETE FROM `bbdd_cs_bog_natura`.`tb_pentaho_gestion_natura`
WHERE FECHA_GESTION = '""" + str(lastdate) + """';"""

executeQuery(mysql_queryDelete, mysql_conn)


oracle_query = """SELECT
            T.FECHA_GESTION,
            T.IDENTIFICACION,
            COD_CUENTA,
            T.CODIGO_PEDIDO,
            GESTION,
            CONSECUENCIA,
            CONTACTO,
            TELEFONO,
            MOTIVO_NO_PAGO,
            CORREO_ELECTRONICO,
            TEL_ADICIONAL,
            ESTADO,
            USUARIO,
            pr.FECHA_PAGO,
            pr.MONTO_PROMESA,
            FECHA_FIN_GESTION,
            DURACION,
            TEL_ADICIONAL_1,
            TEL_ADICIONAL_2,
            CORREO_ELECTRONICO_21,
            OBSERVACIONES
            FROM(
            SELECT GESTION.FECHA_GESTION,
            GESTION.IDENTIFICACION,
            gc.COD_CUENTA,
            gc.CODIGO_PEDIDO,
            GESTION,
            CONSECUENCIA,
            CONTACTO,
            TELEFONO,
            MOTIVO_NO_PAGO,
            CORREO_ELECTRONICO,
            TEL_ADICIONAL,
            'A' AS ESTADO,
            GESTION.USUARIO,
            GESTION.FECHA_FIN_GESTION,
            TRUNC((GESTION.FECHA_FIN_GESTION - GESTION.FECHA_GESTION) * (60 * 60 * 24))  AS  DURACION,
            NULL AS TEL_ADICIONAL_1,
            NULL AS TEL_ADICIONAL_2,
            NULL AS CORREO_ELECTRONICO_21,
            OBSERVACIONES
            FROM NATURA.GESTION
            INNER JOIN NATURA.GESTION_CUENTA gc 
            ON gc.FECHA_GESTION = GESTION.FECHA_GESTION
            AND gc.IDENTIFICACION = GESTION.IDENTIFICACION
            AND gc.USUARIO = GESTION.USUARIO
            WHERE TO_CHAR(GESTION.FECHA_GESTION,'YYYY-MM-DD') >= '""" + str(lastdate) + """') T
            LEFT JOIN 
            NATURA.PROMESAS pr ON pr.IDENTIFICACION  = T.IDENTIFICACION
            AND pr.CODIGO_PEDIDO = T.CODIGO_PEDIDO
            AND pr.NUMERO_CUENTA = T.COD_CUENTA
            AND pr.FECHA_GESTION = T.FECHA_GESTION"""

df_oracle = pentahoRead(oracle_query, oracle_conn)


to_sql(df_oracle, "tb_pentaho_gestion_natura", mysql_conn, 'append', None, 10000)

mysql_conn.close()