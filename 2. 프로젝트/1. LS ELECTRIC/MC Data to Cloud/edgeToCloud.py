import sys
import sshtunnel
import pandas as pd
import numpy as np
import sqlalchemy
import pymysql
# import mysql.connector
from datetime import datetime, timedelta

# ======================================================================================================================
# ======================================================================================================================
### 산전 내부망과 외부망 분리상 Azure DSVM 을 통한 SSH Tunnuling 방법으로 접근
### Maria DB -> DSVM -> Azure MySQL

## Edge Maria DB 접속 정보
edge_host = '165.244.138.57'
edge_user = 'lsisadmin'
edge_password = 'Tbvjaosvkdnj00$'
edge_database = 'mc_edge'
edge_engine = sqlalchemy.create_engine('mysql+pymysql://' + edge_user + ':' + edge_password + '@' + edge_host
                                       + ':3306/' + edge_database + '?charset=utf8')

## Azure MySQL 접속 정보
azure_host = '127.0.0.1'  # ssh를 통해 접속하기 때문에 Local IP
azure_user = 'lsisadmin@mc-mysql'
azure_password = 'Tbvjaosvkdnj00$'
azure_database = 'mc'

## D-1 날짜 : 변수로 받음(Crontab 실행 시)
var1 = sys.argv[1]
day_var = float(var1)
yesterday = datetime.now() - timedelta(days=day_var)
check_dt = yesterday.strftime('%Y-%m-%d')

## Line ID(1 ~ 8)
line_list = list(range(1, 9))
# line_list = [1,2,3,4,5,7,8]

## Edge Server 판정결과 데이터 Select
edge_conn = edge_engine.connect()

result_tmp = []

for lid in line_list :
    try :
        query = """SELECT * FROM mc_edge.report WHERE lid = """ + str(lid) + """ AND dt = '""" + check_dt + """';"""
        df = pd.read_sql_query(query, edge_conn)
        result_tmp.append(df)
    except :
        continue

edge_result = pd.concat(result_tmp, ignore_index=True)

## Azure MySQL에 적재
with sshtunnel.SSHTunnelForwarder(
        ('52.141.56.126', 22),
        ssh_username='lseadmin',
        ssh_password='Tbvjaosvkdnj00$',
        remote_bind_address = ('mc-mysql.mysql.database.azure.com', 3306)
        ) as server:

    azure_port = str(server.local_bind_port)
    azure_engine = sqlalchemy.create_engine('mysql+pymysql://' + azure_user + ':' + azure_password + '@' + azure_host
                                            + ':' + azure_port +'/' + azure_database + '?charset=utf8')

    for attempt in range(5) :
        try :
            print('Attempt : ' + str(attempt + 1))

            azure_conn = azure_engine.connect()

            edge_result.to_sql(con=azure_engine, name='edge_report', index=False, if_exists='append')
        except :
            continue

        break
