#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/24 9:29
# @Author  : liulijun
# @Site    : 
# @File    : DBsync.py
# @Software: PyCharm

import sqlite3
from GearboxDETemp import config
import pandas as pd
import pymysql

def data_sync():

    (conn,cur)=__sqlite_conn__()
    sqlstr= "SELECT farm_code,farm_name,wtgs_id,wtgs_bd,model_name,abnormal_group,abnormal_start_time,abnormal_end_time,abnormal_duration,creator,create_time,analysis_creator FROM early_warning"
    cur.execute(sqlstr)
    new_abnormal_data=cur.fetchall()
    conn.close()


    (conn, cur) = __mysql_conn__('127.0.0.1', 3306, config.local_db['_user'], config.local_db['_passwd'],'sub_healthy_model')
    sqlstr = "INSERT IGNORE INTO early_warning (farm_code,farm_name,wtgs_id,wtgs_bd,model_name,abnormal_group,abnormal_start_time,abnormal_end_time,abnormal_duration,creator,create_time,analysis_creator) VALUES "
    value = '('

    for j in range(len(new_abnormal_data)):
        item = new_abnormal_data[j]
        for i in range(len(item)):
            value += '\'' + str(item[i]) + '\''
            if i != len(item) - 1:
                value += ','
            elif j != len(new_abnormal_data) - 1:
                value += '),('
            else:
                value += ');'
    sqlstr += value
    try:
        cur.execute(sqlstr)
        conn.commit()
    except:
        print('insert error')
        pass
        conn.close()
        print('update finished!')

def __mysql_conn__(_host, _port, _user, _passwd, _db):

    # connect mysql db
    try:
        conn = pymysql.connect(
            host=_host,
            port=_port,
            user=_user,
            passwd=_passwd,
            db=_db,
            charset="utf8"
        )
        cur = conn.cursor()
        return conn, cur
    except:
        print("Could not connect to MySQL server.")

def __sqlite_conn__():

    conn = sqlite3.connect('./DB/early_warning.db')
    cur = conn.cursor()
    return conn, cur

if __name__=="__main__":
    data_sync()