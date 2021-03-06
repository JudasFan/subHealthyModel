#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/12 11:01
# @Author  : liulijun
# @Site    : 
# @File    : TwistAngleLarge.py
# @Software: PyCharm

import multiprocessing
from multiprocessing import Pool
import pandas as pd
from datetime import *
import datetime
import sqlite3
import pymysql

MODEL_NAME = '扭缆角度过大未停机报警'
ANALYSOR='刘利军'
REMOTE_DB = {'user':'llj','passwd':'llj@2016'}
LOCAL_DB = {'user': 'root', 'passwd': '911220'}

class generate:

    def __init__(self, start_time, end_time, author):
        self.start_time=start_time
        self.end_time=end_time
        self.author=author
        self.cal_farm=farm_path()
        self.run()

    def run(self):
        for farm in self.cal_farm.farm_name:
            print(farm, 'begin...')
            self.warning(self.cal_farm.wtgs_path[farm], self.start_time, self.end_time, self.author)

    class warning:

        def __init__(self,db_path,start_time,end_time,author):
            self.abnormal_records = []
            self.full_data = []
            self.__key_tags__(db_path['FARM_NAME'].iloc[0])
            self.db_path = db_path
            self.start_time = start_time
            self.end_time = end_time
            self.author = author
            for key in self.tag_set.keys():
                if '扭' in key and '缆' in key:
                    self.twist_angle = self.tag_set[key]
                if '运行状态' in key:
                    self.run_condition_tag = self.tag_set[key]
            self.run()  # run the algorithm and get the warnning info
            self.export()  # export warning info to .DB (local or remote)

        def run(self):
            # generate two instances of process related
            manager = multiprocessing.Manager()
            q = manager.Queue()
            p = Pool(processes=2)
            # add function to process pool and run
            for index, row in self.db_path.iterrows():
                wtgs = self.db_path.ix[index, :]
                result = p.apply_async(self.query_real_data, args=(wtgs, q))

            p.close()
            p.join()
            if not result.successful():
                print("unfortunately, failed to add process to pool...")

            # transfer datatype
            wtgsValue=[]
            while not q.empty():
                wtgsValue.append(q.get())

            if len(wtgsValue)>0:
                for record in wtgsValue:#逐机组循环
                    wtgs = self.db_path[self.db_path['WTGS_ID']==int(record['wtid'].iloc[0])]
                    farmcode = str(int(wtgs['FARM_CODE']))
                    farmname = str(wtgs['FARM_NAME'].iloc[0])
                    wtgsid = str(int(wtgs['WTGS_ID']))
                    wtgsbd=str(wtgs['WTGS_NAME'].iloc[0])
                    rowi=0
                    while rowi < len(record):
                        if record[self.twist_angle].iloc[rowi] > 756 and record[self.run_condition_tag].iloc[rowi] == 3:  # 扭缆角度大于2.1圈且处于并网状态
                            rowj = rowi + 1
                            if rowj >= len(record):
                                break
                            while rowj < len(record):
                                if record[self.twist_angle].iloc[rowj] > 756 and record[self.run_condition_tag].iloc[rowj] == 3:  # 扭缆角度大于2.1圈且处于并网状态
                                    if rowj == len(record) - 1:  # 假设到统计周期末尾均满足条件（防止进入死循环）
                                        st = datetime.datetime.strptime(str(record['real_time'].iloc[rowi]), "%Y-%m-%d %H:%M:%S")
                                        et = datetime.datetime.strptime(str(record['real_time'].iloc[rowj]), "%Y-%m-%d %H:%M:%S")
                                        abduration = (et - st).seconds / 3600
                                        if (et - st).seconds >= 1800 and rowj >= rowi + 900 and record[self.run_condition_tag].iloc[rowj] == 3:  # 判断是否持续时间超过30分钟，30分钟后没有停机，并且不是相邻的两条记录:
                                            self.abnormal_records.append([farmcode, farmname, wtgsid, wtgsbd, MODEL_NAME, st.strftime('%Y-%m-%d %H:%M:%S'), et.strftime('%Y-%m-%d %H:%M:%S'),abduration, self.author,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
                                        rowi = len(record)
                                        break
                                    else:
                                        rowj += 1
                                else:
                                    st = datetime.datetime.strptime(str(record['real_time'].iloc[rowi]),"%Y-%m-%d %H:%M:%S")
                                    et = datetime.datetime.strptime(str(record['real_time'].iloc[rowj]),"%Y-%m-%d %H:%M:%S")
                                    abduration = (et - st).seconds / 3600
                                    if (et - st).seconds >= 1801 and rowj >= rowi + 900 and record[self.run_condition_tag].iloc[rowj] == 3:
                                        self.abnormal_records.append([farmcode, farmname, wtgsid, wtgsbd, MODEL_NAME, st.strftime('%Y-%m-%d %H:%M:%S'), et.strftime('%Y-%m-%d %H:%M:%S'), abduration,self.author,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
                                    rowi = rowj + 1
                                    break
                        else:
                            rowi += 1
                if len(self.abnormal_records)>0:
                    print(self.abnormal_records)
                print(self.db_path['FARM_NAME'].iloc[0], 'calculate finished!')
            else:
                print(self.db_path['FARM_NAME'].iloc[0], 'record empty!')

        def export(self):

            # export abnormal record to local database
            # input argv:data
            # datatype: dictionary
            if len(self.abnormal_records)>0:
                import socket
                hostname = socket.gethostname()
                if hostname != 'DESKTOP-6RO9O74':
                    # if run in my mobile pc, save data on mysql db, other than sava data on sqlite
                    (conn, cur) = mysql_conn('127.0.0.1', 3306, LOCAL_DB['user'], LOCAL_DB['passwd'],'sub_healthy_model')
                    sqlstr = "INSERT IGNORE INTO early_warning (farm_code,farm_name,wtgs_id,wtgs_bd,model_name,abnormal_start_time,abnormal_end_time,abnormal_duration,creator,create_time) VALUES "
                    value = '('
                else:
                    (conn, cur) =sqlite_conn()
                    sqlstr = "INSERT INTO early_warning (farm_code,farm_name,wtgs_id,wtgs_bd,model_name,abnormal_start_time,abnormal_end_time,abnormal_duration,creator,create_time) VALUES "
                    value = '('
                for j in range(len(self.abnormal_records)):
                    item = self.abnormal_records[j]
                    for i in range(len(item)):
                        value += '\'' + str(item[i]) + '\''
                        if i != len(item) - 1:
                            value += ','
                        elif j != len(self.abnormal_records) - 1:
                            value += '),('
                        else:
                            value += ');'
                sqlstr += value
                print(sqlstr)
                try:
                    cur.execute(sqlstr)
                    conn.commit()
                except:
                    print('insert error')
                    pass
                conn.close()
                print(self.db_path['FARM_NAME'].iloc[0], 'export finished!')
            else:
                print(self.db_path['FARM_NAME'].iloc[0], 'each wtgs is running well!')

        def query_real_data(self, path, q):

            (conn, cur) = mysql_conn(path['HOST'], path['PORT'], REMOTE_DB['user'], REMOTE_DB['passwd'], path['DB'])
            starttimestamp = datetime.datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")
            endtimestmp = datetime.datetime.strptime(self.end_time, "%Y-%m-%d %H:%M:%S")

            query_field = self.twist_angle
            query_field += ',' + self.run_condition_tag

            query_condition = self.twist_angle + ' is not null '
            query_condition += 'AND ' + self.run_condition_tag + ' is not null '

            # 扩展查询时间戳前后半分钟
            sqlstr = "SELECT wtid,real_time," + query_field + " FROM " + path['TABLE_NAME'] + " WHERE " + query_condition + " AND real_time BETWEEN \'" + \
                     str(starttimestamp + timedelta(minutes=-0.5)) + "\' AND \'" + str(endtimestmp + timedelta(minutes=0.5)) + "\' ORDER BY real_time "
            # print(sqlstr)
            cur.execute(sqlstr)
            res = pd.read_sql(sqlstr,con=conn)
            conn.close()
            print(path['WTGS_ID'], 'query finished!', len(res))
            if len(res)>0:
                q.put(res)

        def __key_tags__(self,farm):
            self.tag_set = {}
            dframe = pd.read_excel("./config/tag/" + farm + ".xlsx", sheetname ="sheet1")
            for i in range(len(dframe[MODEL_NAME])):
                if str(dframe[MODEL_NAME].iloc[i]) == '1.0':
                    self.tag_set[dframe.index[i]]=dframe['tag_EN'][i]

class query:

    def __init__(self, start_time, end_time, farm=''):
        self.start_time = start_time
        self.end_time = end_time
        self.farm = []
        if farm:
            self.farm = farm

    def abnormal_records(self):
        (conn, cur) = sqlite_conn()
        if len(self.farm) > 0:
            sqlstr = 'SELECT farm_code,farm_name,wtgs_id,wtgs_bd,model_name,abnormal_start_time,abnormal_end_time,abnormal_duration,creator ' \
                     'FROM early_warning WHERE farm_name=\'' + self.farm + '\' AND abnormal_start_time BETWEEN \'' + self.start_time + '\' AND \'' + self.end_time + '\' AND model_name=\'' + MODEL_NAME + '\''
        else:
            sqlstr = 'SELECT farm_code,farm_name,wtgs_id,wtgs_bd,model_name,abnormal_start_time,abnormal_end_time,abnormal_duration,creator ' \
                     'FROM early_warning WHERE abnormal_start_time BETWEEN \'' + self.start_time + '\' AND \'' + self.end_time + '\' AND model_name=\'' + MODEL_NAME + '\''
        res = pd.read_sql(sqlstr, con=conn)
        conn.close()
        return res

    def detail(self, abnormal_record):
        assert len(abnormal_record) == 1
        self.cal_farm = farm_path()
        self.key_tags(abnormal_record['farm_name'].iloc[0])
        st = datetime.datetime.strptime(abnormal_record['abnormal_start_time'].iloc[0],"%Y-%m-%d %H:%M:%S") - timedelta(minutes=5)  # plus 5 minutes before abnormal duration
        et = datetime.datetime.strptime(abnormal_record['abnormal_end_time'].iloc[0],"%Y-%m-%d %H:%M:%S") + timedelta(minutes=5)  # plus 5 minutes after abnormal duration
        wtgs_path = self.cal_farm.wtgs_path[abnormal_record['farm_name'].iloc[0]]
        target_path = wtgs_path[wtgs_path['WTGS_ID'] == int(abnormal_record['wtgs_id'].iloc[0])]
        (conn, cur) = mysql_conn(target_path['HOST'].iloc[0], int(target_path['PORT'].iloc[0]), REMOTE_DB['user'],REMOTE_DB['passwd'], target_path['DB'].iloc[0])
        sqlstr = 'SELECT ' + ','.join(self.tag_set)
        sqlstr += ' FROM ' + target_path['TABLE_NAME'].iloc[0] + ' WHERE real_time BETWEEN \'' + st.strftime('%Y-%m-%d %H:%M:%S') + '\' AND \'' + et.strftime('%Y-%m-%d %H:%M:%S') + '\''
        res = pd.read_sql(sqlstr, con=conn)
        res.to_csv("D:/work/亚健康模型/1.5&2.0MW/10.11-10.17/" + abnormal_record['farm_name'].iloc[0] + '-' + str(abnormal_record['wtgs_id'].iloc[0]) + '-' + MODEL_NAME + str(round(float(abnormal_record['abnormal_duration'].iloc[0]), 2)) + ".csv")
        return res

    def key_tags(self, farm):

        self.tag_set = []
        dframe = pd.read_excel("./config/tag/" + farm + ".xlsx", sheetname="sheet1")
        for i in range(len(dframe[MODEL_NAME])):
            if str(dframe[MODEL_NAME].iloc[i]) == '1.0':
                self.tag_set.append(dframe['tag_EN'][i])

class farm_path:
    # function: read local config file,.xlsx, and return the db path of each wtgs
    def __init__(self):
        self.farm_list()
        self.wtgs_list()
    def farm_list(self):
        farm_info = pd.read_excel("./config/path/" + "FARM_LIST.xlsx",sheetname ="Sheet1")
        self.farm_name=farm_info[farm_info['is_cal'] == 1]['farm_name'].tolist()
    def wtgs_list(self):
        self.wtgs_path={}
        for farm_name in self.farm_name:
            wtgs_path = pd.read_excel("./config/path/" + farm_name + ".xlsx",sheetname ="Sheet1")

            wtgs_path.index=wtgs_path['WTGS_ID'].tolist()
            self.wtgs_path[farm_name]=wtgs_path

def mysql_conn(_host,_port,_user,_passwd,_db):
    # 建立链接
    try:
        conn = pymysql.connect(
            host=_host,
            port=int(_port),
            user=_user,
            passwd=_passwd,
            db=_db,
            charset="utf8"
        )
        cur = conn.cursor()
        return conn, cur
    except:
        print("Could not connect to MySQL server.")

def sqlite_conn():

    conn = sqlite3.connect('./DB/early_warning.db')
    cur = conn.cursor()
    return conn, cur

if __name__=="__main__":
    aaa = generate('2017-11-15 00:00:00', '2017-11-21 00:00:00', '刘利军')

    # aaa = query('2017-10-11 00:00:00', '2017-10-18 00:00:00')
    # res = aaa.abnormal_records()
    # for i in range(len(res)):
    #     print(res.ix[i:i])
    #     aaa.detail(res.ix[i:i])
