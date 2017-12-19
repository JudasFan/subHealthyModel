#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/12 11:01
# @Author  : liulijun
# @Site    : 
# @File    : WindWheelLock.py
# @Software: PyCharm

import multiprocessing
from multiprocessing import Pool
import pandas as pd
from datetime import *
import datetime
import sqlite3
import pymysql
import matplotlib.pyplot as plt

MODEL_NAME = '风轮锁工作异常'
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
                if '叶轮锁定销1释放' in key:
                    self.lock_off_1 = self.tag_set[key]
                if '叶轮锁定销2释放' in key:
                    self.lock_off_2 = self.tag_set[key]
                if '叶轮转速1' in key:
                    self.wheel_speed_1 = self.tag_set[key]
                if '叶轮转速2' in key:
                    self.wheel_speed_2 = self.tag_set[key]
                if '运行模式' in key:
                    self.run_mode = self.tag_set[key]
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
                    wtgsbd = str(wtgs['WTGS_NAME'].iloc[0])

                    rowi = 0
                    while rowi < len(record):
                        if record[self.run_mode].iloc[rowi] in list(range(11, 16)) and (int(record[self.lock_off_1].iloc[rowi]) == 0 or int(record[self.lock_off_2].iloc[rowi]) == 0) and (record[self.wheel_speed_1].iloc[rowi] >= 0.7 and record[self.wheel_speed_2].iloc[rowi] >= 0.7):  # 机组运行模式在11、12、13、14、15的情况下，叶轮转速均大于0.7，如果叶轮锁销1释放和叶轮锁定销2释放信号其中一个为0
                            rowj = rowi + 1
                            if rowj >= len(record):
                                break
                            while rowj < len(record):
                                if record[self.run_mode].iloc[rowj] in list(range(11, 16)) and (int(record[self.lock_off_1].iloc[rowj]) == 0 or int(record[self.lock_off_2].iloc[rowj]) == 0) and (record[self.wheel_speed_1].iloc[rowj] >= 0.7 and record[self.wheel_speed_2].iloc[rowj] >= 0.7):  # 机组运行模式在11、12、13、14、15的情况下，叶轮转速均大于0.7，如果叶轮锁销1释放和叶轮锁定销2释放信号其中一个为0
                                    if rowj == len(record) - 1:  # 假设到统计周期末尾均满足条件（防止进入死循环）
                                        st = datetime.datetime.strptime(str(record['real_time'].iloc[rowi]), "%Y-%m-%d %H:%M:%S")
                                        et = datetime.datetime.strptime(str(record['real_time'].iloc[rowj]), "%Y-%m-%d %H:%M:%S")
                                        abduration = (et - st).seconds / 3600
                                        if (et - st).seconds >= 2 and rowj >= rowi + 2:
                                            self.abnormal_records.append([farmcode, farmname, wtgsid, wtgsbd, MODEL_NAME,st.strftime("%Y-%m-%d %H:%M:%S"), et.strftime("%Y-%m-%d %H:%M:%S"),abduration, self.author,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
                                        rowi = len(record)
                                        break
                                    else:
                                        rowj += 1
                                else:
                                    st = datetime.datetime.strptime(str(record['real_time'].iloc[rowi]),"%Y-%m-%d %H:%M:%S")
                                    et = datetime.datetime.strptime(str(record['real_time'].iloc[rowj]),"%Y-%m-%d %H:%M:%S")
                                    abduration = (et - st).seconds / 3600

                                    flag = 0  # 1分钟内机组切换到14模式，且叶轮锁释放没有全部切换成1，则机组异常
                                    k = rowj
                                    con_et = datetime.datetime.strptime(str(record['real_time'].iloc[k]),"%Y-%m-%d %H:%M:%S")
                                    while (con_et - et).seconds <= 60:  # 以延长1分钟期间内考虑
                                        if record[self.run_mode].iloc[k] in [14] and (int(record[self.lock_off_1].iloc[k]) == 1 and int(record[self.lock_off_2].iloc[k]) == 1):
                                            flag = 1
                                            break
                                        k += 1
                                        con_et = datetime.datetime.strptime(str(record['real_time'].iloc[k]), "%Y-%m-%d %H:%M:%S")

                                    # 若不存在上述条件，则添加异常记录
                                    if flag == 0:
                                        if (et - st).seconds >= 2 and rowj >= rowi + 2:
                                            self.abnormal_records.append([farmcode, farmname, wtgsid, wtgsbd, MODEL_NAME,st.strftime("%Y-%m-%d %H:%M:%S"), et.strftime("%Y-%m-%d %H:%M:%S"),abduration, self.author,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
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

            query_field = self.run_mode
            query_field += ',' + self.lock_off_1
            query_field += ',' + self.lock_off_2
            query_field += ',' + self.wheel_speed_1
            query_field += ',' + self.wheel_speed_2

            query_condition = self.run_mode + ' is not null '
            query_condition += 'AND ' + self.lock_off_1 + ' is not null '
            query_condition += 'AND ' + self.lock_off_2 + ' is not null '
            query_condition += 'AND ' + self.wheel_speed_1 + ' is not null '
            query_condition += 'AND ' + self.wheel_speed_2 + ' is not null '

            #扩展查询时间戳前后半分钟
            sqlstr = "SELECT wtid,real_time," + query_field + " FROM " + path['TABLE_NAME'] + " WHERE " + query_condition + " AND real_time BETWEEN \'" + \
                     str(starttimestamp+timedelta(minutes=-0.5))+"\' AND \'" + str(endtimestmp+timedelta(minutes=0.5))+"\' ORDER BY real_time "
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
        res.to_csv("D:/work/亚健康模型/1.5&2.0MW/9.13-9.19/" + abnormal_record['farm_name'].iloc[0] + '-' + str(abnormal_record['wtgs_id'].iloc[0]) + '-' + MODEL_NAME + str(round(float(abnormal_record['abnormal_duration'].iloc[0]), 2)) + ".csv")
        return res

    def __curve__(self,detail_data,abnormal_record):
        df = pd.DataFrame(list(detail_data), columns=['机组','时间','机组运行模式','有功功率','发电机转速1','叶轮转速1','叶轮转速2','发电机转速2','叶轮锁定销1锁定','叶轮锁定销2锁定','叶轮锁定销1释放','叶轮锁定销2释放'])

        split_line_x0 = [abnormal_record[6],abnormal_record[6]]
        split_line_y0 = [abnormal_record[7], abnormal_record[7]]

        ymax_value=max([max(df['叶轮锁定销1释放']), max(df['叶轮锁定销2释放']), max(df['叶轮锁定销1锁定']), max(df['叶轮锁定销2锁定']), max(df['叶轮转速1']),max(df['叶轮转速2'])])

        split_line_x1 = [0, ymax_value]
        split_line_y1 = [0, ymax_value]

        y_move = [-ymax_value/100, ymax_value/100, ymax_value/100*2]

        plt.rcParams['font.sans-serif']=['SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        fig, ax1 = plt.subplots(figsize=(15,8))  # 使用subplots()创建窗口
        ax2 = ax1.twinx()  # 创建第二个坐标轴
        ax2.plot(df['时间'], df['叶轮转速1'], color='g', linewidth=2)
        ax2.plot(df['时间'], df['叶轮转速2'], color='orange', linewidth=2)
        ax1.plot(df['时间'], df['机组运行模式'], color='k', linewidth=2)
        ax2.plot(df['时间'], df['叶轮锁定销1释放'], color='r', linewidth=2)
        ax2.plot(df['时间'], df['叶轮锁定销2释放']+y_move[0], color='b', linewidth=2)
        ax2.plot(df['时间'], df['叶轮锁定销1锁定']+y_move[1], color='y', linewidth=2)
        ax2.plot(df['时间'], df['叶轮锁定销2锁定']+y_move[2], color='m', linewidth=2)
        ax2.plot(split_line_x0, split_line_x1, '--r', linewidth=2)
        ax2.plot(split_line_y0, split_line_y1, '--r', linewidth=2)

        ax1.set_xlabel('时间', fontsize=12)
        ax1.set_ylabel('机组运行模式', fontsize=12)
        ax2.set_ylabel('其它指标', fontsize=12)

        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')
        ax1.grid()
        plt.title(abnormal_record[1]+'-'+abnormal_record[3]+'-'+abnormal_record[4]+'-'+abnormal_record[5]+'-'+abnormal_record[6].strftime('%Y-%m-%d %H:%M:%S')+'-'+abnormal_record[7].strftime('%Y-%m-%d %H:%M:%S'), fontsize=14)
        plt.savefig(self.file_path+abnormal_record[1]+'-'+abnormal_record[3]+'-'+abnormal_record[4]+'-'+abnormal_record[5]+'-'+abnormal_record[6].strftime('%Y-%m-%d %H-%M-%S')+'-'+abnormal_record[7].strftime('%Y-%m-%d %H-%M-%S')+'.png')
        # plt.show()

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
        self.farm_name=['广西恭城3.0','克旗121','克旗135','四子王旗3.0']
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
