#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 11:40
# @Author  : liulijun
# @Site    : 
# @File    : GenNDETemp.py
# @Software: PyCharm

import multiprocessing
from multiprocessing import Pool
import pandas as pd
from datetime import *
import datetime
import pymysql
import matplotlib.pyplot as plt
import sqlite3

MODEL_NAME = '发电机非驱动端轴承温度异常'
ANALYSOR='刘利军'
EVA_METHOD = ['箱形图', '均值']
MINUTES = 1
QUARTILE = 3.0
MEAN = 10
REMOTE_DB = {'user':'llj','passwd':'llj@2016'}
LOCAL_DB = {'user': 'root', 'passwd': '911220'}
POWER_SPLITER = [100, 300, 600, 900, 1200, 1500, 1800, 2200]

def quartile(data):
    # calculate max value of quartile
    assert  len(data)>=3
    data=sorted(data)
    #Q1
    pvalue=(len(data) + 1) * 0.25
    intvalue=int(pvalue)
    floatvalue = pvalue - intvalue
    if floatvalue==0:
        q1 = data[intvalue-1]
    else:
        q1 = data[intvalue - 1] * floatvalue + data[intvalue] * (1 - floatvalue)

    # Q2
    pvalue = (len(data) + 1) * 0.5
    intvalue = int(pvalue)
    floatvalue = pvalue - intvalue
    if floatvalue == 0:
        q2 = data[intvalue - 1]
    else:
        q2 = data[intvalue - 1] * floatvalue + data[intvalue] * (1 - floatvalue)

    #Q3
    pvalue = (len(data) + 1) * 0.75
    intvalue = int(pvalue)
    floatvalue = pvalue - intvalue
    if floatvalue==0:
        q3 = data[intvalue - 1]
    else:
        q3 = data[intvalue - 1] * floatvalue + data[intvalue] * (1 - floatvalue)
    max_value = q3 + (q3 - q1) * QUARTILE  # upper
    return max_value,q2

def mean_value(data):
    # return a mean value of a list
    assert len(data) >= 3
    mean_value=(sum(data)-min(data)-max(data))/(len(data)-2) # remove the max and min value of a list
    max_value = mean_value + MEAN  # upper
    return max_value

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
        # return the warning record of a wtgs based run data of a period
        def __init__(self, wtgs_path, start_time, end_time, author):
            self.abnormal_records = []
            self.wtgs_path = wtgs_path
            self.start_time = start_time
            self.end_time = end_time
            self.author = author
            self.key_tags(wtgs_path['FARM_NAME'].iloc[0])
            for key in self.tag_set.keys():  # select the english name of tag based on matching
                if '发电机' in key and '轴承温度' in key and ('NDE端' in key or '非驱动端' in key):
                    self.temp_tag = self.tag_set[key]
                if '运行状态' in key:
                    self.run_condition_tag = self.tag_set[key]
                if '功率' in key:
                    self.power = self.tag_set[key]
            self.run()  # run the algorithm and get the warnning info
            self.export()  # export warning info to .DB (local or remote)

        def run(self):
            # step1:query real data
            manager = multiprocessing.Manager()
            q1 = manager.Queue()  # hold the value of temperature
            q2 = manager.Queue()  # hold the value of power
            p = Pool(processes=2)  # process pool
            # multiprocess: query temperature value based on avg(mins)
            for index, row in self.wtgs_path.iterrows():
                wtgs = self.wtgs_path.ix[index, :]
                result = p.apply_async(self.query_temp_mins_value, args=(wtgs, q1, q2))
            p.close()
            p.join()
            if not result.successful():
                print("unfortunately, failed to add process to pool...")
            # merge
            self.wtgsTempValue = pd.DataFrame()
            self.wtgsPowerValue = pd.DataFrame()
            while not q1.empty():
                if self.wtgsTempValue.empty:
                    self.wtgsTempValue = q1.get()
                else:
                    self.wtgsTempValue = self.wtgsTempValue.join(q1.get())
            while not q2.empty():
                if self.wtgsPowerValue.empty:
                    self.wtgsPowerValue = q2.get()
                else:
                    self.wtgsPowerValue = self.wtgsPowerValue.join(q2.get())
            print(self.wtgs_path['FARM_NAME'].iloc[0],'query finished')
            # generate the abnormal record using boxplot or mean method
            AbnormalRecord = []
            wtgslist = list(self.wtgsPowerValue.columns)
            for time, row in self.wtgsPowerValue.iterrows():
                # part1: divide the wtgs into different group based on its power
                wtgs_as_power = {str(i): [] for i in range(len(POWER_SPLITER) + 1)}
                power_data = self.wtgsPowerValue.ix[time]
                temperature_data = self.wtgsTempValue.ix[time]
                for wtgs in wtgslist:
                    value = power_data[wtgs]
                    if value < POWER_SPLITER[0]:
                        wtgs_as_power['0'].append(wtgs)
                    elif value >= POWER_SPLITER[0] and value <= POWER_SPLITER[1]:
                        wtgs_as_power['1'].append(wtgs)
                    elif value > POWER_SPLITER[1] and value <= POWER_SPLITER[2]:
                        wtgs_as_power['2'].append(wtgs)
                    elif value > POWER_SPLITER[2] and value <= POWER_SPLITER[3]:
                        wtgs_as_power['3'].append(wtgs)
                    elif value > POWER_SPLITER[3] and value <= POWER_SPLITER[4]:
                        wtgs_as_power['4'].append(wtgs)
                    elif value > POWER_SPLITER[4] and value <= POWER_SPLITER[5]:
                        wtgs_as_power['5'].append(wtgs)
                    elif value > POWER_SPLITER[5] and value <= POWER_SPLITER[6]:
                        wtgs_as_power['6'].append(wtgs)
                    elif value > POWER_SPLITER[6] and value <= POWER_SPLITER[7]:
                        wtgs_as_power['7'].append(wtgs)
                    elif value > POWER_SPLITER[7]:
                        wtgs_as_power['8'].append(wtgs)
                    else:
                        pass
                # part2: analyze the temp of wtgs and to find abnormal wtgs in the same group
                for key, wtgsgroup in wtgs_as_power.items():
                    if len(wtgsgroup) >= 4:
                        temp_value_group = temperature_data.ix[[int(wtgs) for wtgs in wtgsgroup]]
                        avg_power = power_data.ix[[int(wtgs) for wtgs in wtgsgroup]]
                        avg_power = sum(avg_power) / len(avg_power)
                        [max_value, avg_temp] = quartile(temp_value_group.tolist()) #四分位方法
                        for abwtgs in temp_value_group.index:
                            if temp_value_group.ix[abwtgs] >= max_value and max_value != 0:
                                AbnormalRecord.append([self.wtgs_path['FARM_CODE'].iloc[0], self.wtgs_path['FARM_NAME'].iloc[0], abwtgs, MODEL_NAME, time, temp_value_group.ix[abwtgs], avg_temp, avg_power])
                            else:
                                continue
            # part3:filter the orignal abnormal record with 10 mins, if a wtgs is under abnormal status more than 10 mins, which selected
            AbnormalRecord = pd.DataFrame(AbnormalRecord,columns=['farm_code', 'farm_name', 'wtgs_id', 'model_name', 'abnormal_time','generator_NDE_temperature', 'avg_temperature', 'avg_power'])
            SelectedAbnormalRecord = pd.DataFrame()
            abnormal_wtgs_list = sorted(list(set(AbnormalRecord['wtgs_id'].tolist())))
            for abnormal_wtgs in abnormal_wtgs_list: # loop all possible wtgs
                abnormal_wtgs_record = AbnormalRecord[AbnormalRecord['wtgs_id'] == abnormal_wtgs]
                if len(abnormal_wtgs_record) >= 10 / MINUTES:
                    rowi = 0
                    while rowi < len(abnormal_wtgs_record):
                        rowj = rowi + 1
                        if rowj == len(abnormal_wtgs_record):
                            break
                        while rowj < len(abnormal_wtgs_record):
                            if (datetime.datetime.strptime(abnormal_wtgs_record['abnormal_time'].iloc[rowj],"%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(abnormal_wtgs_record['abnormal_time'].iloc[rowj - 1],"%Y-%m-%d %H:%M:%S")).seconds == 60:
                                rowj += 1
                                if rowj == len(abnormal_wtgs_record):
                                    rowi = len(abnormal_wtgs_record)
                                    break
                                else:
                                    continue
                            elif (datetime.datetime.strptime(abnormal_wtgs_record['abnormal_time'].iloc[rowj - 1],"%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(abnormal_wtgs_record['abnormal_time'].iloc[rowi],"%Y-%m-%d %H:%M:%S")).seconds >= 600:
                                if SelectedAbnormalRecord.empty:
                                    SelectedAbnormalRecord = abnormal_wtgs_record.iloc[rowi:rowj]
                                    # export data structure
                                    ex_farm_code = str(abnormal_wtgs_record['farm_code'].iloc[rowi])
                                    ex_farm_name = str(abnormal_wtgs_record['farm_name'].iloc[rowi])
                                    ex_wtgs_id = str(abnormal_wtgs_record['wtgs_id'].iloc[rowi])
                                    ex_wtgs_bd = str(self.wtgs_path[self.wtgs_path['WTGS_ID'] == abnormal_wtgs_record['wtgs_id'].iloc[rowi]]['WTGS_NAME'].iloc[0])
                                    ex_start_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowi])
                                    ex_end_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowj - 1])
                                    ex_duration = str((datetime.datetime.strptime(ex_end_time,"%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(ex_start_time, "%Y-%m-%d %H:%M:%S")).seconds/3600)
                                    self.abnormal_records.append([ex_farm_code, ex_farm_name, ex_wtgs_id, ex_wtgs_bd, MODEL_NAME, '',ex_start_time, ex_end_time, ex_duration, ANALYSOR, EVA_METHOD[0],datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '', '', '', '', '', '','', '', '', '', '', '', ''])
                                    rowi = rowj
                                    break
                                else:
                                    SelectedAbnormalRecord = pd.concat([SelectedAbnormalRecord,abnormal_wtgs_record.iloc[rowi:rowj]])
                                    # export data structure
                                    ex_farm_code = str(abnormal_wtgs_record['farm_code'].iloc[rowi])
                                    ex_farm_name = str(abnormal_wtgs_record['farm_name'].iloc[rowi])
                                    ex_wtgs_id = str(abnormal_wtgs_record['wtgs_id'].iloc[rowi])
                                    ex_wtgs_bd = str(self.wtgs_path[self.wtgs_path['WTGS_ID'] ==abnormal_wtgs_record['wtgs_id'].iloc[rowi]]['WTGS_NAME'].iloc[0])
                                    ex_start_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowi])
                                    ex_end_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowj - 1])
                                    ex_duration = str((datetime.datetime.strptime(ex_end_time,"%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(ex_start_time, "%Y-%m-%d %H:%M:%S")).seconds / 3600)
                                    self.abnormal_records.append([ex_farm_code, ex_farm_name, ex_wtgs_id, ex_wtgs_bd, MODEL_NAME, '',ex_start_time, ex_end_time, ex_duration, ANALYSOR, EVA_METHOD[0],datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '', '', '', '', '', '','', '', '', '', '', '', ''])
                                    rowi = rowj
                                    break
                            else:
                                rowi = rowj
                                break
            if len(self.abnormal_records) > 0:
                self.abnormal_records = pd.DataFrame(self.abnormal_records,columns=['farm_code','farm_name','wtgs_id','wtgs_bd','model_name','abnormal_group','abnormal_start_time','abnormal_end_time','abnormal_duration','creator','analysis_creator','create_time','release_time','result_engtec_dep','description_engtec_dep','checktime_engtec_dep','serial_number','serial_creator','serial_create_time','abnormal_level','serial_status','check_time_farm','result_farm','description_farm','delete'])
                self.abnormal_detail = SelectedAbnormalRecord
            print(self.wtgs_path['FARM_NAME'].iloc[0], 'calculate finished!')

        def export(self):

            if len(self.abnormal_records) > 0:
                (conn, cur) = sqlite_conn()
                pd.io.sql.to_sql(self.abnormal_records, 'early_warning', con=conn, if_exists='append')
                pd.io.sql.to_sql(self.abnormal_detail, 'GenNDETemp', con=conn, if_exists='append')
                conn.close()
                try:
                    print(self.wtgs_path['FARM_NAME'].iloc[0], 'export finished!')
                except:
                    print('may be data repeat')
            else:
                print(self.wtgs_path['FARM_NAME'].iloc[0], 'each wtgs is running well!')

        def query_temp_mins_value(self, path, q1, q2):

            (conn, cur) = mysql_conn(path['HOST'], path['PORT'], REMOTE_DB['user'], REMOTE_DB['passwd'], path['DB'])
            starttimestamp = datetime.datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")
            endtimestmp = datetime.datetime.strptime(self.end_time, "%Y-%m-%d %H:%M:%S")
            # expand the query time(MINUTES/2 minutes before start time and MINUTES/2 minutes after end time
            sqlstr = "SELECT wtid,real_time," + \
                     self.temp_tag + ',' + \
                     self.power + \
                     " FROM " + path['TABLE_NAME'] + \
                     " WHERE " \
                     + self.temp_tag + " is not Null and " + \
                     self.power + " is not Null and " + \
                     self.run_condition_tag + "=\'3\' AND real_time BETWEEN \'" + \
                     str(starttimestamp - timedelta(minutes=MINUTES / 2)) + "\' AND \'" \
                     + str(endtimestmp + timedelta(minutes=MINUTES / 2)) + "\' ORDER BY real_time "
            res = pd.read_sql(sqlstr, con=conn)
            conn.close()
            # generate a list of timestamp
            timestampstr = []
            timestamp = starttimestamp
            while timestamp <= endtimestmp:
                timestampstr.append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                timestamp += timedelta(minutes=MINUTES)
            # get avg value of two tag
            tempvalue = []
            powervalue = []
            for id in range(len(timestampstr)):
                fromtime = (datetime.datetime.strptime(timestampstr[id], "%Y-%m-%d %H:%M:%S") - timedelta(
                    minutes=MINUTES / 2)).strftime("%Y-%m-%d %H:%M:%S")
                totime = (datetime.datetime.strptime(timestampstr[id], "%Y-%m-%d %H:%M:%S") + timedelta(
                    minutes=MINUTES / 2)).strftime("%Y-%m-%d %H:%M:%S")
                if len(res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)]):
                    if len(res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.temp_tag]) > 0:
                        data = res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.temp_tag]
                        data = data.dropna(axis=0, how='any').tolist()
                        tempvalue.append(sum(data) / len(data))
                    else:
                        tempvalue.append(0)
                    if len(res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.power]) > 0:
                        data = res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.power]
                        data = data.dropna(axis=0, how='any').tolist()
                        powervalue.append(sum(data) / len(data))
                    else:
                        powervalue.append(0)
                else:
                    tempvalue.append(0)
                    powervalue.append(0)
            tempvalue = pd.DataFrame(tempvalue, index=timestampstr, columns=[path['WTGS_ID']])
            powervalue = pd.DataFrame(powervalue, index=timestampstr, columns=[path['WTGS_ID']])
            q1.put(tempvalue)
            q2.put(powervalue)

        def key_tags(self, farm):
            self.tag_set = {}
            dframe = pd.read_excel("./config/tag/" + farm + ".xlsx", sheetname="sheet1")
            for i in range(len(dframe[MODEL_NAME])):
                if str(dframe[MODEL_NAME].iloc[i]) == '1.0':
                    self.tag_set[dframe.index[i]] = dframe['tag_EN'][i]

class query:

    def __init__(self,start_time, end_time, farm=''):
        self.start_time=start_time
        self.end_time=end_time
        self.farm=[]
        if farm:
            self.farm=farm

    def abnormal_records(self):

        (conn, cur) = sqlite_conn()
        if len(self.farm)>0:
            sqlstr='SELECT farm_code,farm_name,wtgs_id,wtgs_bd,model_name,abnormal_start_time,abnormal_end_time,abnormal_duration,creator ' \
               'FROM early_warning WHERE farm_name=\''+self.farm+'\' AND abnormal_start_time BETWEEN \''+self.start_time+'\' AND \''+self.end_time+'\' AND model_name=\''+MODEL_NAME+'\''
        else:
            sqlstr = 'SELECT farm_code,farm_name,wtgs_id,wtgs_bd,model_name,abnormal_start_time,abnormal_end_time,abnormal_duration,creator ' \
                     'FROM early_warning WHERE abnormal_start_time BETWEEN \'' + self.start_time + '\' AND \'' + self.end_time + '\' AND model_name=\'' + MODEL_NAME + '\''
        res=pd.read_sql(sqlstr,con=conn)
        conn.close()
        return res

    def detail(self,abnormal_record):
        assert len(abnormal_record)==1
        self.cal_farm = farm_path()
        self.key_tags(abnormal_record['farm_name'].iloc[0])
        st =  datetime.datetime.strptime(abnormal_record['abnormal_start_time'].iloc[0], "%Y-%m-%d %H:%M:%S")-timedelta(minutes=5)# plus 5 minutes before abnormal duration
        et =  datetime.datetime.strptime(abnormal_record['abnormal_end_time'].iloc[0], "%Y-%m-%d %H:%M:%S")+timedelta(minutes=5)# plus 5 minutes after abnormal duration
        wtgs_path=self.cal_farm.wtgs_path[abnormal_record['farm_name'].iloc[0]]
        target_path=wtgs_path[wtgs_path['WTGS_ID']==int(abnormal_record['wtgs_id'].iloc[0])]
        (conn, cur) = mysql_conn(target_path['HOST'].iloc[0], int(target_path['PORT'].iloc[0]), REMOTE_DB['user'], REMOTE_DB['passwd'], target_path['DB'].iloc[0])
        sqlstr = 'SELECT '+','.join(self.tag_set)
        sqlstr+=' FROM '+target_path['TABLE_NAME'].iloc[0]+' WHERE real_time BETWEEN \'' + st.strftime('%Y-%m-%d %H:%M:%S') + '\' AND \'' + et.strftime('%Y-%m-%d %H:%M:%S') +'\''
        res=pd.read_sql(sqlstr,con=conn)
        res.to_csv("./DB/result/" + abnormal_record['farm_name'].iloc[0] + '-' + str(
            abnormal_record['wtgs_id'].iloc[0]) + '-' + MODEL_NAME + str(
            round(float(abnormal_record['abnormal_duration'].iloc[0]), 2)) + ".csv")
        return res

    def curve(self,abnormal_record):

        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
        (conn,cur)=sqlite_conn()
        sqlstr = 'SELECT * FROM GenNDETemp WHERE wtgs_id=\''+abnormal_record['wtgs_id'].iloc[0]+'\' AND abnormal_time BETWEEN \'' + abnormal_record['abnormal_start_time'].iloc[0] + '\' AND \'' + abnormal_record['abnormal_end_time'].iloc[0]+ '\''
        res = pd.read_sql(sqlstr, con=conn)
        res.index=[datetime.datetime.strptime(timestr,'%Y-%m-%d %H:%M:%S') for timestr in res['abnormal_time'].tolist()]

        fig, ax1 = plt.subplots(figsize=(15, 8))  # 使用subplots()创建窗口
        ax2 = ax1.twinx()  # 创建第二个坐标轴

        res['generator_NDE_temperature'].plot(ax=ax1,color='r')
        res['avg_temperature'].plot(ax=ax1)
        res['avg_power'].plot(ax=ax2,color='g')
        ax1.legend(['generator_NDE_temperature','avg_temperature'], loc='upper left')
        ax2.legend(['avg_power'], loc='upper right')
        ax1.set_ylabel(MODEL_NAME[0:-2] + '(℃)', fontsize=12)
        ax2.set_ylabel('机组发电功率', fontsize=12)
        ax1.set_xlabel('时间', fontsize=12)
        plt.grid()
        plt.title(abnormal_record['farm_name'].iloc[0] + '-' + str(abnormal_record['wtgs_id'].iloc[0]) + ':' + abnormal_record['abnormal_start_time'].iloc[0] + '-' + abnormal_record['abnormal_end_time'].iloc[0] + '-' + MODEL_NAME + '-' +EVA_METHOD[0])
        # plt.show()
        plt.savefig("./DB/result/"+abnormal_record['farm_name'].iloc[0]+'-'+str(abnormal_record['wtgs_id'].iloc[0])+'-'+MODEL_NAME+'-'+str(round(float(abnormal_record['abnormal_duration'].iloc[0]),2))+".png")

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

if __name__=="__main__":

    aaa = query('2017-11-15 00:00:00', '2017-11-22 00:00:00', '刘利军')
    res = aaa.abnormal_records()
    for i in range(len(res)):
        print(res.ix[i:i])
        aaa.detail(res.ix[i:i])
        aaa.curve(res.ix[i:i])

