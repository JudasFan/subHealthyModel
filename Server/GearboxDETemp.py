#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 11:40
# @Author  : liulijun
# @Site    : 
# @File    : GearboxDETemp.py
# @Software: PyCharm

from getDatasFromGolden import get
import pandas as pd
from datetime import datetime,timedelta
import pymysql
import sqlite3

MODEL_NAME = '齿轮箱驱动端轴承温度异常'
ANALYSOR='刘利军'
EVA_METHOD = ['箱形图', '均值']
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

    conn = sqlite3.connect('../DB/early_warning.db')
    cur = conn.cursor()
    return conn, cur

class generate:

    def __init__(self, start_time, end_time, author):
        self.start_time=start_time
        self.end_time=end_time
        self.author=author
        self.cal_farm=farm_path()
        self.loop()

    def loop(self):
        for farm in self.cal_farm.farm_name:
            print(farm, 'begin...')
            self.run(self.cal_farm.wtgs_path[farm], self.start_time, self.end_time, self.author)

    class run:
        # return the warning record of a wtgs based run data of a period
        def __init__(self, wtgs_path, start_time, end_time, author):
            self.wtgs_path = wtgs_path
            self.start_time = start_time
            self.end_time = end_time
            self.author = author
            self.key_tags(wtgs_path['FARM_NAME'].iloc[0])
            for key in self.tag_set.keys():  # select the english name of tag based on matching
                if '齿轮箱' in key and '轴承温度' in key and ('DE端' in key or '驱动端' in key) and ('N' not in key and '非' not in key):
                    self.temp_tag = self.tag_set[key]
                if '运行状态' in key:
                    self.run_condition_tag = self.tag_set[key]
                if '功率' in key:
                    self.power = self.tag_set[key]
            self.mainstep()  # run the algorithm and get the warnning info
            self.export()  # export warning info to .DB (local or remote)

        def mainstep(self):
            # step1:query real data
            wtgs_list=[]
            for index, row in self.wtgs_path.iterrows():
                wtgs_list.append(str(self.wtgs_path.ix[index, :]['WTGS_ID']))
            wtgsPowerValue=get.MultiWtgsWithOneTag(wtgs_list, self.power, self.start_time, self.end_time)
            wtgsTemperatureValue = get.MultiWtgsWithOneTag(wtgs_list, self.temp_tag, self.start_time, self.end_time)
            print(self.wtgs_path['FARM_NAME'].iloc[0],'query finished')
            # generate the abnormal record using boxplot or mean method
            AbnormalRecord = []
            for time, row in wtgsPowerValue.iterrows():
                # part1: divide the wtgs into different group based on its power
                wtgs_as_power = {str(i): [] for i in range(len(POWER_SPLITER) + 1)}
                power_data = wtgsPowerValue.ix[time]
                temperature_data = wtgsTemperatureValue.ix[time]
                for wtgs in wtgs_list:
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
                        temp_value_group = temperature_data.ix[[str(wtgs) for wtgs in wtgsgroup]]
                        avg_power = power_data.ix[[str(wtgs) for wtgs in wtgsgroup]]
                        avg_power = sum(avg_power) / len(avg_power)
                        [max_value, avg_temp] = quartile(temp_value_group.tolist()) #四分位方法
                        for abwtgs in temp_value_group.index:
                            if temp_value_group.ix[abwtgs] >= max_value and max_value != 0:
                                AbnormalRecord.append([self.wtgs_path['FARM_CODE'].iloc[0], self.wtgs_path['FARM_NAME'].iloc[0], abwtgs, MODEL_NAME, time, temp_value_group.ix[abwtgs], avg_temp, avg_power])
                            else:
                                continue
            self.abnormal_detail = pd.DataFrame(AbnormalRecord,columns=['farm_code', 'farm_name', 'wtgs_id', 'model_name', 'abnormal_time','gearbox_DE_temperature', 'avg_temperature', 'avg_power'])
            print(self.wtgs_path['FARM_NAME'].iloc[0], 'calculate finished!')

        def export(self):
            print(self.abnormal_detail)
            if len(self.abnormal_detail) > 0:
                try:
                    (conn, cur) = sqlite_conn()
                    pd.io.sql.to_sql(self.abnormal_detail, 'GearboxDETemp', con=conn, if_exists='append')
                    conn.close()
                    print(self.wtgs_path['FARM_NAME'].iloc[0], 'export finished!')
                except:
                    print('may be data repeat')
            else:
                print(self.wtgs_path['FARM_NAME'].iloc[0], 'each wtgs is running well!')

        def key_tags(self, farm):
            self.tag_set = {}
            dframe = pd.read_excel("./config/tag/" + farm + ".xlsx", sheetname="sheet1")
            for i in range(len(dframe[MODEL_NAME])):
                if str(dframe[MODEL_NAME].iloc[i]) == '1.0':
                    self.tag_set[dframe.index[i]] = dframe['tag_EN'][i]

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

def abnormalRecordMeta(wtgs_path,abnormal_wtgs_record,rowi,rowj):
    # export data structure
    ex_farm_code = str(abnormal_wtgs_record['farm_code'].iloc[rowi])
    ex_farm_name = str(abnormal_wtgs_record['farm_name'].iloc[rowi])
    ex_wtgs_id = str(abnormal_wtgs_record['wtgs_id'].iloc[rowi])
    ex_wtgs_bd = str(wtgs_path[wtgs_path['WTGS_ID'] == int(abnormal_wtgs_record['wtgs_id'].iloc[0])]['WTGS_NAME'].iloc[0])
    ex_start_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowi])
    ex_end_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowj - 1])
    ex_duration = str((datetime.strptime(ex_end_time, "%Y-%m-%d %H:%M:%S") - datetime.strptime(ex_start_time, "%Y-%m-%d %H:%M:%S")).seconds / 3600)
    return [ex_farm_code, ex_farm_name, ex_wtgs_id, ex_wtgs_bd, MODEL_NAME, '', ex_start_time, ex_end_time, ex_duration, ANALYSOR, EVA_METHOD[0], datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '', '', '', '', '', '', '', '', '', '', '', '', '']

def run_record(currentTime,seccess_flag):
    try:
        [conn,cur]=sqlite_conn()
        sqlstr = "INSERT INTO run_record VALUES (\'"+MODEL_NAME+"\',\'ALL\',\'"+currentTime+"\',\'"+seccess_flag+"\')"
        cur.execute(sqlstr)
        conn.commit()
        conn.close()
    except:
        print('repeat input!')
    finally:
        pass

def main():
    conn = sqlite3.connect('../DB/early_warning.db')
    sqlstr = "SELECT MAX(run_time) FROM run_record WHERE model_name=\'"+MODEL_NAME+"\' AND seccess_flag=\'1'"
    latest_cal_time = pd.read_sql(sql=sqlstr, con=conn)
    conn.close()
    if len(latest_cal_time)>1:
        from_time = str(latest_cal_time['MAX(run_time)'].iloc[0])  # 已经计算的最新时间
    else:
        from_time=(datetime.now()+timedelta(days=-10)).strftime("%Y-%m-%d %H:%M:%S")  # 当前时间往前推7天
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        generate(from_time, currentTime, ANALYSOR)
        run_record(currentTime, '1')
    except:
        run_record(currentTime, '0')

if __name__=="__main__":
    main()

