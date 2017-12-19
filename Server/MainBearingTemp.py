#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 11:40
# @Author  : liulijun
# @Site    : 
# @File    : MainBearingTemp.py
# @Software: PyCharm

import multiprocessing
from multiprocessing import Pool
import pandas as pd
from datetime import *
import datetime
import pymysql
import matplotlib.pyplot as plt
import sqlite3

MODEL_NAME = '主轴轴承温度异常'
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
    # create connect service
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
        for farm in self.cal_farm.farm_name:# loop all farm
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
                if '轴承' in key and '温度' in key and 'A' in key:
                    self.A_temp_tag = self.tag_set[key]
                if '轴承' in key and '温度' in key and 'B' in key:
                    self.B_temp_tag = self.tag_set[key]
                if '功率' in key:
                    self.power_tag = self.tag_set[key]
                if '运行状态' in key:
                    self.run_condition_tag = self.tag_set[key]
            # print(self.U_temp_tag,self.V_temp_tag,self.W_temp_tag)
            self.run()  # run the algorithm and get the warnning info
            self.export()  # export warning info to .DB (local or remote)

        def run(self):
            # step1:query real data
            manager = multiprocessing.Manager()
            q1 = manager.Queue()  # hold the value of power
            q2 = manager.Queue()  # hold the value of A_temp
            q3 = manager.Queue()  # hold the value of B_temp
            p = Pool(processes=2)  # process pool
            # multiprocess: query temperature value based on avg(mins)
            for index, row in self.wtgs_path.iterrows():
                wtgs = self.wtgs_path.ix[index, :]
                result = p.apply_async(self.query_temp_mins_value, args=(wtgs, q1, q2, q3))
            p.close()
            p.join()
            if not result.successful():
                print("unfortunately, failed to add process to pool...")
            # merge
            self.wtgsPowerValue = pd.DataFrame()
            self.ATempValue = pd.DataFrame()
            self.BTempValue = pd.DataFrame()
            while not q1.empty():
                if self.wtgsPowerValue.empty:
                    self.wtgsPowerValue = q1.get()
                else:
                    self.wtgsPowerValue = self.wtgsPowerValue.join(q1.get())
            while not q2.empty():
                if self.ATempValue.empty:
                    self.ATempValue = q2.get()
                else:
                    self.ATempValue = self.ATempValue.join(q2.get())
            while not q3.empty():
                if self.BTempValue.empty:
                    self.BTempValue = q3.get()
                else:
                    self.BTempValue = self.BTempValue.join(q3.get())
            print(self.wtgs_path['FARM_NAME'].iloc[0],'query finished')
            # generate the abnormal record using boxplot or mean method and loop u,v,w
            for TempValue in [self.ATempValue, self.BTempValue]:
                new_abres_detail,new_abres_info=self.ab_abnormal(TempValue)
                if len(new_abres_info)>0:
                    self.abnormal_records=pd.concat(self.abnormal_records,new_abres_info)
                    self.abnormal_detail=pd.concat(self.abnormal_detail,new_abres_detail)
            print(self.wtgs_path['FARM_NAME'].iloc[0], 'calculate finished!')

        def ab_abnormal(self, TempValue):
            AbnormalRecord = []
            wtgslist = list(self.wtgsPowerValue.columns)
            for time, row in self.wtgsPowerValue.iterrows():
                # 第一步：按照功率进行机组群划分
                wtgs_as_power = {str(i): [] for i in range(len(POWER_SPLITER) + 1)}
                power_data = self.wtgsPowerValue.ix[time]  # 某个时间点的功率数据（包含所有机组）
                temp_data = TempValue.ix[time]   #某个时间点的温度数据
                for wtgs in wtgslist: # 按照功率区间进行机组群划分
                    value = power_data[wtgs] # 取机组wtgs的功率
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
                    else:
                        pass
                # 第二步: 按照机组群对温度数据进行查异常
                for key, wtgsgroup in wtgs_as_power.items():
                    if len(wtgsgroup) >= 4:  # wtgsgroup异常机组群
                        power_value_group = power_data.ix[[int(wtgs) for wtgs in wtgsgroup]]  # 异常机组群的功率集合
                        avg_power_value = sum(power_value_group) / len(power_value_group)  # 异常机组群中的平均功率
                        tempvalue = temp_data.ix[[int(wtgs) for wtgs in wtgsgroup]]
                        [max_value, avg_temp] = quartile(tempvalue.tolist())  # 四分位方法其上限
                        for abwtgs in tempvalue.index:
                            if tempvalue.ix[abwtgs] >= max_value and max_value != 0:
                                AbnormalRecord.append([self.wtgs_path['FARM_CODE'].iloc[0], self.wtgs_path['FARM_NAME'].iloc[0], abwtgs,MODEL_NAME, time, tempvalue.ix[abwtgs], avg_temp, avg_power_value])
                            else:
                                continue
            # part3:filter the orignal abnormal record with 60 mins, if a wtgs is under abnormal status more than 10 mins, which selected
            AbnormalRecord = pd.DataFrame(AbnormalRecord,columns=['farm_code', 'farm_name', 'wtgs_id', 'model_name', 'abnormal_time','generator_UVW_temperature', 'avg_UVW_temperature', 'avg_power'])
            print(AbnormalRecord)
            SelectedAbnormalRecord = pd.DataFrame()
            abnormal_records=[]
            abnormal_wtgs_list = sorted(list(set(AbnormalRecord['wtgs_id'].tolist())))
            for abnormal_wtgs in abnormal_wtgs_list:  # loop all possible wtgs
                abnormal_wtgs_record = AbnormalRecord[AbnormalRecord['wtgs_id'] == abnormal_wtgs]
                if len(abnormal_wtgs_record) >= 60 / MINUTES:
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
                            elif (datetime.datetime.strptime(abnormal_wtgs_record['abnormal_time'].iloc[rowj - 1],"%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(abnormal_wtgs_record['abnormal_time'].iloc[rowi],"%Y-%m-%d %H:%M:%S")).seconds >= 120:#10min
                                if SelectedAbnormalRecord.empty:
                                    SelectedAbnormalRecord = abnormal_wtgs_record.iloc[rowi:rowj]
                                    # export data structure
                                    ex_farm_code = str(abnormal_wtgs_record['farm_code'].iloc[rowi])
                                    ex_farm_name = str(abnormal_wtgs_record['farm_name'].iloc[rowi])
                                    ex_wtgs_id = str(abnormal_wtgs_record['wtgs_id'].iloc[rowi])
                                    ex_wtgs_bd = str(self.wtgs_path[self.wtgs_path['WTGS_ID'] ==abnormal_wtgs_record['wtgs_id'].iloc[rowi]]['WTGS_NAME'].iloc[0])
                                    ex_start_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowi])
                                    ex_end_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowj - 1])
                                    ex_duration = str((datetime.datetime.strptime(ex_end_time,"%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(ex_start_time, "%Y-%m-%d %H:%M:%S")).seconds / 3600)
                                    abnormal_records.append([ex_farm_code, ex_farm_name, ex_wtgs_id, ex_wtgs_bd, MODEL_NAME, '',ex_start_time, ex_end_time, ex_duration, ANALYSOR, EVA_METHOD[0],datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '', '', '', '', '', '','', '', '', '', '', '', ''])
                                    rowi = rowj
                                    break
                                else:
                                    SelectedAbnormalRecord = pd.concat([SelectedAbnormalRecord, abnormal_wtgs_record.iloc[rowi:rowj]])
                                    # export data structure
                                    ex_farm_code = str(abnormal_wtgs_record['farm_code'].iloc[rowi])
                                    ex_farm_name = str(abnormal_wtgs_record['farm_name'].iloc[rowi])
                                    ex_wtgs_id = str(abnormal_wtgs_record['wtgs_id'].iloc[rowi])
                                    ex_wtgs_bd = str(self.wtgs_path[self.wtgs_path['WTGS_ID'] ==abnormal_wtgs_record['wtgs_id'].iloc[rowi]]['WTGS_NAME'].iloc[0])
                                    ex_start_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowi])
                                    ex_end_time = str(abnormal_wtgs_record['abnormal_time'].iloc[rowj - 1])
                                    ex_duration = str((datetime.datetime.strptime(ex_end_time,"%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(ex_start_time, "%Y-%m-%d %H:%M:%S")).seconds / 3600)
                                    abnormal_records.append([ex_farm_code, ex_farm_name, ex_wtgs_id, ex_wtgs_bd, MODEL_NAME, '',ex_start_time, ex_end_time, ex_duration, ANALYSOR, EVA_METHOD[0],datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), '', '', '', '', '', '','', '', '', '', '', '', ''])
                                    rowi = rowj
                                    break
                            else:
                                rowi = rowj
                                break
            if len(abnormal_records) > 0:
                abnormal_records = pd.DataFrame(self.abnormal_records,
                                                     columns=['farm_code', 'farm_name', 'wtgs_id', 'wtgs_bd',
                                                              'model_name', 'abnormal_group', 'abnormal_start_time',
                                                              'abnormal_end_time', 'abnormal_duration', 'creator',
                                                              'analysis_creator', 'create_time', 'release_time',
                                                              'result_engtec_dep', 'description_engtec_dep',
                                                              'checktime_engtec_dep', 'serial_number', 'serial_creator',
                                                              'serial_create_time', 'abnormal_level', 'serial_status',
                                                              'check_time_farm', 'result_farm', 'description_farm',
                                                              'delete'])
            return SelectedAbnormalRecord,abnormal_records

        def export(self):
            if len(self.abnormal_records) > 0:
                (conn, cur) = sqlite_conn()
                pd.io.sql.to_sql(self.abnormal_records, 'early_warning', con=conn, if_exists='append')
                pd.io.sql.to_sql(self.abnormal_detail, 'MainBearingTemp', con=conn, if_exists='append')
                conn.close()
                try:
                    print(self.wtgs_path['FARM_NAME'].iloc[0], 'export finished!')
                except:
                    print('may be data repeat')
            else:
                print(self.wtgs_path['FARM_NAME'].iloc[0], 'each wtgs is running well!')

        def query_temp_mins_value(self, path, q1, q2, q3, q4):

            (conn, cur) = mysql_conn(path['HOST'], path['PORT'], REMOTE_DB['user'], REMOTE_DB['passwd'], path['DB'])
            starttimestamp = datetime.datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")
            endtimestmp = datetime.datetime.strptime(self.end_time, "%Y-%m-%d %H:%M:%S")
            # expand the query time(MINUTES/2 minutes before start time and MINUTES/2 minutes after end time
            sqlstr = "SELECT wtid,real_time," + self.A_temp_tag + ',' +self.B_temp_tag + ','+self.power_tag+" FROM " + path['TABLE_NAME'] + \
                     " WHERE " \
                     + self.power_tag + " is not Null and " + \
                     self.A_temp_tag + " is not Null and " + \
                     self.B_temp_tag + " is not Null and " + \
                     self.run_condition_tag + "=\'3\' AND real_time BETWEEN \'" + \
                     str(starttimestamp - timedelta(minutes=MINUTES / 2)) + "\' AND \'" \
                     + str(endtimestmp + timedelta(minutes=MINUTES / 2)) + "\' ORDER BY real_time "
            # print(sqlstr)
            res = pd.read_sql(sqlstr, con=conn)
            # print(res)
            conn.close()
            # generate a list of timestamp
            timestampstr = []
            timestamp = starttimestamp
            while timestamp <= endtimestmp:
                timestampstr.append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                timestamp += timedelta(minutes=MINUTES)
            # get avg value of four tag
            powervalue = []
            Avalue = []
            Bvalue = []
            for id in range(len(timestampstr)):
                fromtime = (datetime.datetime.strptime(timestampstr[id], "%Y-%m-%d %H:%M:%S") - timedelta(minutes=MINUTES / 2)).strftime("%Y-%m-%d %H:%M:%S")
                totime = (datetime.datetime.strptime(timestampstr[id], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=MINUTES / 2)).strftime("%Y-%m-%d %H:%M:%S")
                if len(res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)])>0:
                    if len(res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.power_tag]) > 0:
                        data = res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.power_tag]
                        data = data.dropna(axis=0, how='any').tolist()
                        powervalue.append(sum(data) / len(data))
                    else:
                        powervalue.append(0)
                    if len(res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.U_temp_tag]) > 0:
                        data = res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.U_temp_tag]
                        data = data.dropna(axis=0, how='any').tolist()
                        Avalue.append(sum(data) / len(data))
                    else:
                        Avalue.append(0)
                    if len(res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.V_temp_tag]) > 0:
                        data = res[(res['real_time'] >= fromtime) & (res['real_time'] <= totime)][self.V_temp_tag]
                        data = data.dropna(axis=0, how='any').tolist()
                        Bvalue.append(sum(data) / len(data))
                    else:
                        Bvalue.append(0)
                else:
                    powervalue.append(0)
                    Avalue.append(0)
                    Bvalue.append(0)
            powervalue = pd.DataFrame(powervalue, index=timestampstr, columns=[path['WTGS_ID']])
            Avalue = pd.DataFrame(Avalue, index=timestampstr, columns=[path['WTGS_ID']])
            Bvalue = pd.DataFrame(Bvalue, index=timestampstr, columns=[path['WTGS_ID']])
            q1.put(powervalue)
            q2.put(Avalue)
            q3.put(Bvalue)

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
        return res

    def curve(self,abnormal_record):

        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
        (conn,cur)=sqlite_conn()
        sqlstr = 'SELECT * FROM MainBearingTemp WHERE wtgs_id=\''+abnormal_record['wtgs_id'].iloc[0]+'\' AND abnormal_time BETWEEN \'' + abnormal_record['abnormal_start_time'].iloc[0] + '\' AND \'' + abnormal_record['abnormal_end_time'].iloc[0]+ '\''
        # print(sqlstr)
        res = pd.read_sql(sqlstr, con=conn)
        res.index=[datetime.datetime.strptime(timestr,'%Y-%m-%d %H:%M:%S') for timestr in res['abnormal_time'].tolist()]
        res['main_bearing_temperature'].plot()
        res['avg_main_bearing_temperature'].plot()
        res['avg_power'].plot()
        plt.legend(loc='best')
        plt.grid()
        plt.title(abnormal_record['farm_name'].iloc[0] + '-' + str(abnormal_record['wtgs_id'].iloc[0]) + ':' + abnormal_record['abnormal_start_time'].iloc[0] + '-' + abnormal_record['abnormal_end_time'].iloc[0] + '-' + MODEL_NAME + '-' +EVA_METHOD[0])
        plt.show()
        # plt.savefig(self.file_path + abnormal_record[1] + '-' + abnormal_record[3] + '-' + abnormal_record[4] + '-' +abnormal_record[5] + '-' + abnormal_record[6].strftime('%Y-%m-%d %H-%M-%S') + '-' + abnormal_record[7].strftime('%Y-%m-%d %H-%M-%S') + '.png')

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
    aaa = generate('2017-11-01 00:00:00', '2017-11-08 00:00:00', '刘利军')

