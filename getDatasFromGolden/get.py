#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 11:40
# @Author  : liulijun
# @Site    :
# @File    : GearboxNDETemp.py
# @Software: PyCharm
import os
import pandas as pd
os.environ['CLASSPATH'] = "../Lib/my.golden.jar"
from jnius import autoclass
def OneFarmWithMultiTags(wtgs_id,tag_list,start_time,end_time):#查数据
    assert type(wtgs_id)==str,'请输入字符型机组号, as \'10001001\'！'
    assert len(wtgs_id) == 8, '请输入8位的机组号！'
    assert type(tag_list) == list, '请输入列表型标签点，as [\'a\',\'b\']！'
    assert start_time<=end_time, '查询开始时间大于结束时间！'
    server_impl = autoclass('com.rtdb.service.impl.ServerImpl')
    server = server_impl("192.168.0.37", 6327, "sa", "golden") # 登录
    base_class = autoclass('com.rtdb.service.impl.BaseImpl')
    base = base_class(server)
    historian_impl = autoclass('com.rtdb.service.impl.HistorianImpl')
    his = historian_impl(server)
    data_sort_calss = autoclass('com.rtdb.enums.DataSort')
    condition_class = autoclass('com.rtdb.model.SearchCondition')
    condition = condition_class()
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    return_val={}
    return_val['realtime']=[str(time) for time in pd.date_range(start=start_time, end=end_time, freq='S')]
    for tag in tag_list:
        valuelist = []
        condition.setTagmask("*" + wtgs_id + "*" + tag)
        points_ids = base.search(condition, 200, data_sort_calss.SORT_BY_TAG)  # 根据表名 批量获取
        if base.getTypes(points_ids)[0].getNum() in [6,7,8,9]:#INT类型
            result = his.getIntInterpoValues(int(points_ids[0]), count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
        else:#float类型
            result = his.getFloatInterpoValues(int(points_ids[0]), count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
        if result.size() > 0:
            for i in range(result.size()):
                r = result.get(i)
                if r.getValue() is not None or r.getValue()!=[]:
                    valuelist.append(r.getValue()) # 存储值序列
                else:
                    valuelist.append('')
            return_val[tag]=valuelist
    return_val=pd.DataFrame.from_dict(return_val)
    return_val=return_val[['realtime']+tag_list]
    return_val.index = return_val['realtime']
    return_val.drop(['realtime'])
    return_val = return_val[tag_list]
    # print(return_val)
    return return_val

def MultiFarmWithOneTag(wtgs_list,tag,start_time,end_time):#查数据
    assert type(wtgs_list)==list,'请输入列表型机组号, as [\'10001001\',\'10001002\']！'
    assert type(tag) == str, '请输入字符串型标签点名！'
    assert start_time<=end_time, '查询开始时间大于结束时间！'
    server_impl = autoclass('com.rtdb.service.impl.ServerImpl')
    server = server_impl("192.168.0.37", 6327, "sa", "golden") # 登录
    base_class = autoclass('com.rtdb.service.impl.BaseImpl')
    base = base_class(server)
    historian_impl = autoclass('com.rtdb.service.impl.HistorianImpl')
    his = historian_impl(server)
    data_sort_calss = autoclass('com.rtdb.enums.DataSort')
    condition_class = autoclass('com.rtdb.model.SearchCondition')
    condition = condition_class()
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    return_val={}
    return_val['realtime']=[str(time) for time in pd.date_range(start=start_time, end=end_time, freq='S')]
    for wtgs_id in wtgs_list:
        valuelist = []
        # print("*" + wtgs_id + "*" + tag)
        condition.setTagmask("*" + wtgs_id + "*" + tag)
        points_ids = base.search(condition, 200, data_sort_calss.SORT_BY_TAG)  # 根据表名 批量获取
        if base.getTypes(points_ids)[0].getNum() in [6,7,8,9]:#INT类型
            result = his.getIntInterpoValues(int(points_ids[0]), count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
        else:#float类型
            result = his.getFloatInterpoValues(int(points_ids[0]), count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
        if result.size() > 0:
            for i in range(result.size()):
                r = result.get(i)
                if r.getValue() is not None or r.getValue()!=[]:
                    valuelist.append(r.getValue()) # 存储值序列
                else:
                    valuelist.append('')
            return_val[wtgs_id]=valuelist
    return_val=pd.DataFrame.from_dict(return_val)
    return_val=return_val[['realtime']+wtgs_list]
    return_val.index=return_val['realtime']
    return_val = return_val[wtgs_list]
    # print(return_val)
    return return_val

if __name__=="__main__":
    MultiFarmWithOneTag(['30002001','30002002'],'grgridactivepower', '2017-12-21 00:00:00', '2017-12-21 00:10:00')