# -*- coding: utf-8 -*-
"""
Created on Thu Feb  4 16:15:28 2021

@author: huangshizhi

爬虫加工常用util方法

"""
import re
import calendar
import time
import hashlib
import pandas as pd
from datetime import datetime
from pandas.io import sql
from loguru import logger


@logger.catch
def generate_hashkey(x,*args):
    '''
    根据对应的列生成对应的hashkey,如args=(['dt','type_name','prod_standard'])
    hashkey由('20200820', '国内石化PE生产比例汇总', '100级管材')生成
    '''  
    arg_list = [x[a] for a in args]
    b = str(tuple(arg_list))  
    #print(b)
    hashkey = hashlib.md5(b.encode('utf-8')).hexdigest()
    return hashkey


@logger.catch
def save_data_to_mysql(df,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns):
    '''
    增量更新数据
    table_name:存储到mysql的表名
    通过临时表进行update
    判断hashkey是否重复,更新重复的hashkey再添加到数据库表中
    df = pe_factory_percent_all_data.copy()
    schema_name = "market_db"
    tmp_schema_name = "tmp_market_db"
    table_name = "pe_factory_percent"
    update_columns = ['dt','type_name','prod_standard']
    update_str =",".join( ["s."+x +" =  t."+x for x in update_columns])
    
    '''
  
    conn = mysql_engine.connect()
    try:
        conn.info
    except Exception as e:
        logger.info(e)
        conn = mysql_engine.connect()
    finally:   
        stime = time.time()
        #更新字段设置
        update_str =",".join( [" s."+x +" =  t."+x for x in update_columns])      
        insert_columns = str(tuple(list(df.columns))).replace("'","") #插入目标表字段
        #临时表字段
        tmp_insert_columns = str(tuple(['t.'+ x for x in list(df.columns)])).replace("'","").replace("(","").replace(")","")
        #插入到临时表
        df.to_sql(table_name, con=mysql_engine, if_exists='replace',schema= tmp_schema_name,index=False,chunksize=50000,method='multi')
        #更新的SQL语句
        update_sql = "update "+schema_name+"."+table_name+" s inner join " + tmp_schema_name+"."+table_name \
                    +" t on s.hashkey=t.hashkey set " + update_str+",s.update_time = CURRENT_TIMESTAMP() where 1 = 1"
        #插入的SQL语句     
        insert_sql = "INSERT INTO "+schema_name+"."+table_name+ insert_columns \
            +"(select " + tmp_insert_columns +" from " + tmp_schema_name+"."+table_name \
            + " t where t.hashkey not in (select hashkey from "+schema_name+"."+table_name+"))"
         
        #先更新数据
        sql.execute(update_sql, con=mysql_engine)
        logger.info("更新"+tmp_schema_name+"."+table_name+"导入到数据库耗时%.2fs"%(time.time()-stime))
        #再插入新的数据
        sql.execute(insert_sql, con=mysql_engine)
        conn.invalidate()
        logger.info(schema_name+"."+table_name+"增量更新到数据库耗时%.2fs"%(time.time()-stime))

@logger.catch        
def truncate_insert_data(df,mysql_engine,schema_name,table_name):
    '''
    先清空数据再插入数据
    '''  
    conn = mysql_engine.connect()
    try:
        conn.info
    except Exception as e:
        logger.info(e)
        conn = mysql_engine.connect()
    finally:   
        stime = time.time()
        truncate_sql = "truncate table " +schema_name+"."+table_name+";"
        #先更新数据
        sql.execute(truncate_sql, con=mysql_engine)
        df.to_sql(table_name, con=mysql_engine, if_exists='append',schema= schema_name,index=False,chunksize=50000,method='multi')
        logger.info("导入"+schema_name+"."+table_name+"到数据库耗时%.2fs"%(time.time()-stime))

@logger.catch
def get_month_list(start_dt,end_dt,freq='M'):
    '''
    根据起始日期返回对应月份list
    如 start_dt = '20210605',end_dt = '20210818'
    返回[202106,202107,202108]
    '''
    period_list  =  pd.period_range(start = start_dt,end = end_dt,freq = freq)
    period_list2 = period_list.astype('datetime64[ns]').to_frame()[0].tolist()
    #month_list = [datetime.strftime(x,'%Y%m') for x in period_list2]   
    
    return period_list2


@logger.catch
def get_dateframe(start_dt,end_dt,freq='D'):
    '''
    根据起始日期返回对应日期数据框
    '''
    period_list  =  pd.period_range(start = start_dt,end = end_dt,freq = freq)
    period_df = period_list.astype('datetime64[ns]').to_frame()  
    period_df.columns=['dt']
    period_df.index = range(len(period_df))
    period_df['dt'] = period_df['dt'].apply(lambda x : datetime.strftime(x,"%Y%m%d"))
       
    return period_df

@logger.catch 
def get_month_loss_capacity(maintenance_start_dt,maintenance_end_dt,annual_prod_capacity,chn_pattern):
    '''
    根据检修开始、结束日期、年产能及文本匹配情况计算每月检修损失量
    Parameters
    ----------
    maintenance_start_dt : 检修开始日期
    maintenance_end_dt : 检修结束日期
    annual_prod_capacity : 年产能
    chn_pattern:中文正则表达式
    Returns
    -------
    month_loss_capacity : TYPE
        DESCRIPTION.

    p4.index = range(len(p4))
    idx = 0
    maintenance_start_dt = p4.iloc[idx]['maintenance_start_dt']
    maintenance_end_dt = p4.iloc[idx]['maintenance_end_dt']
    annual_prod_capacity = p4.iloc[idx]['annual_prod_capacity']
    '''
    
    month_loss_capacity_list = []
    #根据检修开始日期，检修结束日期计算每月损失量
    if (len("".join(re.findall(chn_pattern,maintenance_start_dt)))==0) & (len("".join(re.findall(chn_pattern,maintenance_end_dt)))==0):
        start_dt = datetime.strptime(maintenance_start_dt,'%Y%m%d')
        end_dt = datetime.strptime(maintenance_end_dt,'%Y%m%d')
        start_month_str = datetime.strftime(datetime.strptime(maintenance_start_dt,'%Y%m%d'),'%Y%m')
        start_month = start_dt.month
        end_month = end_dt.month
        
        #判断检修日期是否在同一个月内
        if (start_month == end_month)&(start_dt.year==end_dt.year):
            month_loss_capacity = {}
            maintenance_days = (end_dt - start_dt).days+1
            month_loss_capacity['loss_month'] = start_month_str
            month_loss_capacity['month_loss_capacity'] = round(float(annual_prod_capacity)*0.003*maintenance_days,4)        
            month_loss_capacity_list.append(month_loss_capacity)
        else:
            month_list = get_month_list(start_dt,end_dt,freq='M')
            #month_list = list(range(start_month,end_month+1))
            #month_list = pd.date_range(start_dt,end_dt,closed=None,normalize=(True),freq='M').to_frame()[0].tolist()
            for m in month_list:                
                month_loss_capacity = {}
                this_month_start_dt = datetime(m.year, m.month, 1)
                this_month_str = datetime.strftime(this_month_start_dt,'%Y%m')
                this_month_end_dt = datetime(m.year, m.month, calendar.monthrange(start_dt.year, m.month)[1])
                #结束日期大于月末日期
                if this_month_end_dt <= end_dt:
                    maintenance_start_dt  = max(this_month_start_dt,start_dt)
                    #检修日期=月末日期-检修开始日期+1
                    maintenance_days = (this_month_end_dt - maintenance_start_dt).days+1
                else:
                    #结束日期小于月末日期
                    maintenance_end_dt  = min(this_month_end_dt,end_dt)
                    #检修日期=检修结束日期-月初日期+1
                    maintenance_days = (maintenance_end_dt - this_month_start_dt).days+1            
                
                #month_loss_capacity[this_month_str] = round(float(annual_prod_capacity)*0.003*maintenance_days,4)
                month_loss_capacity['loss_month'] = this_month_str
                month_loss_capacity['month_loss_capacity'] = round(float(annual_prod_capacity)*0.003*maintenance_days,4)
                month_loss_capacity_list.append(month_loss_capacity)
    
    return month_loss_capacity_list


def get_ffill_device_data(device_data):
    '''
    按照厂商和产线进行分组补前值
    '''   
    ffill_device_data = pd.DataFrame()
    tmp_df = device_data[['prod_factory','prod_line_id']].drop_duplicates()
    prod_factory_line_tuple = list(zip(tmp_df['prod_factory'],tmp_df['prod_line_id']))
    for i in range(len(prod_factory_line_tuple)):   
        device_data2 = device_data[(device_data['prod_factory']==prod_factory_line_tuple[i][0])
                                   &(device_data['prod_line_id']==prod_factory_line_tuple[i][1])]     
        #p5 = p4[(p4['prod_factory']=='福建联合')&(p4['prod_line_id']=='line_1')]   
        min_dt = min(device_data2['dt'])
        today = datetime.strftime(datetime.now(),'%Y%m%d')
        period_df = get_dateframe(min_dt,today,freq='D')
        device_data3 = pd.merge(left=device_data2,right=period_df,how='right',on='dt')
        device_data3['prod_remark'] = device_data3['prod_remark'].fillna("爬虫补前值")
        device_data4 = device_data3.fillna(method="ffill")
        ffill_device_data = pd.concat([ffill_device_data,device_data4],axis=0)
    
    ffill_device_data.index = range(len(ffill_device_data))
    return ffill_device_data


