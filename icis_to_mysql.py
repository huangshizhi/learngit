# -*- coding: utf-8 -*-
"""
Created on Tue Mar 23 09:40:50 2021

@author: huangshizhi

测试从excel自动更新安迅思数据到数据库

ICIS Excel Plug-In
WDF.Addin

"""
from logger import Logger
import numpy as np
import time
import pandas as pd
from sqlalchemy import create_engine
from scrapy_util import *
from datetime import datetime,date
import time
import win32con
import win32gui
from pymouse import PyMouse

def trigger_update_excel_data(frameclass,frametitle, sleep_time=5):
    '''
    根据excel更新及保存位置刷新并保存数据;
    窗口最大化后更新和保存按钮的坐标不变;
    '''
    stime = time.time()
    hwnd = win32gui.FindWindow(frameclass, frametitle)
    print(hwnd)
    if hwnd != 0:
        win32gui.ShowWindow(hwnd, win32con.SW_SHOWMAXIMIZED)#窗口最大化        
        win32gui.SetForegroundWindow(hwnd)  # 设置前置窗口

    m = PyMouse() #建立鼠标对象
    #icis更新按钮坐标
    icis_location = (155, 13)
    #excel保存按钮坐标
    save_location = (48, 6)
    
    #双击
    log.logger.info("开始更新数据......")
    m.click(icis_location[0],icis_location[1])
    m.click(icis_location[0],icis_location[1])
    m.click(icis_location[0],icis_location[1])
    log.logger.info("数据更新结束！......")
    time.sleep(sleep_time)
    #保存数据
    m.click(save_location[0],save_location[1])
    m.click(save_location[0],save_location[1])
    m.click(save_location[0],save_location[1])

    log.logger.info("保存数据结束！")
    log.logger.info("更新并保存安迅思数据共耗时%.2fs"%(time.time()-stime))
    
def get_quote_type(icis_column_name):
    '''
    根据安迅思列名，得到对应报价类型，如【国内价-进口价-CFR】
    '''
    if 'domestic' in icis_column_name:
        return 'domestic'
    if 'import' in icis_column_name:
        return 'import'
    if 'CFR' in icis_column_name:
        return 'CFR'
    if 'China' in icis_column_name:
        return 'domestic'
    else:
        return ''
  

def update_icis_data(icis_file_name,update_days,isis_column_mapping_data,mysql_engine,schema_name,tmp_schema_name,table_name):
    '''
    读取excel中安迅思数据，更新并加载到mysql数据库
    update_days:每次更新的天数
    isis_column_mapping_data:安迅列映射
    mysql_engine:mysql引擎名
    schema_name:库名
    tmp_schema_name:临时表名
    table_name:表名
    '''
    log.logger.info("开始更新数据......")
    start_time = time.time()
    #读取excel数据
    icis_data = pd.read_excel(icis_file_name,header = 11,index_col= 0,skipfooter = 11)
    icis_data.index = range(len(icis_data))
        
    icis_data['dt'] = icis_data['Date'].apply(lambda x : datetime.strftime(x, "%Y%m%d"))
    print("最大更新日期为:"+str(max(icis_data['dt'])))   
    icis_data = icis_data.drop(['Date'],axis=1)
    #只更新近30天的数据
    icis_data = icis_data[-1*(update_days):]
    #转成窄表
    mydata1=icis_data.melt(id_vars=["dt"],   #要保留的主字段
                        var_name="icis_column_name",  #拉长的分类变量
                        value_name="icis_price"    #拉长的度量值名称
                        )
    
    mydata2 = mydata1.dropna()    
    mydata3 = pd.merge(left = mydata2,right=isis_column_mapping_data,how='left',on=['icis_column_name'])
    mydata3['prod_unit'] = mydata3.icis_column_name.apply(lambda x: x.split(':')[1]) #报价单位
    #价格类型【低-中-高】
    mydata3['price_type'] = mydata3.icis_column_name.apply(lambda x : x[x.rfind('(')+1:x.rfind(')')]) 
    #报价类型【国内-进口价-CFR】
    mydata3['quote_type'] = mydata3.icis_column_name.apply(get_quote_type)  
    #更新频率
    mydata3['original_frequency'] = mydata3.icis_column_name.apply(lambda x : 'Daily' if 'Daily' in x else 'Weekly')  
    mydata3 = mydata3.fillna("")
    
    mydata3['hashkey'] = mydata3.apply(generate_hashkey,args=(['icis_column_name','dt']),axis=1)
        
    update_columns =['dt','icis_column_name','icis_price','mapping_eng_column_name',
                     'mapping_chn_column_name','prod_unit','price_type','quote_type','original_frequency']
    
    save_data_to_mysql(mydata3,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns)
    log.logger.info("完成安迅思数据共更新耗时%.2fs"%(time.time()-start_time))

if __name__=='__main__':
    logfilename = r"E:\project\data_center\code\Log\icis_to_mysql.log"
    #logfilename = r"C:\Log\icis_to_mysql.log"
    log = Logger(logfilename,level='info')
    log.logger.info("-"*50)
    #刷新并保存安迅思数据
    trigger_update_excel_data("XLMAIN", "icis_data.xlsx - Excel",sleep_time=1)
    mysql_con = "mysql+pymysql://root:dZUbPu8eY$yBEoYK@27.150.182.135/"
    mysql_engine = create_engine(mysql_con,encoding='utf-8', echo=False,pool_timeout=3600)
    schema_name = "market_db"
    tmp_schema_name = "tmp_market_db"
    table_name = "icis_spot_data"
    update_days = 30 #全量更新时，取366即可,默认更新近一年数据
    isis_column_mapping_sql = '''SELECT icis_column_name, mapping_eng_column_name, 
    mapping_chn_column_name FROM market_db.isis_column_mapping
    '''
    isis_column_mapping_data = pd.read_sql(isis_column_mapping_sql,con = mysql_engine)
       
    icis_file_name = r"C:\data\icis_data.xlsx" 
       
    #更新安迅思数据
    update_icis_data(icis_file_name,update_days,isis_column_mapping_data,mysql_engine,schema_name,tmp_schema_name,table_name)