# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 15:34:00 2021

@author: huangshizhi

PE爬虫数据加工

"""
import numpy as np
import time
import pandas as pd
import re
from pymongo import MongoClient
from sqlalchemy import create_engine
from datetime import datetime,date
from scrapy_util import generate_hashkey,get_month_loss_capacity,save_data_to_mysql,get_dateframe,get_ffill_device_data
from loguru import logger

@logger.catch
def get_pdata(mycol):
    '''
    从mongoDB取数,返回DataFrame
    '''
    start_time = time.time()
    dlist = []
    for x in mycol.find():
        dlist.append(x)
    logger.info("加载mongodb数据库数据耗时%.2fs"%(time.time()-start_time))
    pdata = pd.DataFrame(data=dlist)
    return pdata

@logger.catch
def generate_brand(prod_brand,prod_brand_change):
    '''
    生成新的牌号
    '''
    #logger.info(prod_brand,'1',prod_brand_change,'2',dt)
    if len(str(prod_brand_change))== 0:
        prod_brand = prod_brand
    if len(str(prod_brand))==0:
        prod_brand = prod_brand_change
        
    return prod_brand
    
@logger.catch
def generate_init_dt(prod_brand,prod_brand_change,dt):
    '''
    根据网页加载的数据,生成新的初始日期
    '''
    if len(str(prod_brand_change))== 0:
        dt = dt
    if len(str(prod_brand))==0:
        dt = ""
        
    return dt

@logger.catch
def generate_load_dt(uploadTime,init_dt):
    '''
    根据网页更新日期uploadTime,加工后的初始日期init_dt,生成加载日期
    '''
    if len(str(init_dt)) == 0:
        dt = datetime.strftime(datetime.strptime(uploadTime,"%Y-%m-%d %H:%S"),"%Y%m%d")
    else :       
        month = init_dt[:init_dt.find('月')]
        day = init_dt[init_dt.find('月')+1:init_dt.find('日')]
        year = datetime.strptime(uploadTime,"%Y-%m-%d %H:%S").year
        dt_datetime = date(year,int(month),int(day)) #拼接成日期
        dt = datetime.strftime(dt_datetime,"%Y%m%d")
        
    return dt


@logger.catch
def transfer_data(df2,melt_columns):
    '''
    加工处理原始数据
    '''
    stime = time.time()
    mydata1=df2.melt(id_vars = melt_columns,   #要保留的主字段
                    var_name= "init_dt",            #拉长的分类变量
                    value_name="prod_brand"    #拉长的度量值名称
                    )
    
    mydata1 = mydata1.fillna("")
    #先变换日期再变换牌号
    mydata1['init_dt'] = mydata1.apply(lambda x : generate_init_dt(x['prod_brand'],x['prod_brand_change'],x['init_dt']),axis=1)
    mydata1['prod_brand'] = mydata1.apply(lambda x : generate_brand(x['prod_brand'],x['prod_brand_change']),axis=1)
    mydata1[['prod_brand_change', 'prod_brand','prod_standard']] = mydata1[['prod_brand_change', 'prod_brand','prod_standard']].astype(str)

    #将列值替换,list类型变换为tuple，以便去重
    mydata1 = mydata1.replace([r'\[',r'\]'],['(',')'],regex=True) 
    #加工uploadTime
    mydata1['initdata_update_time'] = mydata1['initdata_update_time'].apply(lambda x :x.replace("(","").replace(")",""))
    
    #h1 = mydata1[mydata1['link']=='https://plas.chem99.com/news/37295776.html']

    #去除无效数据
    mydata2 = mydata1[mydata1['prod_brand'].str.len()>0]
    mydata3 = mydata2.drop_duplicates()  
    mydata3['dt'] = mydata1.apply(lambda x : generate_load_dt(x['initdata_update_time'],x['init_dt']),axis=1)  
    mydata4 = mydata3.drop(['init_dt','prod_brand_change'],axis=1)
    mydata4 = mydata4.replace([r'\(',r'\)'],['',''],regex=True) 
    mydata4['prod_brand'] = mydata4['prod_brand'].apply(lambda x : x.replace('\'','')) #字符串替换
    logger.info("加工数据耗时%.2fs"%(time.time()-stime))
    return mydata4


@logger.catch
def datalist_step1_etl(initdata,mapping_dict,chn_pattern):
    '''
    加工dataList列及根据mapping_dict映射列名
    initdata = p1.copy()
    '''
    #一行扩展成多行(dataList拆成多行)
    df = initdata.reindex(initdata.index.repeat(initdata.dataList.str.len())).assign(dataList=np.concatenate(initdata.dataList.values))    
    df['dt'] = df['dataList'].apply(lambda x :list(x.keys())[0])
    df['dt'] = df['dt'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y-%m-%d"),"%Y%m%d")) 
    df['dataList2'] = df['dataList'].apply(lambda x :list(x.values())[0])    
    df2 = pd.concat([df.drop(['dataList2'], axis=1), df['dataList2'].apply(pd.Series)], axis=1)
    #不含中文的列名
    melt_columns = [ s for s in df2.columns if len(re.findall(chn_pattern,s))==0]
    #包含中文的列名      
    chn_columns = [ s for s in df2.columns if len(re.findall(chn_pattern,s))>0]      
    if len(chn_columns)>0:
        mydata1=df2.melt(id_vars = melt_columns,   #要保留的主字段
                        var_name= "prod_name",            #拉长的分类变量
                        value_name="prod_price"    #拉长的度量值名称
                        )
    else:
        mydata1 = df2.copy()

    dcolumns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(mydata1.columns)]    
    mydata1.columns = dcolumns  
    mydata1['initdata_update_time'] = mydata1['initdata_update_time'].apply(lambda x : x.replace('[','').replace(']','')) #字符串替换   
    return mydata1

@logger.catch
def save_stretch_film_price(p1,mapping_dict,chn_pattern,schema_name,tmp_schema_name,table_name,mysql_engine,calendar_data):
    '''
    加工PE缠绕膜的数据
    p1 = stretch_film_price_data.copy()
    '''    
    df2 = datalist_step1_etl(p1,mapping_dict,chn_pattern)
    df2['initdata_update_time'] = df2['initdata_update_time'].apply(lambda x : x[:16])
    df2['prod_standard']="25μ"
    df2['prod_name']="PE缠绕膜"
    df2['prod_unit']="元/吨"
    keep_columns = ['url_link','type_name','dt','initdata_update_time','prod_name','prod_standard','prod_unit','prod_price']
    stretch_file_hashkey_list = ['url_link','type_name','dt','initdata_update_time','prod_name','prod_standard','prod_unit']
    update_columns = keep_columns
    
    df3 = df2[keep_columns]
    mindt = min(df3['dt'])
    maxdt = max(df3['dt'])
    
    df4 = pd.merge(left=calendar_data,right=df3,how='left')
    df4 = df4[(df4['dt']>=mindt)&(df4['dt']<=maxdt)]
    df4 = df4.fillna(method='ffill')
    
    df4['hashkey'] = df4.apply(generate_hashkey,args=(stretch_file_hashkey_list),axis=1)     
    save_data_to_mysql(df4,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns)
   
 
@logger.catch
def save_agricultural_film_price(p1,mapping_dict,chn_pattern,schema_name,tmp_schema_name,table_name,mysql_engine,calendar_data):
    '''
    加工农膜日评(山东省,双防膜+地膜)
    p1 = agricultural_film_data.copy()
    '''    
    p2 = datalist_step1_etl(p1,mapping_dict,chn_pattern)
    p2['prod_low_price'] =p2['prod_price'].apply(lambda x : (x[:x.find("-")]))
    p2['prod_high_price'] =p2['prod_price'].apply(lambda x :(x[x.find("-")+1:]))
    p2['prod_province']='山东'
    p2['prod_unit']='元/吨'
    keep_columns = ['url_link','type_name','dt','prod_name','prod_province','initdata_update_time','prod_low_price','prod_high_price','title']
    stretch_file_hashkey_list = ['url_link','type_name','dt','prod_name','prod_province','initdata_update_time','title']
    update_columns = keep_columns

    p3 = p2[keep_columns]
    mindt = min(p3['dt'])
    maxdt = max(p3['dt'])
    #进行分组补值
    p5 = pd.DataFrame()
    prod_name_list = p3['prod_name'].drop_duplicates().tolist()
    for p in prod_name_list:
        tmp_df = p3[p3['prod_name']==p]
        p4 = pd.merge(left=calendar_data,right=tmp_df,how='left')
        p4 = p4[(p4['dt']>=mindt)&(p4['dt']<=maxdt)]
        p4 = p4.fillna(method='ffill')
        p5 = pd.concat([p5,p4],axis=0)
   
    p5['initdata_update_time'] =p5['initdata_update_time'].apply(lambda x : x[:16])
    p5 = p5[p5['url_link']!='https://plas.chem99.com/news/35435050.html'] #异常数据
    p5['hashkey'] = p5.apply(generate_hashkey,args=(stretch_file_hashkey_list),axis=1)     

    save_data_to_mysql(p5,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns)
    
    
@logger.catch    
def save_pe_factory_percent_data(initdata,mapping_dict,chn_pattern,pe_category_dict,schema_name,tmp_schema_name,table_name,mysql_engine):
    '''   
    国内石化PE生产比例汇总
    数据根据dt,initdata_update_time进行覆盖
    加工dataList列及根据mapping_dict映射列名
    initdata = pe_factory_percent_data.copy()
    table_name = pe_scrapy_factory_percent
    更新20200820号之后的数据，之前的数据数据格式比较凌乱！
    
    '''
    #一行扩展成多行(dataList拆成多行)
    df = initdata.reindex(initdata.index.repeat(initdata.dataList.str.len())).assign(dataList=np.concatenate(initdata.dataList.values))    
    df['uploadTime'] = df['uploadTime'].apply(lambda x : x.replace('[','').replace(']','')[:16]) #字符串替换   
    df['uploadTime'] = df['uploadTime'].apply(lambda x : datetime.strftime(datetime.strptime(x,"%Y-%m-%d %H:%M"),"%Y%m%d"))   
    df = df[df['uploadTime']>'20200820']

    df['dt'] = df['dataList'].apply(lambda x :list(x.values())[0])  
    df = df[df['dt']!='']
    df['dt'] = df['dt'].apply(lambda x : datetime.strftime(datetime.strptime(x,"%Y/%m/%d"),"%Y%m%d"))
  
    df2 = pd.concat([df.drop(['dataList'], axis=1), df['dataList'].apply(pd.Series)], axis=1)
    df2 = df2.drop(['日期'],axis=1)    
    #不含中文的列名
    melt_columns = [ s for s in df2.columns if len(re.findall(chn_pattern,s))==0]
    #包含中文的列名      
    chn_columns = [ s for s in df2.columns if len(re.findall(chn_pattern,s))>0]      
    if len(chn_columns)>0:
        mydata1=df2.melt(id_vars = melt_columns,   #要保留的主字段
                        var_name= "prod_standard",   #拉长的分类变量
                        value_name="prod_percent"    #拉长的度量值名称
                        )

    dcolumns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(mydata1.columns)]    
    mydata1.columns = dcolumns  
    #mydata1['initdata_update_time'] = mydata1['initdata_update_time'].apply(lambda x : x.replace('[','').replace(']','')) #字符串替换   
    keep_columns = ['url_link','type_name','dt','initdata_update_time','prod_standard','prod_percent']
    mydata2 = mydata1[keep_columns]
    
    #分组后取最新的一条
    #h = mydata2[mydata2['prod_standard']=='高压']
    agg_funcs = {'initdata_update_time':'max'}
    mydata3 = mydata2.groupby(['dt','type_name','prod_standard']).agg(agg_funcs).reset_index()
    
    mydata3['parent_category'] = mydata3['prod_standard'].apply(lambda x :pe_category_dict[x])
    mydata3['remark'] ='爬虫'
    mydata4 = pd.merge(left=mydata3,right= mydata2,how='left',on=['dt','type_name','prod_standard','initdata_update_time'])



    pe_factory_percent_list = ['dt','type_name','prod_standard','parent_category']
    mydata4['hashkey'] = mydata4.apply(generate_hashkey,args=(pe_factory_percent_list),axis=1)  
    update_columns = ['dt','type_name','prod_standard','parent_category']
    #增量更新历史数据，并保存到数据库
    save_data_to_mysql(mydata4,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns)
         
@logger.catch
def save_oil_repertory_data(initdata,mapping_dict,schema_name,tmp_schema_name,table_name,mysql_engine):     
    '''
    加工两油库存数据
    initdata = oil_repertory_data.copy()
    '''
    df = initdata.reindex(initdata.index.repeat(initdata.dataList.str.len())).assign(dataList=np.concatenate(initdata.dataList.values))    
    df['dt'] = df['dataList'].apply(lambda x :list(x.keys())[0])
    df['dt'] = df['dt'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y/%m/%d"),"%Y%m%d")) 
    df['oil_repertory'] = df['dataList'].apply(lambda x :list(x.values())[0])  
    df['prod_unit']='万吨'
    dcolumns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(df.columns)]    
    df.columns = dcolumns 
    df['initdata_update_time'] = df['initdata_update_time'].apply(lambda x : x.replace('[','').replace(']','')) #字符串替换   

    keep_columns = ['url_link','initdata_update_time','type_name','dt','oil_repertory','title']
    stretch_file_hashkey_list = ['url_link','type_name','dt']
    update_columns = keep_columns
    df2 = df[keep_columns]
    
    #取最新的一条数据
    agg_funcs = {'initdata_update_time':'max'}
    df3 = df2.groupby(['dt','type_name']).agg(agg_funcs).reset_index()
    
    df4 = pd.merge(left=df3,right= df2,how='left',on=['dt','type_name','initdata_update_time'])   
    df4['hashkey'] = df4.apply(generate_hashkey,args=(stretch_file_hashkey_list),axis=1)     
    df4['create_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df4['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #df4.to_sql("oil_repertory",con=mysql_engine,schema="market_db",if_exists='append',index=False)
    
    save_data_to_mysql(df4,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns)
 
@logger.catch
def save_pe_bidding_data(pe_bidding_data,mapping_dict,prod_name_map_dict,schema_name,tmp_schema_name,pe_bidding_table_name,mysql_engine):
    '''
    保存神华PE竞拍结果
    '''
    p1 = pe_bidding_data.copy()
    df = p1.reindex(p1.index.repeat(p1.dataList.str.len())).assign(dataList=np.concatenate(p1.dataList.values))    
    df2 = pd.concat([df.drop(['dataList'], axis=1), df['dataList'].apply(pd.Series)], axis=1)
    
    df3 = df2[['_id','link','Type','uploadTime','产品类型','产品牌号','成交价','成交率','交货地','挂单量（吨）','成交量（吨）']]
    dcolumns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(df3.columns)]    
    df3.columns = dcolumns  
    df3['initdata_update_time'] = df3['initdata_update_time'].apply(lambda x : x.replace('[','').replace(']','')[:16]) #字符串替换   
    #加工日期数据
    df3['dt'] = df3['initdata_update_time'].apply(lambda x : datetime.strftime(datetime.strptime(x,"%Y-%m-%d %H:%M"),"%Y%m%d"))  
    df4 = df3[df3['prod_name'].str.len()>0]
    #取成交价的最低价
    df4['prod_price'] = df4['transaction_price'].apply(lambda x : x[:x.rfind("-")] if "-" in x else('' if '流拍' in x  else x))
    df4['create_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df4['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  
    df4['prod_name'] = df4['prod_name'].map(prod_name_map_dict) #映射产品名称
    keep_columns = ['url_link','prod_name','type_name','dt','initdata_update_time','prod_brand','transaction_price','delivery_place','turnover_rate','prod_price','pending_order_amount','trading_amount']
    df5 = df4[keep_columns].drop_duplicates() 
    
    pe_shenhua_bidding_list = ['dt','prod_name','type_name','prod_brand','delivery_place','transaction_price','url_link','pending_order_amount','trading_amount']
    df5['hashkey'] = df5.apply(generate_hashkey,args=(pe_shenhua_bidding_list),axis=1)  
    
    #t = df5[df5['hashkey']=='3d3b3413616f0d108cdb7dc085d1aace']
    update_columns = keep_columns
    #增量更新数据
    save_data_to_mysql(df5,mysql_engine,schema_name,tmp_schema_name,pe_bidding_table_name,update_columns)

@logger.catch
def generate_prod_brand(df):
    '''
    根据用途列是否为停车，判断牌号取值
    '''
    if df['prod_standard'] == '停车':
        return ''
    else:
        return df['prod_brand']

@logger.catch
def transfer_prod_standard(prod_standard):
    '''
    加工PE装置用途字段
    '''
    if ('0'== prod_standard) or ('停车'== prod_standard) or ('开车中'== prod_standard) or ('检修'== prod_standard):
        return ""
    else:
        return prod_standard

@logger.catch   
def transfer_prod_brand(prod_brand,prod_brand_pattern,jianxiu_pattern):
    '''
    加工PE装置牌号字段
    1.过滤检修,停车,'0','计划检修\d+天'等字符串
    2.1 包含','则字符串从,开始截取
    2.2 匹配数字和英文字母得到牌号
    h = '0'   ==> ''
    j ='检修' ==> ''
    k ='停车' ==> ''
    a = '周六转产5675，计划今日转产8008' ==> '8008'
    b = '周六已转产55110' ==> '55110'
    c = '周六开产5420'   ==> '5420'
    d = '10月16日停车检修,计划检修67天' ==> ''
    e = '转产J182A' ==>  J182A
    m = '2月21日-3月3日停车'
    h = '先转LD607后转LD9202W' ==>LD9202W
    k ='2810H转2426H'
    prod_brand_pattern= r"[A-Za-z0-9-]" #PE牌号匹配
    a2 = '12日起停车检修'

    r"[A-Za-z0-9]"
    prod_brand = a2
    t = transfer_prod_brand(m,prod_brand_pattern,jianxiu_pattern)
    logger.info(t)
    '''
    prod_brand = prod_brand.replace("，",",")
    if '转' in prod_brand:
        prod_brand = prod_brand[prod_brand.find('转'):]    
    comma_pos = prod_brand
    if ('0'== prod_brand) or ('日起停车检修' in prod_brand) or ('起大修，计划检修至' in prod_brand)or ('检修'== prod_brand) or ('停车'== prod_brand) or ('日停车' in prod_brand) or len(re.findall(jianxiu_pattern,prod_brand))>0:
        return ''
    
    else:
        if (',' in prod_brand):
            comma_pos = prod_brand[prod_brand.find(',')+1:]
        elif '产' in prod_brand:
            comma_pos = prod_brand[prod_brand.find('产'):]
        if '后转' in prod_brand:
            comma_pos = prod_brand[prod_brand.find('后转'):]
        if '今日转' in prod_brand:
            comma_pos = prod_brand[prod_brand.find('今日转'):]   
        
        prod_brand_str = "".join(re.findall(prod_brand_pattern,comma_pos))
        return prod_brand_str
        
#t = transfer_prod_brand(k,prod_brand_pattern,jianxiu_pattern)
@logger.catch  
def get_pe_service_data(tmp_pe_device_dynamic_data,mapping_dict,chn_pattern,prod_brand_pattern,jianxiu_pattern):
    '''
    根据爬虫原始数据pp_device_dynamic_data加工得到装置动态产能数据
    pe_device_dynamic_data = tmp_pe_device_dynamic_data.copy()
    '''
    df = tmp_pe_device_dynamic_data.reindex(tmp_pe_device_dynamic_data.index.repeat(tmp_pe_device_dynamic_data.dataList.str.len())).assign(dataList=np.concatenate(tmp_pe_device_dynamic_data.dataList.values))    
    df2 = pd.concat([df.drop(['dataList'], axis=1), df['dataList'].apply(pd.Series)], axis=1)    
    dcolumns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(df2.columns)]
    df2.columns = dcolumns
    #列名中文替换
    melt_columns = [ s for s in dcolumns if len(re.findall(chn_pattern,s))==0]      
    #包含中文的列名      
    chn_columns = [ s for s in df2.columns if len(re.findall(chn_pattern,s))>0]   
    if len(chn_columns)>0:
        df3=df2.melt(id_vars = melt_columns,   #要保留的主字段
                        var_name= "init_dt",            #拉长的分类变量
                        value_name="prod_brand"    #拉长的度量值名称
                        ).fillna("")
    else:
        df3= df2.copy()        
        df3 = df3.rename(columns={"prod_brand_change":"prod_brand"})
    
    if 'annual_prod_capacity' not in df3.columns:
        df3['annual_prod_capacity'] = ''
    if 'prod_standard' not in df3.columns:
        df3['prod_standard'] = ''
    #列类型变更为字符串，去除替换无效字符
    replace_columns = ['prod_factory','prod_area','annual_prod_capacity','prod_standard','prod_brand']
    df3[replace_columns] = df3[replace_columns].astype(str)
    for c in replace_columns:
        df3[c]  = df3[c].apply(lambda x : (x.replace('[','').replace(']','').replace('\'','')))      
    #用途列若包含'停车'或者'0'的统一变更为停车
    df3['prod_standard'] = df3['prod_standard'].apply(transfer_prod_standard)   
    #加工牌号字段
    df3['prod_brand'] = df3['prod_brand'].apply(lambda x :transfer_prod_brand(x,prod_brand_pattern,jianxiu_pattern))

    #判断是否有日期列,刑如1月29日的，需要补上2021年,init_dt=日期，initdata_update_time更新时间
    if 'init_dt' in list(df3.columns):
        df3['init_dt'] = df3['init_dt'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y年%m月%d日"),"%Y-%m-%d") if '年' in x else datetime.strftime(datetime.strptime('2021年'+x,"%Y年%m月%d日"),"%Y-%m-%d"))
              
    else:
        df3['init_dt']= df3['initdata_update_time']    

    df3['dt']=df3['init_dt'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y-%m-%d"),"%Y%m%d"))

    df3 = df3[['url_link', 'type_name', 'title', 'initdata_update_time', 'prod_area', 'prod_factory', 'annual_prod_capacity', 'prod_name', 'prod_brand','prod_standard', 'init_dt', 'dt']]    
    
    return df3

@logger.catch
def get_prod_name(prod_name_x,prod_name_y):
    '''
    根据牌号或者用途关联产品名称
    '''
    if len(str(prod_name_x))>0:
        return prod_name_x
    else:
        return prod_name_y

@logger.catch            
def save_pe_device_dynamic_data(pe_device_dynamic_sql,pe_device_dynamic_data,device_prod_brand_mapping_sql,pe_device_factory_mapping_dict,mapping_dict,chn_pattern,prod_brand_pattern,jianxiu_pattern,mysql_engine,schema_name,tmp_schema_name,device_table_name):
    '''
    保存PE装置生产数据
    '''
    device_prod_brand_mapping_data = pd.read_sql(device_prod_brand_mapping_sql,con = mysql_engine)
  
    device_dynamic_data = pd.read_sql(pe_device_dynamic_sql,con = mysql_engine)

    #牌号产品名称映射
    prod_brand_name = device_prod_brand_mapping_data[['prod_brand','prod_name']].drop_duplicates()
    prod_brand_name = prod_brand_name[prod_brand_name['prod_brand'].str.len()>0]
    
    #用途产品名称映射
    prod_standard_name = device_prod_brand_mapping_data[['prod_standard','prod_name']].drop_duplicates()
     
    pe_device_dynamic_data = pe_device_dynamic_data.rename(columns={"uploadTime":"initdata_update_time"})
    pe_device_dynamic_data['initdata_update_time'] = pe_device_dynamic_data['initdata_update_time'].apply(lambda x :x.replace("[","").replace("]","")[:16])
    #a = pe_device_dynamic_data.iloc[-1]['initdata_update_time'][:16] 
    pe_device_dynamic_data['initdata_update_time'] = pe_device_dynamic_data['initdata_update_time'].apply(lambda x : datetime.strftime(datetime.strptime(x,"%Y-%m-%d %H:%S"),"%Y-%m-%d"))
    pubtimelist = pe_device_dynamic_data['initdata_update_time'].drop_duplicates().sort_values().tolist()  
    #pubtimelist = pe_device_dynamic_data[pe_device_dynamic_data['initdata_update_time']>='2020-12-20']['initdata_update_time'].drop_duplicates().sort_values().tolist()
    pe_device_dynamic_all_data = pd.DataFrame()
    
    #不同类型的格式分开存储
    for ptime in pubtimelist:
        logger.info(ptime)
        #ptime ='2020-04-24'
        tmp_pe_device_dynamic_data = pe_device_dynamic_data[pe_device_dynamic_data['initdata_update_time']==ptime]
        tmp_pdata = get_pe_service_data(tmp_pe_device_dynamic_data,mapping_dict,chn_pattern,prod_brand_pattern,jianxiu_pattern)
        pe_device_dynamic_all_data = pd.concat([pe_device_dynamic_all_data,tmp_pdata],axis=0)
    
    pe_device_dynamic_all_data['prod_factory'] = pe_device_dynamic_all_data['prod_factory'].apply(lambda x : pe_device_factory_mapping_dict[x] if x in list(pe_device_factory_mapping_dict.keys()) else x)     
    #分组后取最新的一条    
    #p1 = pe_device_dynamic_all_data[pe_device_dynamic_all_data['prod_factory']=='茂名石化']
    agg_funcs = {'initdata_update_time':'max'}
    pe_device_dynamic_all_data2 = pe_device_dynamic_all_data.groupby(['dt','prod_factory']).agg(agg_funcs).reset_index() 
    pe_device_dynamic_all_data3 = pd.merge(left=pe_device_dynamic_all_data2,right= pe_device_dynamic_all_data,how='left',on=['dt','prod_factory','initdata_update_time'])
      
    p1 = pe_device_dynamic_all_data3[['url_link','dt','initdata_update_time','prod_factory','prod_name','prod_area','prod_standard','prod_brand','annual_prod_capacity']]    
    p1 = p1.rename(columns={"prod_name":"prod_line_name"})
    p1['annual_prod_capacity'] = p1['annual_prod_capacity'].apply(lambda x : int(-1) if len(str(x))==0 else x) 
    p1['daily_prod_capacity'] = p1['annual_prod_capacity'].apply(lambda x: float(x)/330 if float(x)>0 else float(x))
    
    p1['rank_id'] = p1.index    
    p1['rank']= p1.groupby(['dt','prod_factory'])['rank_id'].rank(method='first')
    p1['prod_line_id']= p1['rank'].apply(lambda x :'line_'+str(int(x)))
    p1['po_type'] ='PE'
    p1['prod_unit']='万吨/年'
    p1['prod_remark']='爬虫'
    p1 = p1.drop(['rank','rank_id'],axis=1)
    
    p2 = pd.merge(left = p1,right=prod_brand_name,how='left',on='prod_brand')
    p3 = pd.merge(left = p2,right=prod_standard_name,how='left',on='prod_standard')
    p3 = p3.fillna("")
    p3['prod_name'] = p3.apply(lambda x :get_prod_name(x['prod_name_x'],x['prod_name_y']),axis=1)
    p3 = p3.drop(['prod_name_x','prod_name_y'],axis=1)

    #关联得到准确的产能
    p3 = p3.drop(['annual_prod_capacity','daily_prod_capacity'],axis=1)
    p4 = pd.merge(left=p3,right=device_dynamic_data,how='inner',on=['prod_factory','prod_line_id','po_type'])   
    p5 = get_ffill_device_data(p4)       
    dynamic_column_hashkey_list =['dt','prod_line_id','prod_factory','po_type','prod_remark']
    update_device_dynamic_columns = list(p5.columns)      
    p5['hashkey'] = p5.apply(generate_hashkey,args=(dynamic_column_hashkey_list),axis=1)  
    save_data_to_mysql(p5,mysql_engine,schema_name,tmp_schema_name,device_table_name,update_device_dynamic_columns)

@logger.catch
def get_pe_maintenance_dt(maintenance_dt,maintenance_pattern):
    '''
    根据装置停车计划开车、停车时间加工字段
    maintenance_pattern=r"\d{1,2}年\d{1,2}月\d{1,2}日" #装置检修计划匹配

    '''    
    #maintenance_dt = '14年6月12日'
    #maintenance_dt ='2021/5/11'
    maintenance_str = re.findall(maintenance_pattern,maintenance_dt)
    if len(maintenance_str)>0:
        maintenance_str2 = maintenance_str[0]
        if maintenance_str2.find("年")>2:
            maintenance_str3 = datetime.strftime(datetime.strptime(maintenance_str2,"%Y年%m月%d日"),"%Y%m%d")        
        else:
            maintenance_str3 = datetime.strftime(datetime.strptime(maintenance_str2,"%y年%m月%d日"),"%Y%m%d")            
        return maintenance_str3
    if '/' in maintenance_dt:
        maintenance_str3 = datetime.strftime(datetime.strptime(maintenance_dt,"%Y/%m/%d"),"%Y%m%d")           
        return maintenance_str3
    else:
        return maintenance_dt

@logger.catch
def save_pe_device_dynamic_future_data(pe_device_dynamic_future_data2,mapping_dict,maintenance_pe_pattern,chn_pattern,mysql_engine,schema_name,tmp_schema_name,pe_device_dynamic_future_table_name):
    pe_device_dynamic_future_data2['pubTime'] = pe_device_dynamic_future_data2['uploadTime'].apply(lambda x : datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
    p1 = pe_device_dynamic_future_data2.reindex(pe_device_dynamic_future_data2.index.repeat(pe_device_dynamic_future_data2.dataList.str.len())).assign(dataList=np.concatenate(pe_device_dynamic_future_data2.dataList.values))    
    p2 = pd.concat([p1.drop(['dataList'], axis=1), p1['dataList'].apply(pd.Series)], axis=1)
    p2.columns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(p2.columns)]
    p2.index = range(len(p2))  
    p2['dt'] = p2['initdata_update_time'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y-%m-%d %H:%M:%S"),"%Y%m%d"))    
    #判断日期是否为将来时(取最大的日期为将来时，其余为0)
    p2['dt_future_type'] = p2['dt'].apply(lambda x: 1 if x==max(p2['dt']) else 0)
    p2['annual_prod_capacity'] = p2['annual_prod_capacity'].apply(lambda x: 0 if '-' in x else x)
    p2['annual_prod_capacity'] =p2['annual_prod_capacity'].astype(float)
    p2['maintenance_start_dt'] = p2.apply(lambda x : get_pe_maintenance_dt(x['maintenance_start_dt'],maintenance_pe_pattern),axis=1)
    p2['maintenance_end_dt'] = p2.apply(lambda x : get_pe_maintenance_dt(x['maintenance_end_dt'],maintenance_pe_pattern),axis=1)
       
    p3 = p2.drop(['_id','year','type_name','hashKey','day','dayQuotes','month','tableData','year','pubTime'],axis=1)
    p3['prod_unit'] ='万吨/年'
    p3['po_type']='PE'
    p3 = p3[p3['prod_factory']!='总计']
    
    #p4 = p3[p3['dt_future_type']==1]  
    p3['loss_capacity'] = p3.apply(lambda x : get_month_loss_capacity(x['maintenance_start_dt'],x['maintenance_end_dt'],x['annual_prod_capacity'],chn_pattern),axis=1)   
    p7 = p3.dropna()
    p8 = p7.reindex(p7.index.repeat(p7.loss_capacity.str.len())).assign(loss_capacity=np.concatenate(p7.loss_capacity.values))    
    p9= pd.concat([p8.drop(['loss_capacity'], axis=1), p8['loss_capacity'].apply(pd.Series)], axis=1)
   
    update_columns = p9.columns
    hashkey_column_list = ['dt','loss_month','prod_factory','po_type','prod_line_name','maintenance_start_dt']
    #生产唯一键
    p9['hashkey'] = p9.apply(generate_hashkey,args=(hashkey_column_list),axis=1)   
    save_data_to_mysql(p9,mysql_engine,schema_name,tmp_schema_name,pe_device_dynamic_future_table_name,update_columns)


@logger.catch
def scrapy_pe_etl():
#if __name__=='__main__':
    chn_pattern= r"[\u4e00-\u9fa5]" #中文匹配
    mysql_con = "mysql+pymysql://root:dZUbPu8eY$yBEoYK@27.150.182.135/"
    mysql_engine = create_engine(mysql_con,encoding='utf-8', echo=False,pool_timeout=3600)
    #工作日数据
    calendar_data = pd.read_sql("select cd.dt from market_db.calendar_date cd where cd.is_workday = 1",con=mysql_engine)
    chn_pattern= r"[\u4e00-\u9fa5]" #中文匹配
    prod_brand_pattern= r"[A-Za-z0-9-]" #PE牌号匹配
    maintenance_pe_pattern =r"\d{1,2}年\d{1,2}月\d{1,2}日" #装置检修计划匹配
    jianxiu_pattern =r",计划检修\d+天"

    myclient = MongoClient("mongodb://root:root123456@27.150.182.135:27017/")
    mycol = myclient["Quotation"]["pe_wangye_articleData"]
    #myclient["Quotation"].list_collection_names()
    pdata = get_pdata(mycol)
    schema_name = "market_db"
    tmp_schema_name = "tmp_market_db"

    #pdata['Type'].value_counts()
    mapping_dict = {"地区":"prod_area","厂家名称":"prod_factory","产能":"annual_prod_capacity",
                    "品种":"prod_name","用途":"prod_standard","装置动态":"prod_brand_change",
                    "25μ":"prod_price","link":"url_link","Type":"type_name","uploadTime":"initdata_update_time",
                    "白膜":"white_film","双防膜":"double_membrane_film","地膜":"mulch_film","西瓜膜":"watermelon_film",                    
                    "产销国":"stat_country","贸易方式":"trade_mode","数量":"imports_kilogram",
                    "产品类型":"prod_name","产品牌号":"prod_brand","成交价":"transaction_price",
                    "成交率":"turnover_rate","交货地":"delivery_place","挂单量（吨）":"pending_order_amount","成交量（吨）":"trading_amount",
                    "生产企业":"prod_factory","开始日期":"maintenance_start_dt","结束日期":"maintenance_end_dt",
                    "总停工天数":"maintenance_days","产量损失预计":"yield_loss_capacity","备注":"maintenance_remark",
                    "装置":"prod_line_name","装置能力":"annual_prod_capacity" }
   
    pe_category_dict =  {"高压":"高压","涂覆":"高压","80级管材":"低压管材","100级管材":"低压管材",
                         "地暖级管材":"低压管材","大中空":"低压中空","小中空":"低压中空",
                         "低熔注塑":"低压注塑","高熔注塑":"低压注塑","低压薄膜":"低压薄膜",
                         "低压拉丝":"低压拉丝","线性":"线性","其他":"其他","检修":"检修"}
    
    
 
    #1.国内PE装置动态汇总 
    device_table_name = "domestic_device_dynamics_data"  
    pe_device_dynamic_data = pdata[pdata['Type']=='国内PE装置动态汇总'][-30:]
    #pe_device_dynamic_data = pdata[pdata['Type']=='国内PE装置动态汇总']
    pe_device_factory_mapping_dict ={"扬石化":"扬子石化","华东某企业":"上海赛科","延长中煤二期":"延长中煤","宝丰二期":"宁夏宝丰",
                                     "华南某合资企业一期":"中海壳牌","华南某合资企业二期":"中海壳牌",
                                     "中韩石化":"武汉乙烯"}
    
    device_prod_brand_mapping_sql = "select prod_brand,prod_standard,prod_name from market_db.device_prod_brand_mapping  where po_type = 'PE' "
    
    pe_device_dynamic_sql = ''' select ddi.prod_factory ,ddi.prod_line_id ,ddi.po_type ,ddi.annual_prod_capacity ,ddi.daily_prod_capacity 
                             from market_db.device_dynamic_info ddi where ddi.po_type = 'PE' '''
    
    save_pe_device_dynamic_data(pe_device_dynamic_sql,pe_device_dynamic_data,device_prod_brand_mapping_sql,pe_device_factory_mapping_dict,mapping_dict,chn_pattern,prod_brand_pattern,jianxiu_pattern,mysql_engine,schema_name,tmp_schema_name,device_table_name)    
    
        
    #3.塑膜收盘价格表(PE缠绕膜)
    stretch_film_price_table_name = "stretch_film_price"    
    stretch_film_price_data = pdata[pdata['Type'].isin(['塑膜收盘价格表'])]
    save_stretch_film_price(stretch_film_price_data,mapping_dict,chn_pattern,schema_name,tmp_schema_name,stretch_film_price_table_name,mysql_engine,calendar_data)
   
      
    #5.农膜日评(双防膜+地膜)
    agricultural_film_table_name = "agricultural_film_price"    
    agricultural_film_data = pdata[pdata['Type'].isin(['农膜日评'])] 
    save_agricultural_film_price(agricultural_film_data,mapping_dict,chn_pattern,schema_name,tmp_schema_name,agricultural_film_table_name,mysql_engine,calendar_data)
    
        
    #7.国内石化PE生产比例汇总，只加载到20200804 
    pe_factory_percent_table_name = "pe_scrapy_factory_percent"    
    pe_factory_percent_data = pdata[pdata['Type'].isin(['国内石化PE生产比例汇总'])]
    save_pe_factory_percent_data(pe_factory_percent_data,mapping_dict,chn_pattern,pe_category_dict,schema_name,tmp_schema_name,pe_factory_percent_table_name,mysql_engine)
    

    # #12.神华PE竞拍
    pe_bidding_table_name = "pe_shenhua_bidding"
    pe_bidding_data = pdata[pdata['Type'].isin(['神华PE竞拍'])]
    prod_name_map_dict = {"线性":"LLDPE","低压":"HDPE","高压":"LDPE"}
    save_pe_bidding_data(pe_bidding_data,mapping_dict,prod_name_map_dict,schema_name,tmp_schema_name,pe_bidding_table_name,mysql_engine)
    
    
    
    
    # #PE国内企业装置检修计划
    # pe_device_dynamic_future_data = pdata[pdata['Type']=='PE国内企业装置检修']
    # pe_device_dynamic_future_data2 = pe_device_dynamic_future_data[pe_device_dynamic_future_data['title'].str.contains('装置检修计划统计')]  
    # pe_device_dynamic_future_data2 = pe_device_dynamic_future_data2.sort_values(['uploadTime'])[-10:]
     
    # pe_device_dynamic_future_table_name = 'domestic_device_dynamics_future_data'
    # save_pe_device_dynamic_future_data(pe_device_dynamic_future_data2,mapping_dict,maintenance_pe_pattern,chn_pattern,mysql_engine,schema_name,tmp_schema_name,pe_device_dynamic_future_table_name)
  