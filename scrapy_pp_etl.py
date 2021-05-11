# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 15:34:00 2021

@author: huangshizhi

PP数据采集

"""

import numpy as np
import time
import pandas as pd
import re
from pymongo import MongoClient
from sqlalchemy import create_engine
from datetime import datetime
from scrapy_util import generate_hashkey,get_ffill_device_data,save_data_to_mysql,get_month_loss_capacity
from loguru import logger

@logger.catch
def transfer_prod_brand_str(prod_brand,prod_brand_pattern,jianxiu_pattern):
    '''
    返回生产线名称,先替换标点符号("，",",")
    若包含产字，从产字后面截取包含英文大写数字-的字符串;
    若为0,则返回空;
    
    p1 = "三号装置产K7726H,计划3月23日停车检修,4月29日重启。" ==> K7726H
    p2 = "一号装置产PPB-MT25-S" ==> PPB-MT25-S
    p3 ="装置产Y35" ==> Y35
    p4 ="MM60" ==> MM60
    p5 = "3月1日期停车检修，计划50-60天" ==> ''
    p6 = "二期因造粒机故障，产粉料"	 ==> 粉料 
    p7= 'PP3线产EP200K-B'
    p8 ='STPP线产FCP80	'
    p9= '二线产PPH-Y25L厂家PP装置计划于3月20日进行全厂检修，检修时间为15-20天'
    p10= '二期产L5E89粉料'
    prod_brand = p10
    prod_line_list = [prod_line1,prod_line2,prod_line3,prod_line4,prod_line5,prod_line6]
    jianxiu_pattern =r"停车检修,计划.{1,}天"
    re.findall(jianxiu_pattern,prod_brand)

    '''
    prod_brand = prod_brand.replace("，",",").replace("厂家PP装置计划",",厂家PP装置计划")
    if '转' in prod_brand:
        prod_brand = prod_brand[prod_brand.find('转'):]    
    comma_pos = prod_brand
    if ('0'== prod_brand) or ('检修'== prod_brand) or ('停车'== prod_brand) or ('日停车' in prod_brand) or len(re.findall(jianxiu_pattern,prod_brand))>0:
        return ''
    
    else:
        if ('产' in prod_brand) and (',' in prod_brand):
            comma_pos = prod_brand[prod_brand.find('产'):prod_brand.find(',')]
        if ('产' in prod_brand) and (',' not in prod_brand):
            comma_pos = prod_brand[prod_brand.find('产'):]
        if '后转' in prod_brand:
            comma_pos = prod_brand[prod_brand.find('后转'):]
        if '今日转' in prod_brand:
            comma_pos = prod_brand[prod_brand.find('今日转'):]   
        
        prod_brand_str = "".join(re.findall(prod_brand_pattern,comma_pos))
        if '粉料' in prod_brand_str:
            prod_brand_str = '粉料'
        #logger.info(prod_brand,prod_brand_str)
        return prod_brand_str
    
    


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
def save_pp_factory_percent_data(initdata,mapping_dict,pp_factory_percent_dict,schema_name,tmp_schema_name,table_name,mysql_engine):
    '''   
    国内石化PP生产比例汇总
    数据根据dt,initdata_update_time进行覆盖
    加工dataList列及根据mapping_dict映射列名
    initdata = pp_factory_percent_data.copy()
    table_name = pe_scrapy_factory_percent
    更新20200820号之后的数据，之前的数据数据格式比较凌乱！
    
    '''
    #一行扩展成多行(dataList拆成多行)
    df = initdata.reindex(initdata.index.repeat(initdata.dataList.str.len())).assign(dataList=np.concatenate(initdata.dataList.values))    
    df['pubTime'] = df['pubTime'].apply(lambda x : datetime.strftime(datetime.strptime(x,"%Y-%m-%d"),"%Y%m%d"))   
    df = df[df['pubTime']>'20200820']
    
    df['dt'] = df['dataList'].apply(lambda x :list(x.values())[0])  
    df2 = pd.concat([df.drop(['dataList'], axis=1), df['dataList'].apply(pd.Series)], axis=1)
    
    #均聚注塑,共聚注塑,纤维,管材需要进一步拆分
    df21 = pd.concat([df2.drop(['均聚注塑'], axis=1), df2['均聚注塑'].apply(pd.Series)], axis=1)
    df22 = pd.concat([df21.drop(['共聚注塑'], axis=1), df21['共聚注塑'].apply(pd.Series)], axis=1)
    df23 = pd.concat([df22.drop(['纤维'], axis=1), df22['纤维'].apply(pd.Series)], axis=1)
    df3 = pd.concat([df23.drop(['管材'], axis=1), df23['管材'].apply(pd.Series)], axis=1)
    df3 = df3.drop(['日期'],axis=1)
    melt_columns = ["_id","hashKey","Type","link","pubTime","dt"]
    mydata1=df3.melt(id_vars = melt_columns,   #要保留的主字段
                        var_name= "prod_standard",   #拉长的分类变量
                        value_name="prod_percent"    #拉长的度量值名称
                        )

    #hh = mydata1['prod_standard'].value_counts()


    mydata1['dt'] = mydata1['dt'].apply(lambda x : datetime.strftime(datetime.strptime(x,"%Y/%m/%d"),"%Y%m%d"))   
    dcolumns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(mydata1.columns)]    
    mydata1.columns = dcolumns
    keep_columns = ['url_link','type_name','dt','initdata_update_time','prod_standard','prod_percent']
    mydata2 = mydata1[keep_columns]
    #分组后取最新的一条
    agg_funcs = {'initdata_update_time':'max'}
    mydata3 = mydata2.groupby(['dt','type_name','prod_standard']).agg(agg_funcs).reset_index()
    
    mydata3['parent_category'] = mydata3['prod_standard'].apply(lambda x :pp_factory_percent_dict[x])
    mydata3['remark'] ='爬虫'

    mydata4 = pd.merge(left=mydata3,right= mydata2,how='left',on=['dt','type_name','prod_standard','initdata_update_time'])

    pp_factory_percent_list = ['dt','type_name','parent_category','prod_standard']
    mydata4['hashkey'] = mydata4.apply(generate_hashkey,args=(pp_factory_percent_list),axis=1)
    
    mydata5  = mydata4.drop_duplicates(subset=['hashkey'],keep='first')
      
    update_columns = ['dt','type_name','prod_standard','parent_category','remark']
    
    
    #h5 = mydata4[mydata4['hashkey']=='a6ae4f95145cc05622ada708b128ecd6']
    
    #增量更新历史数据，并保存到数据库
    save_data_to_mysql(mydata5,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns)


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
def get_pp_service_data(tmp_pp_device_dynamic_data,mapping_dict,chn_pattern,prod_brand_pattern,jianxiu_pattern):
    '''
    根据爬虫原始数据pp_device_dynamic_data加工得到装置动态产能数据
    pp_device_dynamic_data = tmp_pp_device_dynamic_data.copy()
    '''
    df = tmp_pp_device_dynamic_data.reindex(tmp_pp_device_dynamic_data.index.repeat(tmp_pp_device_dynamic_data.dataList.str.len())).assign(dataList=np.concatenate(tmp_pp_device_dynamic_data.dataList.values))    
    df2 = df[df['link']!='http://plas.chem99.com/news/35756762.html'] #脏数据
    df2 = pd.concat([df2.drop(['dataList'], axis=1), df2['dataList'].apply(pd.Series)], axis=1)    
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
    replace_columns = ['prod_factory','sinopec_prod_factory','annual_prod_capacity','prod_standard','prod_brand']
    df3[replace_columns] = df3[replace_columns].astype(str)
    for c in replace_columns:
        df3[c]  = df3[c].apply(lambda x : (x.replace('[','').replace(']','').replace('\'','')))      
    #用途列若包含“停车”的统一变更为停车
    df3['prod_standard'] = df3['prod_standard'].apply(lambda x : "停车" if '停车' in x else x)
    df3['prod_brand'] = df3.apply(generate_prod_brand,axis=1)   
    #得到生产线ID
    df3['prod_brand'] = df3['prod_brand'].apply(lambda x :transfer_prod_brand_str(x,prod_brand_pattern,jianxiu_pattern))

    #判断是否有日期列,刑如1月29日的，需要补上2021年,init_dt=日期，initdata_update_time更新时间
    if 'init_dt' in list(df3.columns):
        df3['init_dt'] = df3['init_dt'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y年%m月%d日"),"%Y-%m-%d") if '年' in x else datetime.strftime(datetime.strptime('2021年'+x,"%Y年%m月%d日"),"%Y-%m-%d"))
              
    else:
        df3['init_dt']= df3['initdata_update_time']    
    #过滤init_dt和pubTime一致的日期
    #df4 = df3[df3['init_dt']==df3['initdata_update_time']]
    df3['dt']=df3['init_dt'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y-%m-%d"),"%Y%m%d"))    
    return df3
          
@logger.catch
def save_pp_device_data(pp_device_dynamic_sql,pp_device_dynamic_data,pp_device_factory_mapping_dict,mapping_dict,chn_pattern,prod_brand_pattern,jianxiu_pattern,mysql_engine,schema_name,tmp_schema_name,device_table_name):
    '''
    增量更新PP装置动态汇总
    不同日期，不同工厂,根据更新时间取最新的一条
    产线ID与网页展示的顺序对应
    '''
    pp_device_dynamic_data = pp_device_dynamic_data[pp_device_dynamic_data['link']!='https://plas.chem99.com/news/38202559.html']
    device_dynamic_data = pd.read_sql(pp_device_dynamic_sql,con = mysql_engine)
    
    #只取2020-12-20之后的数据
    pubtimelist = pp_device_dynamic_data[pp_device_dynamic_data['pubTime']>'2021-03-08']['pubTime'].drop_duplicates().sort_values().tolist()[-30:]
    #pubtimelist = ['2021-03-17','2021-03-18']
    pp_device_dynamic_all_data = pd.DataFrame()
    #不同类型的格式分开存储
    for ptime in pubtimelist:
        logger.info(ptime)
        #ptime ='2021-03-02'
        tmp_pp_device_dynamic_data = pp_device_dynamic_data[pp_device_dynamic_data['pubTime']==ptime]
        tmp_pdata = get_pp_service_data(tmp_pp_device_dynamic_data,mapping_dict,chn_pattern,prod_brand_pattern,jianxiu_pattern)
        pp_device_dynamic_all_data = pd.concat([pp_device_dynamic_all_data,tmp_pdata],axis=0)
    
    pp_device_dynamic_all_data['prod_factory'] = pp_device_dynamic_all_data['prod_factory'].apply(lambda x : pp_device_factory_mapping_dict[x] if x in list(pp_device_factory_mapping_dict.keys()) else x)
       
    #不同日期，不同工厂,根据更新时间取最新的一条   
    agg_funcs = {'initdata_update_time':'max'}
    #pp_device_dynamic_all_data2 = pp_device_dynamic_all_data.groupby(['dt','type_name','annual_prod_capacity','prod_factory']).agg(agg_funcs).reset_index()
    pp_device_dynamic_all_data2 = pp_device_dynamic_all_data.groupby(['dt','prod_factory']).agg(agg_funcs).reset_index() 
    pp_device_dynamic_all_data3 = pd.merge(left=pp_device_dynamic_all_data2,right= pp_device_dynamic_all_data,how='left',on=['dt','prod_factory','initdata_update_time'])
    
    p1 = pp_device_dynamic_all_data3[['url_link','dt','initdata_update_time','prod_factory','sinopec_prod_factory','prod_standard','prod_brand','annual_prod_capacity']]    
    p1['annual_prod_capacity'] = p1['annual_prod_capacity'].apply(lambda x : int(-1) if len(str(x))==0 else x) 
    p1['daily_prod_capacity'] = p1['annual_prod_capacity'].apply(lambda x: float(x)/330 if float(x)>0 else float(x))
    p1['rank_id'] = p1.index  
    #p11 = p1[p1['prod_factory']=='扬子石化']   
    #p11['rank']= p11.groupby(['dt','prod_factory'])['id'].rank(method='first')     
    #分组进行排序,增加产线字段(产线ID与网页排序一致)
    p1['rank']= p1.groupby(['dt','prod_factory'])['rank_id'].rank(method='first')
    p1['prod_line_id']= p1['rank'].apply(lambda x :'line_'+str(int(x)))
    
    p11 = p1.drop(['annual_prod_capacity','daily_prod_capacity'],axis=1)
    p2 = pd.merge(left=p11,right=device_dynamic_data,how='left',on=['prod_factory','prod_line_id'])
    
    p2['po_type'] ='PP'
    p2['prod_unit']='万吨/年'
    p2['prod_remark']='爬虫'
    p2['prod_name'] ='PP'
    p2 = p2.drop(['rank','rank_id'],axis=1)
    
    p5 = get_ffill_device_data(p2)  
    dynamic_column_hashkey_list =['dt','prod_line_id','prod_factory','po_type','prod_remark']
    update_device_dynamic_columns = list(p5.columns)
    p5['hashkey'] = p5.apply(generate_hashkey,args=(dynamic_column_hashkey_list),axis=1)    
    
    #p3 = p2[p2['dt']=='20210430'] 
    #p1.to_sql("domestic_pp_device_dynamics",index=False,con= mysql_engine,schema="market_db",if_exists='append')         
    save_data_to_mysql(p5,mysql_engine,schema_name,tmp_schema_name,device_table_name,update_device_dynamic_columns)
  
@logger.catch
def datalist_step2_etl(initdata,mapping_dict):
    '''
    加工dataList列及根据mapping_dict映射列名，修改部分字段名称
    initdata = filmdata.copy()
    '''
    #一行扩展成多行(dataList拆成多行)
    df = initdata.reindex(initdata.index.repeat(initdata.dataList.str.len())).assign(dataList=np.concatenate(initdata.dataList.values))       
    df2 = pd.concat([df.drop(['dataList'], axis=1), df['dataList'].apply(pd.Series)], axis=1)

    #字段中文名映射
    dcolumns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(df2.columns)]    
    df2.columns = dcolumns  
    df2['dt'] = df2['dt'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y-%m-%d"),"%Y%m%d")) 
    df2['prod_unit']="元/吨"
    df2['initdata_update_time'] = df2['initdata_update_time'].apply(lambda x : x.replace('[','').replace(']','')) #字符串替换   
    return df2

@logger.catch
def save_plastic_film_price_data(filmdata,mapping_dict,schema_name,tmp_schema_name,table_name,mysql_engine):
    '''
    加工PE缠绕膜的数据
    filmdata = plastic_film_price_data.copy()
    '''    
    filmdata2 = datalist_step2_etl(filmdata,mapping_dict)
    filmdata2['prod_name'] = filmdata2['prod_name'].apply(lambda x : 'BOPP光膜' if x =='BOPP厚光膜' else x)

    keep_columns = ['url_link','type_name','dt','initdata_update_time','prod_name','prod_thicknesses','prod_unit','prod_price']    
    filmdata3 = filmdata2[keep_columns] 
    hashkey_column_list = ['dt','prod_name','prod_thicknesses','prod_unit']
    #生产唯一键
    filmdata3['hashkey'] = filmdata3.apply(generate_hashkey,args=(hashkey_column_list),axis=1)   
    
    filmdata4 = filmdata3[filmdata3['url_link']!='https://plas.chem99.com/news/37938002.html']
    
    update_columns = keep_columns
    save_data_to_mysql(filmdata4,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns)

@logger.catch
def save_pp_upstram_data(pp_price_data,mapping_dict,chn_pattern,schema_name,tmp_schema_name,table_name,mysql_engine):
    '''
    保存聚丙烯粉料及上游丙烯价格
    '''
    df = pp_price_data.reindex(pp_price_data.index.repeat(pp_price_data.dataList.str.len())).assign(dataList=np.concatenate(pp_price_data.dataList.values))       
    df2 = pd.concat([df.drop(['dataList'], axis=1), df['dataList'].apply(pd.Series)], axis=1)
    df2.columns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(df2.columns)]

    #包含中文的列名      
    chn_columns = [ s for s in df2.columns if len(re.findall(chn_pattern,s))>0]   
    melt_columns = [ s for s in df2.columns if len(re.findall(chn_pattern,s))==0]
    if len(chn_columns)>0:
        df3=df2.melt(id_vars = melt_columns,   #要保留的主字段
                        var_name= "prod_name",            #拉长的分类变量
                        value_name="prod_price"    #拉长的度量值名称
                        ).fillna("")
    df3['prod_price'] = df3['prod_price'].apply(lambda x : x[:x.find('-')])
    df3['dt']=df3['initdata_update_time'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y-%m-%d"),"%Y%m%d"))    
    df3['prod_unit'] = '元/吨'
    #部分数据重复(url仅有细小差别)
    agg_funcs = {'url_link':'max'}
    df4 = df3.groupby(['type_name','dt','initdata_update_time','prod_name','prod_area','prod_price','prod_unit']).agg(agg_funcs).reset_index()  
   
    keep_columns = ['url_link','type_name','dt','initdata_update_time','prod_name','prod_area','prod_price','prod_unit']    
    df5 = df4[keep_columns]

    #生产唯一键
    hashkey_column_list = ['dt','prod_name','prod_area','prod_unit']
    df5['hashkey'] = df5.apply(generate_hashkey,args=(hashkey_column_list),axis=1) 

    save_data_to_mysql(df5,mysql_engine,schema_name,tmp_schema_name,table_name,keep_columns)
    
@logger.catch
def get_maintenance_dt(maintenance_dt,year,maintenance_pattern):
    '''
    根据装置停车计划开车、停车时间加工字段
    '''    
    #maintenance_dt = p5.iloc[1]['maintenance_start_dt']
    maintenance_str = re.findall(maintenance_pattern,maintenance_dt)
    if len(maintenance_str)>0:
        maintenance_str2 = maintenance_str[0]
        maintenance_str3 = datetime.strftime(datetime.strptime(maintenance_str2,"%m月%d日"),"%m%d")
        return str(year) + str(maintenance_str3)
    else:
        return str(year)+"年"+maintenance_dt

@logger.catch
def get_maintenance_days(maintenance_start_dt,maintenance_end_dt,chn_pattern):
    '''
    加工得到预计检修天数
    maintenance_start_dt = p5.iloc[1]['maintenance_start_dt']
    maintenance_end_dt = p5.iloc[1]['maintenance_end_dt']
    
    maintenance_start_dt = '2021年9月左右'
    '''

    if (len(re.findall(chn_pattern,maintenance_start_dt))==0) & (len(re.findall(chn_pattern,maintenance_end_dt))==0):
        maintenance_start_datetime = datetime.strptime(maintenance_start_dt,"%Y%m%d")
        maintenance_end_datetime = datetime.strptime(maintenance_end_dt,"%Y%m%d")
        return (maintenance_end_datetime - maintenance_start_datetime).days+1
    else:
        return '--'
    
@logger.catch
def save_pp_device_dynamic_future_data(pp_device_dynamic_future_data,mapping_dict,maintenance_pattern,chn_pattern,mysql_engine,schema_name,tmp_schema_name,table_name):
    '''
    加工PP装置检修计划   
    #pp_device_dynamic_future_data = pp_device_dynamic_future_data2.copy()
    '''
    
    pp_device_dynamic_future_data['pubTime'] = pp_device_dynamic_future_data['uploadTime'].apply(lambda x : datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
    pp_device_dynamic_future_data['year'] = pp_device_dynamic_future_data['pubTime'].apply(lambda x :x.year)
    p1 = pp_device_dynamic_future_data.reindex(pp_device_dynamic_future_data.index.repeat(pp_device_dynamic_future_data.dataList.str.len())).assign(dataList=np.concatenate(pp_device_dynamic_future_data.dataList.values))    

    p2 = pd.concat([p1.drop(['dataList'], axis=1), p1['dataList'].apply(pd.Series)], axis=1)
    p3 = p2[p2['tb_title'].str.contains('表2')] #过滤检修计划数据
    #二次展开字段   
    p4 = p3.reindex(p3.index.repeat(p3.tb_data.str.len())).assign(tb_data=np.concatenate(p3.tb_data.values))    
    p5 = pd.concat([p4.drop(['tb_data','pubTime'], axis=1), p4['tb_data'].apply(pd.Series)], axis=1)
    p5.columns = [ mapping_dict[c] if c in list(mapping_dict.keys()) else c for c in list(p5.columns)]
    p5.index = range(len(p5))
    p5['dt'] = p5['initdata_update_time'].apply(lambda x :datetime.strftime(datetime.strptime(x,"%Y-%m-%d %H:%M:%S"),"%Y%m%d"))    
    p5['dt_future_type'] = p5['dt'].apply(lambda x: 1 if x==max(p5['dt']) else 0)
    p5['title'] = p5['tb_title'].apply(lambda x : (x[x.find('表2')+3:]).strip())
    p5['maintenance_start_dt'] = p5.apply(lambda x : get_maintenance_dt(x['maintenance_start_dt'],x['year'],maintenance_pattern),axis=1)
    p5['maintenance_end_dt'] = p5.apply(lambda x : get_maintenance_dt(x['maintenance_end_dt'],x['year'],maintenance_pattern),axis=1)      
    p5['maintenance_days'] = p5.apply(lambda x : get_maintenance_days(x['maintenance_start_dt'],x['maintenance_end_dt'],chn_pattern),axis=1)   
    
    
    p5['prod_line_name'] = p5['maintenance_prod_capacity'].apply(lambda x : "".join(re.findall(chn_pattern,x)))
    p5['annual_prod_capacity'] = p5['maintenance_prod_capacity'].apply(lambda x : "".join(re.findall(r"\d{1,}",x)))
    p6 = p5.drop(['_id','year','type_name','tb_title','maintenance_prod_capacity'],axis=1)
    p6['prod_unit'] ='万吨/年'
    p6['po_type']='PP'
      
   
    #p7 = p6[p6['dt_future_type']==1]
    #p7.index = range(len(p7))
    p6['loss_capacity'] = p6.apply(lambda x : get_month_loss_capacity(x['maintenance_start_dt'],x['maintenance_end_dt'],x['annual_prod_capacity'],chn_pattern),axis=1)
    
    #p8 = pd.concat([p7.drop(['loss_capacity'], axis=1), p7['loss_capacity'].apply(pd.Series)], axis=1)
    p7 = p6.dropna()
    p8 = p7.reindex(p7.index.repeat(p7.loss_capacity.str.len())).assign(loss_capacity=np.concatenate(p7.loss_capacity.values))    
    p9= pd.concat([p8.drop(['loss_capacity'], axis=1), p8['loss_capacity'].apply(pd.Series)], axis=1)

    #从这边更新数据
    update_columns = p9.columns
    hashkey_column_list = ['dt','loss_month','prod_factory','po_type','prod_line_name','maintenance_start_dt','annual_prod_capacity']
    #生产唯一键
    p9['hashkey'] = p9.apply(generate_hashkey,args=(hashkey_column_list),axis=1)   
    save_data_to_mysql(p9,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns)



@logger.catch
def scrapy_pp_etl():
    chn_pattern= r"[\u4e00-\u9fa5]" #中文匹配
    prod_brand_pattern= r"[A-Za-z0-9-粉料]" #PP生产线匹配
    jianxiu_pattern =r"停车检修,计划.{1,}天"
    maintenance_pattern =r"\d{1,2}月\d{1,2}日" #装置检修计划匹配
    mysql_con = "mysql+pymysql://root:dZUbPu8eY$yBEoYK@27.150.182.135/"
    mysql_engine = create_engine(mysql_con,encoding='utf-8', echo=False,pool_timeout=3600)
    
    myclient = MongoClient("mongodb://root:root123456@27.150.182.135:27017/")
    mycol = myclient["Quotation"]["pp_zhuochuang_articleData"]
    lz_col = myclient["Quotation"]["pp_wangye_articleData"]
    
    #myclient["Quotation"]. list_collection_names()
    pdata = get_pdata(mycol)
    lz_data = get_pdata(lz_col)
    schema_name = "market_db"
    tmp_schema_name = "tmp_market_db"
    
    #lz_data['Type'].value_counts()
    mapping_dict = {"地区":"prod_area","厂家名称":"prod_factory","产能":"annual_prod_capacity",
                    "品种":"prod_name","用途":"prod_standard","装置动态":"prod_brand_change",
                    "25μ":"prod_price","link":"url_link","Type":"type_name","uploadTime":"initdata_update_time",
                    "白膜":"white_film","双防膜":"double_membrane_film","地膜":"mulch_film","西瓜膜":"watermelon_film",                    
                    "产销国":"stat_country","贸易方式":"trade_mode","数量":"imports_kilogram",
                    "产品类型":"prod_name","产品牌号":"prod_brand","成交价":"transaction_price",
                    "成交率":"turnover_rate","交货地":"delivery_place","挂单量（吨）":"pending_order_amount","成交量（吨）":"trading_amount",
                    "所属公司":"sinopec_prod_factory","石化名称":"prod_factory",
                    "拉丝":"pp_drawing","BOPP":"pp_bopp","CPP":"pp_cpp","pubTime":"initdata_update_time",
                    "透明料":"pp_transparent","其他":"pp_other","停车":"pp_stop",
                    "薄壁注塑":"thin_wall_moulding","普通均聚注塑":"common_homo_injection",
                    "低熔注塑":"low_melt_injection","中熔注塑":"middle_melt_injection",
                    "高熔注塑":"high_melt_injection","低熔纤维":"low_metl_fiber",
                    "高熔纤维":"high_melt_fiber","PPR管材":"ppr_tube","PPB管材":"ppb_tube",
                    "企业名称":"prod_factory","停车产能":"maintenance_prod_capacity",
                    "停车原因":"maintenance_remark","停车时间":"maintenance_start_dt","开车时间/检修天数":"maintenance_end_dt",
                    "日期":"dt","产品":"prod_name","规格":"prod_thicknesses","价格":"prod_price"}
    
    #PP排产比例上级目录
    pp_factory_percent_dict ={"拉丝":"拉丝","薄壁注塑":"均聚注塑","普通均聚注塑":"均聚注塑",
                              "低熔注塑":"共聚注塑","中熔注塑":"共聚注塑","高熔注塑":"共聚注塑",
                               "低熔纤维":"纤维","高熔纤维":"纤维","BOPP":"BOPP","CPP":"CPP",
                               "PPR管材":"管材","PPB管材":"管材","透明料":"透明料",	"其他":"其他",	"停车":"检修"}
    
    
   
    #PP装置动态汇总、装置动态企业名称映射
    pp_device_factory_mapping_dict ={"扬石化":"扬子石化","华东某合资企业":"上海赛科",
                                     "延长中煤榆林二期":"延长中煤榆林",
                                     "延安延长":"延安炼厂",
                                     "泉州炼厂":"中化泉州",
                                     "福基石化":"东华能源(宁波)",
                                     "东华扬子江石化":"东华能源(张家港)",
                                     "东华扬子江":"东华能源(张家港)"}
    device_table_name = "domestic_device_dynamics_data"
    pp_device_dynamic_data = pdata[pdata['Type']=='PP装置动态汇总'][-30:]
    pp_device_dynamic_sql = ''' select ddi.prod_factory ,ddi.prod_line_id ,ddi.po_type ,ddi.annual_prod_capacity ,ddi.daily_prod_capacity 
                             from market_db.device_dynamic_info ddi where ddi.po_type = 'PP' '''
    save_pp_device_data(pp_device_dynamic_sql,pp_device_dynamic_data,pp_device_factory_mapping_dict,mapping_dict,chn_pattern,prod_brand_pattern,jianxiu_pattern,mysql_engine,schema_name,tmp_schema_name,device_table_name) 
    
    #国内石化PP生产比例汇总 
    pp_factory_percent_table_name = "pp_scrapy_factory_percent"    
    pp_factory_percent_data = pdata[pdata['Type'].isin(['国内石化PP生产比例汇总'])]
    save_pp_factory_percent_data(pp_factory_percent_data,mapping_dict,pp_factory_percent_dict,schema_name,tmp_schema_name,pp_factory_percent_table_name,mysql_engine)
    
    
    #塑膜收盘价格表
    plastic_film_price_table_name = "plastic_film_price"    
    plastic_film_price_data = pdata[pdata['Type'].isin(['塑膜收盘价格表'])]
    save_plastic_film_price_data(plastic_film_price_data,mapping_dict,schema_name,tmp_schema_name,plastic_film_price_table_name,mysql_engine)
   
    
    #聚丙烯粉料及上游丙烯价格一览
    pp_upstram_table_name = 'pp_upstram_price'
    pp_price_data = pdata[pdata['Type'].isin(['聚丙烯粉料及上游丙烯价格一览'])]
    save_pp_upstram_data(pp_price_data,mapping_dict,chn_pattern,schema_name,tmp_schema_name,pp_upstram_table_name,mysql_engine)
    
    
    # #PP 装置检修计划一览表(隆众)
    # pp_device_dynamic_future_data = lz_data[lz_data['Type']=='国内PP装置检修']
    # pp_device_dynamic_future_data2 = pp_device_dynamic_future_data[pp_device_dynamic_future_data['title'].str.contains('国内PP装置检修及未来检修计划一览表')]
    # pp_device_dynamic_future_data2 = pp_device_dynamic_future_data2.sort_values(['uploadTime'])[-20:] #更新最近的几条记录
    # pp_device_dynamic_future_table_name = 'domestic_device_dynamics_future_data'
    # save_pp_device_dynamic_future_data(pp_device_dynamic_future_data2,mapping_dict,maintenance_pattern,chn_pattern,mysql_engine,schema_name,tmp_schema_name,pp_device_dynamic_future_table_name)
    