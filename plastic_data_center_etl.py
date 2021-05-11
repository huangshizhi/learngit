 # -*- coding: utf-8 -*-
"""
Created on Fri Feb  5 08:55:37 2021

@author: huangshizhi
塑化行情中心加工脚本(包含PP+PE两个品种)

"""
import requests
import json
from loguru import logger
import time
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime,timedelta
from scrapy_util import generate_hashkey,save_data_to_mysql,truncate_insert_data 
from scrapy_pp_etl import scrapy_pp_etl
from scrapy_pe_etl import scrapy_pe_etl
from pathlib import Path

@logger.catch
def get_recent_months_contract_value(a,b,c,t):
    '''
    根据日期转换值及近、中远、远月值得到近月值
    a =近月
    b =中远月
    c =远月
    '''
    if 115<t<515:
        return b
    elif 515<t<917:
        return c
    else:
        return a

@logger.catch    
def get_middle_months_contract_value(a,b,c,t):
    '''
    根据日期转换值及近、中远、远月值得到中远月值
    a =近月
    b =中远月
    c =远月
    '''
    if 115<t<515:
        return c
    elif 515<t<917:
        return a
    else:
        return b

@logger.catch    
def get_far_months_contract_value(a,b,c,t):
    '''
    根据日期转换值及近、中远、远月值得到远值
    a =近月
    b =中远月
    c =远月
    '''
    if 115<t<515:
        return a
    elif 515<t<917:
        return b
    else:
        return c    
  
@logger.catch    
def save_monthbias_data(wind_pe_price_sql,mysql_engine,schema_name,table_name):
    '''
    计算PE中远月月差等
    '''
    wind_pe_price_data = pd.read_sql(wind_pe_price_sql,con = mysql_engine)
    wind_pe_price_data2 = wind_pe_price_data.drop(['wind_name'],axis=1)
    #2.1先转成宽表   
    wind_pe_pivot_table = wind_pe_price_data2.pivot(
        index=["dt"],    #行索引（可以使多个类别变量）
        columns="wind_code",  #列索引（可以使多个类别变量）
        values="securities_price"         #值（一般是度量指标）
     ).reset_index()
    
    #计算日期值
    wind_pe_pivot_table['day_value'] = wind_pe_pivot_table['dt'].apply(lambda x :100*datetime.strptime(x, "%Y%m%d").month+datetime.strptime(x, "%Y%m%d").day)
      
    #根据当前day_value得到对应近月、中远月、远月等
    wind_pe_pivot_table['recent_months_contract_value'] = wind_pe_pivot_table.apply(lambda x :get_recent_months_contract_value(x['S0266365'],x['S0266431'],x['S0266499'],x['day_value']),axis=1)
    wind_pe_pivot_table['recent_middle_contract_value'] = wind_pe_pivot_table.apply(lambda x :get_middle_months_contract_value(x['S0266365'],x['S0266431'],x['S0266499'],x['day_value']),axis=1)
    wind_pe_pivot_table['recent_far_contract_value'] = wind_pe_pivot_table.apply(lambda x :get_far_months_contract_value(x['S0266365'],x['S0266431'],x['S0266499'],x['day_value']),axis=1)
    
    wind_pe_pivot_table = wind_pe_pivot_table.eval('''monthbias91 = S0266499-S0266365
                                                       monthbias15=S0266365-S0266431
                                                       monthbias59=S0266431-S0266499
                                                       monthbiasrecentmiddle=recent_months_contract_value-recent_middle_contract_value  
                                                       monthbiasmiddlefar=recent_middle_contract_value-recent_far_contract_value
                                                    ''')
       
    monthbias_data = wind_pe_pivot_table[['dt','S0266365','S0266431','S0266499','monthbias91','monthbias15','monthbias59','monthbiasrecentmiddle','monthbiasmiddlefar']]
    truncate_insert_data(monthbias_data,mysql_engine,schema_name,table_name)
     
@logger.catch   
def wind_data_etl(wind_sql,mysql_engine):
    '''
    加工wind数据，转为宽表
    '''    
    wind_data = pd.read_sql(wind_sql,con=mysql_engine)
    #转成宽表
    wind_pivot_data = wind_data.pivot(
        index=["dt"],    #行索引（可以使多个类别变量）
        columns=["wind_code"],  #列索引（可以使多个类别变量）
        values="securities_price"         #值（一般是度量指标）
     ).reset_index()    
    #即期汇率:美元兑人民币(按前值计算)
    wind_pivot_data['M0067855'] = wind_pivot_data['M0067855'].fillna(method='ffill') 
    #增值税率 2018年之前系数是1.17,之后是1.13)
    wind_pivot_data['vat_rates'] = wind_pivot_data['dt'].apply(lambda x : 1.13 if int(x[:4])>=2018 else 1.17)
    wind_pivot_data['dt'] = wind_pivot_data['dt'].astype(int)   
    return wind_pivot_data


@logger.catch
def icis_data_etl(icis_sql,mysql_engine):
    '''
    加工安迅思数据，转为宽表
    '''    
    icis_data = pd.read_sql(icis_sql,con = mysql_engine)          
    icis_pivot_data = icis_data.pivot(
        index=["dt"],   #行索引（可以使多个类别变量）
        columns=["icis_code"],  #列索引（可以使多个类别变量）
        values="icis_price"         #值（一般是度量指标）
     ).reset_index()    
    #大数替换为空值  
    icis_pivot_data2 = icis_pivot_data.fillna(bignum)    
    icis_pivot_data2[icis_pivot_data2.columns] = icis_pivot_data2[icis_pivot_data2.columns].apply(pd.to_numeric)    
    #判断值大小进行替换abs(x)>=900000000
    icis_pivot_data2[abs(icis_pivot_data2)>0.9*bignum]=""   
    return icis_pivot_data2

@logger.catch
def scrapy_data_etl(scrapy_sql,mysql_engine):
    '''
    加工爬虫数据，转为宽表
    '''    
    scrapy_data = pd.read_sql(scrapy_sql,con = mysql_engine).drop_duplicates()          
    scrapy_pivot_data = scrapy_data.pivot(
        index=["dt"],   #行索引（可以使多个类别变量）
        columns=["po_code"],  #列索引（可以使多个类别变量）
        values="prod_price"         #值（一般是度量指标）
     ).reset_index()    
    #大数替换为空值  
    scrapy_pivot_data2 = scrapy_pivot_data.fillna(bignum)    
    scrapy_pivot_data2[scrapy_pivot_data2.columns] = scrapy_pivot_data2[scrapy_pivot_data2.columns].apply(pd.to_numeric)    

    return scrapy_pivot_data2

@logger.catch
def save_po_supply_chain_profit(wind_sql,icis_sql,scrapy_sql,mysql_engine,schema_name,tmp_schema_name,table_name):
    '''    
    加工PE+PP利润价差等指标
    vat_rates:增值税率
    oil_pe_profit:油制PE利润,oil_pp_profit:油制PP利润
    mto_pe_profit:MTO制PE利润,mto_pp_profit:MTO制PP利润
    ethylene_pe_profit:乙烯制PE利润, pdh_pp_profit:丙烷脱氢(PDH)利润
    import_pe_profit:PE进口利润,import_pp_profit:PP进口利润
    export_pe_profit:PE出口利润,export_pp_profit:PP出口利润   
    pe_price_bias:PE主流牌号价格基差,pp_price_bias:PP主流牌号价格基差
    lldpe_zsgy_bd_bias:新料-再生料高压价差
    lldpe_zsdy_ly_bias:新料-再生料低压价差
    pe_stretch_film_profit:缠绕膜利润
    mulch_film_profit:地膜利润
    double_membrane_film_profit:双防膜利润
    propylene_pp_profit:丙烯制PP利润
    pp_yarn_recycled_materials_bias:新料-回料价差
    pp_yarn_powder_bias:粒料-粉料价差
    bopp_pp_profit:BOPP利润
    adhesive_jumbo_roll_profit:胶带利润
    '''
    start_time = time.time()
    wind_data = wind_data_etl(wind_sql,mysql_engine) #wind数据
    icis_data = icis_data_etl(icis_sql,mysql_engine) #安迅思数据   
    scrapy_data = scrapy_data_etl(scrapy_sql,mysql_engine) #爬虫数据
    
    #合并所有数据
    market_data_init = pd.merge(left = wind_data,right=icis_data,how='left',on='dt')
    market_data = pd.merge(left = market_data_init,right = scrapy_data,how='left',on='dt')
    
    market_data = market_data.fillna("")
    market_data[market_data.columns] = market_data[market_data.columns].apply(pd.to_numeric) 
    
    
    #加工PE+PP利润数据
    market_data = market_data.eval(''' oil_pe_profit = domestic_north_pe_lldpe_film_daily_low-(((S5111905+4)*7.3*M0067855*vat_rates+685)*1.3+560)
                                   mto_pe_profit = domestic_east_pe_lldpe_film_daily_low-2.9*S5443799-1250
                                   ethylene_pe_profit = M0067855*(S5431475-S5400550-150)
                                   import_pe_profit = import_east_pe_lldpe_film_daily_low - (M0067855*cfr_pe_lldpe_film_daily_low*vat_rates*1.065+150)
                                   export_pe_profit = (S5431478-(domestic_east_pe_lldpe_film_daily_low/M0067855)/vat_rates-30)                                                                      
                                   pe_price_bias =(domestic_east_pe_lldpe_film_daily_low-M0066351)                                  
                                   oil_pp_profit = domestic_east_pp_yarn_daily_low-(((S5111905+4)*7.35*M0067855*vat_rates+585)*1.35+500)
                                   mto_pp_profit = domestic_east_pp_yarn_daily_low-2.9*S5443799-1250
                                   pdh_pp_profit = domestic_east_pp_yarn_daily_low-((1.2*S5122021+200)*vat_rates*M0067855*1.01+800)
                                   import_pp_profit = domestic_east_pp_yarn_daily_low -(S5431502*M0067855*vat_rates*1.065+150)
                                   export_pp_profit = S5431505-(domestic_east_pp_yarn_daily_low /M0067855)/vat_rates-30
                                   pp_price_bias = domestic_east_pp_yarn_daily_low - S0203118                      
                                   lldpe_zsgy_bd_bias= domestic_north_pe_lldpe_film_daily_low-pe_zsgy_bd_gybtmyjzl
                                   lldpe_zsdy_ly_bias=domestic_north_pe_lldpe_film_daily_low-pe_zsdy_ly_whhbskl
                                   pe_stretch_film_profit=(pe_stretch_film - domestic_north_pe_lldpe_film_daily_low-1600)
                                   mulch_film_profit=(mulch_film/0.915-domestic_north_pe_lldpe_film_daily_low-1000)
                                   double_membrane_film_profit=(double_membrane_film/0.915-0.6*domestic_north_pe_lldpe_film_daily_low-0.4*domestic_north_pe_ldpe_film_daily_low-1500)                          
                                   propylene_pp_profit = (domestic_east_pp_yarn_daily_low-pp_propene_jz_price-800)
                                   pp_yarn_recycled_materials_bias=domestic_east_pp_yarn_daily_low-pp_zs_ly_btmyzyjpcl
                                   pp_yarn_powder_bias=domestic_east_pp_yarn_daily_low-pp_powder_jz_price
                                   bopp_pp_profit=bopp_light_membrane-domestic_east_pp_yarn_daily_low-1400
                                   adhesive_jumbo_roll_profit=(adhesive_jumbo_roll-(bopp_light_membrane+S5443282)/2-600)
                                ''')
        
    
    #宽表转窄表
    market_melt_data = market_data.melt(id_vars=["dt"],   #要保留的主字段
                    var_name="po_code",         #拉长的分类变量
                    value_name="po_profit"      #拉长的度量值名称
                    )
    market_melt_data = market_melt_data.dropna()
    market_melt_data['po_profit'] = market_melt_data['po_profit'].apply(pd.to_numeric)    
    #market_melt_data2 = market_melt_data[abs(market_melt_data['po_profit'])>0.01*bignum]
    market_melt_data_filter = market_melt_data[abs(market_melt_data['po_profit'])<0.01*bignum]
    update_columns = ['po_code','dt','po_profit']
    hashkey_columns = ['po_code','dt']
    market_melt_data_filter['hashkey'] = market_melt_data_filter.apply(generate_hashkey,args=(hashkey_columns),axis=1)   
    save_data_to_mysql(market_melt_data_filter,mysql_engine,schema_name,tmp_schema_name,table_name,update_columns)
    logger.info("更新聚烯烃利润价差等指标耗时%.2fs"%(time.time()-start_time))

@logger.catch
def load_operating_rate_data(operating_rate_url):
    '''
    从业务系统的API获取数据
    '''
    s = requests.session()
    s.keep_alive = False # 关闭多余连接
    operating_rate_mapping_dict = {"id":"id","date":"dt","categoryName":"po_type","materialName":"prod_name","productName":"prod_standard","operatingRate":"operating_rate"}
    response = s.get(url = operating_rate_url,headers={'Content-Type':'application/json',"Connection":"keep-alive"})
    logger.info("返回的状态码:%s"%json.loads(response.text)['code'])
    response_code = json.loads(response.text)['code']
    if response_code == '200':
        df_list = json.loads(response.text)['entity']
        df = pd.DataFrame(df_list)
        df.columns = [operating_rate_mapping_dict[x] for x in list(df.columns)]
        df['hashkey'] = df.apply(generate_hashkey,args=(['dt','po_type','prod_name','prod_standard']),axis=1)
        df = df.drop(['id'],axis=1)
        
    return df


if __name__=='__main__': 
    mysql_con = "mysql+pymysql://root:dZUbPu8eY$yBEoYK@27.150.182.135/"
    mysql_engine = create_engine(mysql_con,encoding='utf-8', echo=False)
    bignum = 9e10
    schema_name = "market_db"
    tmp_schema_name = "tmp_market_db"
    operating_rate_url = "https://hqadmintest.zuiyouliao.com/api/quotation/operatingRate/getOperatingRateAll"
    project_path = Path.cwd().parent       ##获取当前模块文件所在目录的上一层
    log_path = Path(project_path, "log")   ##产生日志目录字符串    
    t = time.strftime("%Y%m%d_%H%M%S")     ##20200624_111255
    
    logger.add(f"{log_path}/log_info_{t}.log", 
               format=" {time:YYYY-MM-DD HH:MM:SS} |{name} | {line} | {message}" , 
               enqueue=True,level="INFO",retention="90 days",
               rotation="500MB", encoding="utf-8")   
    
    
    
    logger.info("*"*80)
    logger.info("专塑行情数据加工开始......")
    start_time = time.time()
    
    #1.补充获取下游开工率数据    
    operating_rate_pdate_columns = ['dt','po_type','prod_name','prod_standard']  
    operating_rate_table_name = "downstream_factory_operating_rate"
    operating_rate_data = load_operating_rate_data(operating_rate_url)
    save_data_to_mysql(operating_rate_data,mysql_engine,schema_name,tmp_schema_name,operating_rate_table_name,operating_rate_pdate_columns)    
  
    #2.加工PE数据
    scrapy_pe_etl()
    #3.加工PP数据
    scrapy_pp_etl()

    #4.整合加工wind+icis+爬虫三方数据进行加工
    #增量更新近30天数据
    start_load_dt = datetime.strftime(datetime.today()-timedelta(days=30),"%Y%m%d")
    #start_load_dt = '20120101'
    #4.1 Wind查询SQL
    wind_sql = ''' select dt,wind_code,wind_name,securities_price
                    from market_db.wind_data
                    where wind_code in ('M0066351','S5443799','S5122021','S5400550',
                    	'S5431502','S5431505','S0203118','S0031869','S5431475','S5431478',
                    	'M0086441','M0096604','S0266365','S0266431','M0000185','S5443282',
                    	'S0266499','S0164028','S5111905','M0067855')
                    and dt >= '''+start_load_dt +" order by dt desc"         

    #4.2安迅思查询SQL
    icis_sql = "select dt,mapping_eng_column_name as icis_code,icis_price from market_db.icis_spot_data where length(mapping_eng_column_name)>0 " + " and dt >=" + start_load_dt
    #4.3 爬虫数据SQL
    scrapy_sql = "SELECT dt, po_code, prod_name, prod_market, prod_specifications, prod_factory, prod_area, prod_price, plat_source_remark FROM market_db.po_scrapy_price_v where 1=1 and dt >= " + start_load_dt   
    save_po_supply_chain_profit(wind_sql,icis_sql,scrapy_sql,mysql_engine,schema_name,tmp_schema_name,table_name='po_supply_chain_profit')
    logger.info("完成数据中心数据加工耗时%.2fs"%(time.time()-start_time))
    
    #5.期货月差(PE日报)
    wind_pe_bias_sql = "SELECT dt,wind_code,wind_name,securities_price FROM market_db.wind_data \
    where wind_code in ('M0066351','S0031869','M0086441','M0096604','S0266365','S0266431','S0266499','S0164028','S5111905') and dt>='20120101'"
    save_monthbias_data(wind_pe_bias_sql,mysql_engine,schema_name,table_name='monthbias_data')


