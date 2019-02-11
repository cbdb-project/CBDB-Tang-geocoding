# -*- coding: utf-8 -*-
"""
Created on Thu Nov  8 19:17:13 2018

@author: yyy
"""

# 导入需要用到的函数
import requests, json
import numpy as np
import pandas as pd
from IPython.display import display

# step 1 建立一个新的dataframe，用来储存处理检索之后的数据，大写字母的attributes储存输入文件的内容，小写字母的attributes储存tgaz检索返回的结果

data = [["ID"],
        ["NAME"],
        ["FIRSTYEAR"],
        ["LASTYEAR"],
        ["count"],
        ["source"],
        ["name_found"],
        ["latitude"],
        ["longitude"],
        ["sys_id"],
        ["uri"],
        ["y_start"],
        ["y_end"],
        ["type"],
        ["parent_name"],
        ["parent_sys_id"]]
new_df=pd.DataFrame(data, columns=["attributes"]).set_index("attributes")

# 导入文件数据
dataframe = pd.read_excel('For_Yuying_Test_3250.xlsx', encoding='utf-8')

# 1）dataframe里面地名和起止年的读取
name_series=dataframe["name_simp_to_map"] # 储存地名
year_fy_series=dataframe['time_ft_col'] # 储存开始年份
year_ly_series=dataframe['time_lt_col'] # 储存结束年份
ID_series=dataframe['new_id'] # 储存原ID

# 2）-1 建立检索函数，返回相应的placements对应的values
# 返回一个【块】，用object type来过滤polygon，并且对找不到结果的东西定义成“空”
def find_placenames(name, year_fy, year_ly):
    tgaz_url = "http://maps.cga.harvard.edu/tgaz/placename?fmt=json&"
    name_url = "n="
    year_url = "&yr="
    other_url = "&ftyp=&src="
    full_url=tgaz_url + name_url+ name + year_url + other_url
    response = requests.get(full_url)
    search_results=response.json()
    places_list=[]
    if search_results["placenames"]:
        for item in search_results["placenames"]:
            if item["object type"]=="POINT":
                year_span=item["years"].split("~")
                b0=int(year_span[0]) # 检索结果的开始年
                b1=int(year_span[1]) # 检索结果的终止年
                if (b0<=year_fy and b1>=year_fy) or (b0<=year_ly and b1>=year_ly) or (b0>year_fy and b1<year_ly):
                    places_list.append(item)
    else:
        places_list=[]
    return(places_list)
    
# 3）-1 定义函数，对每个values里面的内容进行拆解并不断赋给new_dataframe里面相应的地方
def add_values(new_df,places_list,original_ID,original_name,year_fy, year_ly):
    i0=original_ID
    j=0
    if places_list:
        for item in places_list:
            j+=1
            i=str(i0)+"_"+str(j)
            new_df[i]=None
            new_df[i]["ID"]=original_ID
            new_df[i]["NAME"]=original_name
            new_df[i]["FIRSTYEAR"]=year_fy
            new_df[i]["LASTYEAR"]=year_ly
            new_df[i]["count"]=len(places_list)
            new_df[i]["source"]="tgaz"
            new_df[i]["name_found"]=item["name"]
            new_df[i]['latitude']=item["xy coordinates"].split(",")[1]
            new_df[i]['longitude']=item["xy coordinates"].split(",")[0]
            new_df[i]['sys_id']=item["sys_id"]
            new_df[i]["uri"]=item["uri"]
            new_df[i]["y_start"]=item["years"].split("~")[0]
            new_df[i]["y_end"]=item["years"].split("~")[1]
            new_df[i]["type"]=item["feature type"][0]
            new_df[i]["parent_name"]=item["parent name"]
            new_df[i]["parent_sys_id"]=item["parent sys_id"]
    else:
        i=str(i0)+"_"+str(j)
        new_df[i]=None
        new_df[i]["ID"]=original_ID
        new_df[i]["NAME"]=original_name
        new_df[i]["FIRSTYEAR"]=year_fy
        new_df[i]["LASTYEAR"]=year_ly
        new_df[i]["count"]=0
        new_df[i]["name_found"]=None
        new_df[i]['latitude']=None
        new_df[i]['longitude']=None
        new_df[i]['sys_id']=None
        new_df[i]["uri"]=None
        new_df[i]["y_start"]=None
        new_df[i]["y_end"]=None
        new_df[i]["type"]=None
        new_df[i]["parent_name"]=None
        new_df[i]["parent_sys_id"]=None
    return(new_df)

# 3）-2 对提取的原表格内容进行遍历循环，调用前面定义的两个函数
for k in range(0,len(name_series)):
    search_ID=ID_series[k]
    search_name=name_series[k]
    search_year_fy=year_fy_series[k] 
    search_year_ly=year_ly_series[k] 
    search_results=find_placenames(search_name,search_year_fy,search_year_ly)
    final_df=add_values(new_df,search_results,search_ID,search_name,search_year_fy, search_year_ly)

# 转置dataframe，并且按照ID 的大小进行排序
trans_new_df=new_df.transpose()
final_df=trans_new_df.sort_values("ID")

# 导出到文件
final_df.to_csv("yuying_test_3250_utf8.csv", encoding='utf-8')