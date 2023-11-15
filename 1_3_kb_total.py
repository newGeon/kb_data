import pandas as pd
import requests
import datetime
import warnings
import time
import json
import os

from tqdm import tqdm

from kbutil.dbutil import db_connector


warnings.filterwarnings(action='ignore')

if __name__ == '__main__':

    print("=== 객체 전체 리스트 만드는 코드 (START) ====================================================")

    df_new = pd.DataFrame(columns=['collect', 'data_type', 'big_category', 'small_category', 'keyword_ko', 'keyword_en'])
    json_name = './data_info/Total_Object_list_1013.json'

    with open(json_name, 'r', encoding='utf-8-sig') as f:
        contents = f.read() # string 타입
        json_data = json.loads(contents)

    for one_key in tqdm(json_data):
        
        one_json = json_data[one_key]

        temp_list = one_json.split('\\')

        j_big_class = temp_list[1].split('.', 1)[1]
        j_small_class = temp_list[2].split('.', 1)[1]
        j_keyword_ko = temp_list[3].split('.', 1)[1]

        df_new = df_new.append({'collect': 'JSON', 'data_type': '간접', 'big_category': j_big_class, 
                                'small_category': j_small_class, 'keyword_ko': j_keyword_ko, 
                                'keyword_en': ''}, ignore_index=True)

    file_name = './data_info/check_search_keyword_1121.xlsx'

    df_data = pd.read_excel(file_name)
    df_data.columns = ['data_type', 'big_category', 'small_category', 'keyword_ko', 'keyword_en']
    df_data = df_data.fillna('')

    
    list_big_category = list(set(df_data['big_category'].to_list()))
    list_big_category.sort()

    # 중복 제거
    for one_big in tqdm(list_big_category):
        
        df_big = df_data[df_data['big_category'] == one_big].sort_values(by=['small_category', 'keyword_ko', 'keyword_en']).reset_index()

        list_keywrod_ko = list(set(df_big['keyword_ko'].to_list()))

        for ko_word in list_keywrod_ko:
            
            keyword_en = ''

            df_keyword = df_big[df_big['keyword_ko'] == ko_word].sort_values(by=['keyword_en']).reset_index()

            list_keyword_en = list(set(df_keyword['keyword_en'].to_list()))

            if len(df_keyword) > 1:                
                for o_en in list_keyword_en:
                    if len(o_en) > 0:
                        keyword_en = o_en
            else:
                keyword_en = list_keyword_en[0]

            one_small = list(set(df_keyword['small_category'].to_list()))[0]

            df_new = df_new.append({'collect': 'EXCEL', 'data_type': '간접', 'big_category': one_big, 
                                    'small_category': one_small, 'keyword_ko': ko_word, 
                                    'keyword_en': keyword_en}, ignore_index=True)

    df_new = df_new.sort_values(by=['collect', 'big_category', 'small_category', 'keyword_ko'])


    df_save = pd.DataFrame(columns=['data_type', 'big_category', 'small_category', 'keyword_ko', 'keyword_en', 'total_cnt', 'excel_cnt', 'json_cnt'])

    list_keyword = list(set(df_data['keyword_ko'].to_list()))

    for one_word in tqdm(list_keyword):
        # print(one_word)
        df_temp = df_new[df_new['keyword_ko'] == one_word].sort_values(by=['collect']).reset_index()

        df_excel = df_temp[df_temp['collect'] == 'EXCEL'].reset_index()
        df_json = df_temp[df_temp['collect'] == 'JSON'].reset_index()

        keyword_en = ''

        list_keyword_en = list(set(df_temp['keyword_en'].to_list()))

        if len(df_temp) > 1:                
            for o_en in list_keyword_en:                
                if len(o_en) > 0:
                    keyword_en = o_en
        else:
            keyword_en = list_keyword_en[0]        

        df_save = df_save.append({'data_type': '간접', 'big_category': df_temp['big_category'][0], 
                                  'small_category': df_temp['small_category'][0], 'keyword_ko': df_temp['keyword_ko'][0], 
                                  'keyword_en': keyword_en, 'total_cnt': len(df_temp), 
                                  'excel_cnt': len(df_excel), 'json_cnt': len(df_json)}, ignore_index=True)


    df_save = df_save.sort_values(by=['data_type', 'big_category', 'small_category', 'keyword_ko']).reset_index()
    df_save.to_excel('./data_info/save_check_keyword_1121.xlsx')
    print("=== Keyword JSON 과 Knowledgebase DB 비교 코드 (END) ============================")
    print("================================================================================")
