import pandas as pd
import requests
import datetime
import mariadb
import time
import json
import os

from tqdm import tqdm


def db_connector():
    # Connect to MariaDB Platform    
    conn = mariadb.connect(
            user="geon",
            password="1234",
            host="127.0.0.1",
            port=3306,
            database="kb_data"
        )
    return conn

if __name__ == '__main__':

    print("=== Image JSON DATA 로드 후 DB와 비교 (START) ====================================================")

    basic_path = './data_json'

    file_list = os.listdir(basic_path)

    db_list_object = []
    db_list_action = []
    db_list_scene = []

    list_object = []
    list_action = []
    list_scene = []

    conn = db_connector()
    cur = conn.cursor()

    for one_file in tqdm(file_list):
        json_path = basic_path + '/' + one_file

        with open(json_path, 'r', encoding='utf-8-sig') as f:
            contents = f.read() # string 타입
            json_data = json.loads(contents)

        data_annotations = json_data['annotations']
        print('------------------------------')
        print(json_path)
        print('------------------------------')
        
        for one_data in tqdm(data_annotations):

            image_id = one_data['IMAGE_ID']

            sql_select = """ SELECT file_id, book_collection_name AS image_name, book_title AS image_title, image_class,
                                    image_large_category AS main_object, image_middle_category AS sub1_object, image_small_category AS sub2_object,
                                    book_publisher_name AS action, book_contents_type AS scene
                                FROM job_file_meta
                            WHERE image_class = ? """
            valuse_sql = (image_id, )
            cur.execute(sql_select, valuse_sql)
            data_result = cur.fetchone()         

            db_main_object = data_result[4]
            db_sub1_object = data_result[5]
            db_sub2_object = data_result[6]
        
            db_action = data_result[7]
            db_scene = data_result[8]
            
            db_list_object.append(db_main_object)
            db_list_object.append(db_sub1_object)
            db_list_object.append(db_sub2_object)

            db_list_action.append(db_action)
            db_list_scene.append(db_scene)


            main_object = one_data['MAINOBJECT']
            sub1_object = one_data['SUBOBJECT_1']
            sub2_object = one_data['SUBOBJECT_2']
        
            action = one_data['ACTION']
            scene = one_data['SCENE']
            
            list_object.append(main_object)
            list_object.append(sub1_object)
            list_object.append(sub2_object)

            list_action.append(action)
            list_scene.append(scene)
    
    conn.close()
  
    db_set_object = set(db_list_object)
    db_set_action = set(db_list_action)
    db_set_scene = set(db_list_scene)

    db_keyword_dict = dict()
    db_keyword_dict = {'object': list(db_set_object), 'action': list(db_set_action), 'scene': list(db_set_scene)}

    with open(os.path.join('./data_info/keyword_db.json'), 'w', encoding='UTF-8') as fp:
        fp.write(json.dumps(db_keyword_dict, ensure_ascii=False, default=str, indent='\t'))

    set_object = set(list_object)
    set_action = set(list_action)
    set_scene = set(list_scene)

    keyword_dict = dict()
    keyword_dict = {'object': list(set_object), 'action': list(set_action), 'scene': list(set_scene)}

    with open(os.path.join('./data_info/keyword_json.json'), 'w', encoding='UTF-8') as fp:
        fp.write(json.dumps(keyword_dict, ensure_ascii=False, default=str, indent='\t'))

    print("=== Image JSON DATA 로드 (END) =================================================")
    print("================================================================================")
  
