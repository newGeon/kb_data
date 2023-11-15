import pandas as pd
import requests
import datetime
import time
import json
import sys
import os

from tqdm import tqdm

from kbutil.dbutil import db_connector

sys.path.append(os.getcwd())

if __name__ == '__main__':

    print("=== Image JSON DATA 로드 (START) ====================================================")

    basic_path = './data_json'

    file_list = os.listdir(basic_path)

    list_object = []
    list_action = []
    list_scene = []
    

    for one_file in tqdm(file_list):
        json_path = basic_path + '/' + one_file

        with open(json_path, 'r', encoding='utf-8-sig') as f:
            contents = f.read() # string 타입
            json_data = json.loads(contents)

        data_annotations = json_data['annotations']

        for one_data in data_annotations:

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
   
    set_object = set(list_object)
    set_action = set(list_action)
    set_scene = set(list_scene)

    keyword_dict = dict()
    keyword_dict = {'object': list(set_object), 'action': list(set_action), 'scene': list(set_scene)}

    with open(os.path.join('./data_info/keyword.json'), 'w', encoding='UTF-8') as fp:
        fp.write(json.dumps(keyword_dict, ensure_ascii=False, default=str, indent='\t'))

    print("=== Image JSON DATA 로드 (END) =================================================")
    print("================================================================================")
