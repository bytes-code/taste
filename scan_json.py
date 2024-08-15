import json
import argparse
from tqdm import tqdm
import mysql.connector
from data_process.data_processor import *


max_columns = 0
max_rows = 0

with open("data/gittables/train.gittables_100k_20c.json", 'r') as fcc_file:
    fcc_data = json.load(fcc_file)
    print(f"table cnt: {len(fcc_data)}")
    
    for i in range(len(fcc_data)):
        table_id = fcc_data[i][0]
        pgTitle = fcc_data[i][1]
        pgEntity = fcc_data[i][2]
        secTitle = fcc_data[i][3]
        caption = fcc_data[i][4]
        headers = fcc_data[i][5]
        cells = fcc_data[i][6]
        # annotations = fcc_data[i][7]

        max_columns = max(len(headers), max_columns)
        
        for col_idx in range(len(headers)):
            max_rows = max(len(cells[col_idx]), max_rows)
        
print(max_columns)
print(max_rows)
