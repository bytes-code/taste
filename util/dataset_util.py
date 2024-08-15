import json


def get_max_col_num(datasets):
    max_columns = 0
    
    for dataset in datasets:
        with open(dataset, 'r') as fcc_file:
            fcc_data = json.load(fcc_file)
            for i in range(len(fcc_data)):
                headers = fcc_data[i][5]
                max_columns = max(len(headers), max_columns)
            
    return max_columns


