import json
import argparse
from data_process.data_processor import *


def split_list(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]
    
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--col_split_threshold", default=20, type=int, required=False)
    parser.add_argument("--train_dataset", default=None, type=str, required=True)
    parser.add_argument("--dev_dataset", default=None, type=str, required=True)
    args = parser.parse_args()
    
    col_split_threshold = args.col_split_threshold

    for src_file in [args.train_dataset, args.dev_dataset]:
        small_tables = []
        with open(src_file, 'r') as fcc_file:
            fcc_data = json.load(fcc_file)
            
            for i in range(len(fcc_data)):
                table_id = fcc_data[i][0]
                pgTitle = fcc_data[i][1]
                pgEntity = fcc_data[i][2]
                secTitle = fcc_data[i][3]
                caption = fcc_data[i][4]
                headers = fcc_data[i][5]
                cells = fcc_data[i][6]
                annotations = fcc_data[i][7]

                headers_split = split_list(headers, col_split_threshold)
                cells_split = split_list(cells, col_split_threshold)
                annotations_split = split_list(annotations, col_split_threshold)
                
                for s_headers, s_cells, s_annotations in zip(headers_split, cells_split, annotations_split):
                    small_table_data = [table_id,pgTitle,pgEntity,secTitle,caption,s_headers,s_cells,s_annotations]
                    small_tables.append(small_table_data)
            
        with open(f"{src_file.replace('.json', '')}_{col_split_threshold}c.json", 'w', encoding='utf-8') as file:
            json.dump(small_tables, file, ensure_ascii=False)
