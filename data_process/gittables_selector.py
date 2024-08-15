import os
import random
from pyarrow import parquet as pq
import json
import math
import argparse


def read_all_file_name(folder_path):
    folder_cnt = 0
    file_cnt = 0

    all_file_paths = []
    for root, dirs, _ in os.walk(folder_path):
        for sub_dir in dirs:
            sub_dir_path = os.path.join(root, sub_dir)
            folder_cnt += 1

            print(folder_cnt, file_cnt, end='\r')

            for _, _, files in os.walk(sub_dir_path):
                for file_name in files:
                    if '.parquet' not in file_name:
                        continue

                    file_path = os.path.join(sub_dir_path, file_name)
            
                    all_file_paths.append(file_path)
                    file_cnt += 1
    return all_file_paths


def random_select_tables(all_file_paths, random_select_cnt, random_seed=0):
    tables = []

    random.seed(random_seed)
    random.shuffle(all_file_paths)
    for file_path in all_file_paths:
        try:
            table = pq.read_table(file_path)
        except:
            continue
        
        table_name = file_path.split('/')[-1].replace('.parquet', '')
        tables.append((table_name, table))
        print(f'select_cnt: {len(tables)} / {random_select_cnt}', end='\r')
        if len(tables) >= random_select_cnt:
            break
    return tables


def split_tables(all_tables, split_sizes, random_seed=0):
    random.seed(random_seed)
    random.shuffle(all_tables)

    total_length = len(all_tables)
    print(total_length)
    split_1_length = math.ceil(total_length * split_sizes[0])
    split_2_length = int(total_length * split_sizes[1])

    train_tables = all_tables[:split_1_length]
    dev_tables = all_tables[split_1_length:split_1_length+split_2_length]
    test_tables = all_tables[split_1_length+split_2_length:]
    return train_tables, dev_tables, test_tables


def construct_datasets(train_tables, dev_tables, test_tables, reserve_rows_for_train):
    train_dataset = []
    dev_dataset = []
    test_dataset = []

    type_set = set()
    table_id = 0
    for tables, dataset, dataset_type in (
            (train_tables, train_dataset, "train"), 
            (dev_tables, dev_dataset, "dev"), 
            (test_tables, test_dataset, "test")
        ):
        for table_name, table in tables:
            pgTitle = ''
            pgEntity = 0
            secTitle = ''
            caption = table_name

            metadata = table.schema.metadata
            table_df = table.to_pandas()
            table_df = table_df.reset_index(drop=True)
            column_types = json.loads(metadata[b'gittables'].decode('utf-8'))

            headers = [c for c in table_df.columns]
            annotations = [[] for _ in range(len(headers))]

            for col_idx, col in enumerate(table_df.columns):
                if col in column_types['dbpedia_semantic_column_types']:
                    col_type = column_types['dbpedia_semantic_column_types'][col]['cleaned_label'].lower()
                    annotations[col_idx].append(col_type)
                    type_set.add(col_type)

            cells = [[] for _ in range(len(headers))]
            for row_idx, row in table_df.iterrows():
                if dataset_type != 'test' and row_idx >= reserve_rows_for_train:
                    # for train/dev dataset, only reserve a fixed number of rows to save memory usage during training
                    # but for test dataset,  reserve all rows to simulate real prediction scenarios.
                    break
                for col_idx, header in enumerate(headers):
                    cell_value = str(row[header])
                    if cell_value == 'None' or cell_value == 'nan':
                        cell_value = ''
                    cells[col_idx].append([[row_idx, col_idx], [0, cell_value]])

            table_data = [table_id,pgTitle,pgEntity,secTitle,caption,headers,cells,annotations]
            dataset.append(table_data)
            table_id += 1
            print(table_id, end='\r')

    return train_dataset, dev_dataset, test_dataset, type_set


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--reserve_rows_for_train", default=50, type=int, required=False)
    args = parser.parse_args()
    
    folder_path = 'data/gittables/unzipped'

    all_file_paths = read_all_file_name(folder_path)
    print(f'all files cnt: {len(all_file_paths)}')
    if len(all_file_paths) == 0:
        print(f'ERROR: please check if the folder path is correct, folder_path={folder_path}')
    all_file_paths.sort()

    random_select_cnt = 100000
    tables = random_select_tables(all_file_paths, random_select_cnt)

    train_tables, dev_tables, test_tables = split_tables(tables, [0.8,0.1,0.1])
    print('split finished, sizes:', len(train_tables), len(dev_tables), len(test_tables))

    train_dataset, dev_dataset, test_dataset, type_set = construct_datasets(train_tables, dev_tables, test_tables, args.reserve_rows_for_train)
    print('construct datasets finished, sizes:', len(train_dataset), len(dev_dataset), len(test_dataset))
    
    print('saving into files..')
    with open(f'data/gittables/test.gittables_{int(random_select_cnt/1000)}k.json', 'w', encoding='utf-8') as file:
        json.dump(test_dataset, file, ensure_ascii=False)

    with open(f'data/gittables/dev.gittables_{int(random_select_cnt/1000)}k.json', 'w', encoding='utf-8') as file:
        json.dump(dev_dataset, file, ensure_ascii=False)

    with open(f'data/gittables/train.gittables_{int(random_select_cnt/1000)}k.json', 'w', encoding='utf-8') as file:
        json.dump(train_dataset, file, ensure_ascii=False)
    print('finished.')
    
    all_type = list(type_set)
    all_type.sort()
    random.seed(0)
    random.shuffle(all_type)

    with open(f'type_vocab/gittables/type_vocab_{len(all_type)}.txt', 'w') as f:
        for i, type in enumerate(all_type):
            f.write(str(i) + '\t' + type + '\n')
            