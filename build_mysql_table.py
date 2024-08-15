import json
import argparse
from tqdm import tqdm
import mysql.connector
from data_process.data_processor import *

import warnings
warnings.filterwarnings('ignore')


COLUMN_NAME_CONFLICT_SEPARATOR = "%^"
COMMENT_SEPARATOR = "#|+="
AUTO_INCREMENT_PKEY = "pkey___"


def is_int(string):
    if "_" in string:
        return False
    try:
        int(string)
        return True
    except ValueError:
        return False
    
    
def get_col_data_type(is_int_type, max_int, max_str_len):
    if max_int == 0 and max_str_len == 0:
        return "CHAR"
    elif not is_int_type:
        if max_str_len < 3000:
            return f"VARCHAR({max_str_len})"
        else:
            return f"TEXT"
    elif abs(max_int) < 128:
        return "TINYINT"
    elif abs(max_int) < 32768:
        return "SMALLINT"
    elif abs(max_int) < 8388608:
        return "MEDIUMINT"
    elif abs(max_int) < 2147483648:
        return "INT"
    else:
        return "BIGINT"


def build_single_table(connection, cursor, table_id, pgTitle, pgEntity, secTitle, caption, headers, cells):
    table_name = 'table_' + str(table_id).replace('-', '_')
    
    # preprocess content data
    col_data_type = []
    table_data = []
    is_int_type = True
    for ci in range(len(headers)):
        max_int = 0
        max_str_len = 0
        
        col_cell = cells[ci]
        for cj in range(len(col_cell)):
            index = col_cell[cj][0]
            cell_id = col_cell[cj][1][0]
            cell_value = col_cell[cj][1][1]

            row_idx = index[0]
            col_idx = index[1]

            existed_rows = len(table_data)
            for _ in range(row_idx + 1 - existed_rows):
                table_data.append([None for _ in range(len(headers))])

            table_data[row_idx][col_idx] = cell_value
            
            max_str_len = max(len(cell_value), max_str_len)
            if is_int(cell_value):
                max_int = max(abs(int(cell_value)), max_int)
            else:
                is_int_type = False
        
        col_data_type.append(get_col_data_type(is_int_type, max_int, max_str_len))
            
    # create table
    comment = COMMENT_SEPARATOR.join([pgTitle[:100], str(pgEntity), secTitle[:100], caption[:100]])
    comment = str(comment).replace('\\', "\\\\").replace("'", "\\\'").replace('"', "\\\"")
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name}("
    create_table_sql += f"`{AUTO_INCREMENT_PKEY}` bigint NOT NULL AUTO_INCREMENT,"
    
    column_names = []
    for ci in range(len(headers)):
        column_name = str(headers[ci])[:64]
        column_name = column_name.strip().replace('`', '``')
        if len(column_name) == 0:
            column_name = COLUMN_NAME_CONFLICT_SEPARATOR + str(ci)
        if column_name.lower() in column_names:
            postfix = COLUMN_NAME_CONFLICT_SEPARATOR + str(ci)
            column_name = column_name[:64 - len(postfix)] + postfix
        column_names.append(column_name.lower())
        create_table_sql += f"`{column_name}` {col_data_type[ci]} "
        create_table_sql += ", "
    
    create_table_sql += f"PRIMARY KEY (`{AUTO_INCREMENT_PKEY}`))"
    create_table_sql += "COMMENT = "
    create_table_sql += '\"'
    create_table_sql += comment
    create_table_sql += '\"'
    
    cursor.execute(create_table_sql)
    connection.commit()

    # check if already inserted
    query = f"SELECT COUNT(*) FROM {table_name}"
    cursor.execute(query)
    row_count = cursor.fetchone()[0]
    if row_count != len(table_data):
        # insert data
        for row_idx in range(len(table_data)):
            insert_sql = f"INSERT INTO {table_name}("
            for col_idx in range(len(table_data[row_idx])):
                insert_sql += f"`{column_names[col_idx]}`" + ", "
            insert_sql = insert_sql.rstrip(', ') + ")"
            insert_sql += f" VALUES ("
        
            for col_idx in range(len(table_data[row_idx])):
                if table_data[row_idx][col_idx] != None:
                # if table_data[row_idx][col_idx] and len(table_data[row_idx][col_idx]) > 0:
                    insert_sql += '\"'
                    insert_sql += table_data[row_idx][col_idx].replace('\\', "\\\\").replace("'", "\\'").replace('"', '\\"')
                    insert_sql += '\"' + ", "
                else:
                    insert_sql += 'null' + ", "

            insert_sql = insert_sql.rstrip(', ') + ")"
            cursor.execute(insert_sql)
        connection.commit()

    # generate histogram
    for ci in range(len(headers)):
        generate_histogram_sql = f"ANALYZE TABLE {table_name} UPDATE HISTOGRAM ON `{column_names[ci]}` WITH 1024 BUCKETS;"
        cursor.execute(generate_histogram_sql)
        cursor.fetchall()
    connection.commit()


def build_tables(src_data_path, db_name, connection):
    database_name = db_name
    cursor = connection.cursor()

    # create database
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    print(f"Database '{database_name}' created successfully.")

    cursor.execute(f"USE {database_name}")
    print(f"Switched to database '{database_name}'.")

    print(f"Building tables...")
    with open(src_data_path, 'r') as fcc_file:
        fcc_data = json.load(fcc_file)
        for i in tqdm(range(len(fcc_data)), desc="Processing"):
            table_id = fcc_data[i][0]
            pgTitle = fcc_data[i][1]
            pgEntity = fcc_data[i][2]
            secTitle = fcc_data[i][3]
            caption = fcc_data[i][4]
            headers = fcc_data[i][5]
            cells = fcc_data[i][6]
            # annotations = fcc_data[i][7]

            build_single_table(connection, cursor, table_id, pgTitle, pgEntity, secTitle, caption, headers, cells)

    cursor.close()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--mysql_host", default=None, type=str, required=True)
    parser.add_argument("--mysql_port", default=3306, type=int, required=False)
    parser.add_argument("--mysql_user", default=None, type=str, required=True)
    parser.add_argument("--mysql_password", default=None, type=str, required=True)
    parser.add_argument("--eval_database", default=None, type=str, required=True)
    parser.add_argument("--test_dataset", default=None, type=str, required=True)

    args = parser.parse_args()

    connection = mysql.connector.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
    )

    build_tables(args.test_dataset, args.eval_database, connection)

    connection.close()


if __name__ == "__main__":
    main()

