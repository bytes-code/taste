import mysql.connector
import numpy as np
from build_mysql_table import AUTO_INCREMENT_PKEY


class MysqlTableLoader():
    def __init__(self, host, port, username, password, database):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database
            )
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def disconnect(self):
        if self.connection is not None and self.connection.is_connected():
            self.connection.close()

    def list_all_tables(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW TABLES")
            result = cursor.fetchall()
            cursor.close()
            return result
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None

    def get_histograms(self):
        try:
            cursor = self.connection.cursor()
            get_histogram_sql = f"select * from information_schema.column_statistics where SCHEMA_NAME = '{self.database}'"
            cursor.execute(get_histogram_sql)
            result = cursor.fetchall()
            cursor.close()
            return result
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None

    def get_metadata(self, table_name, histogram_map, with_hist=False):
        try:
            cursor = self.connection.cursor()

            table_id = table_name[6:].replace("_", "-")
            
            get_table_comment_sql = f"SELECT table_comment FROM information_schema.tables WHERE table_schema = '{self.database}' AND table_name = '{table_name}'"
            cursor.execute(get_table_comment_sql)
            table_comment = cursor.fetchall()[0]

            metadata_list = str(table_comment[0]).split("#|+=")
            pgTitle = metadata_list[0]
            pgEnt = int(metadata_list[1])
            secTitle = metadata_list[2]
            caption = metadata_list[3]

            get_column_comment_sql = f'''
                SELECT COLUMN_NAME 
                FROM information_schema.COLUMNS 
                WHERE table_schema = '{self.database}' AND table_name = '{table_name}'
                ORDER BY ordinal_position
            '''
            cursor.execute(get_column_comment_sql)
            rs = cursor.fetchall()
            column_names = []
            for column_name in rs:
                column_names.append(column_name[0])
            column_names = column_names[1:] # skip first column, which is AUTO_INCREMENT key
            
            histogram = []
            if with_hist:
                for column_name in column_names:
                    column_histogram = histogram_map[table_name + "||" + column_name]
                    histogram.append(column_histogram)
                histogram = np.array(histogram)

            cursor.close()
            return table_id, pgTitle, pgEnt, secTitle, caption, column_names, histogram
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None

    def get_entity_data(self, table_name, col_num, col_names, col_idxs=None, random_select=False, select_cnt=50):
        try:
            if col_idxs is None:
                col_idxs = [idx for idx in range(col_num)]
            select_cols = ','.join([f"`{col_names[idx].replace('`', '``')}`" for idx in col_idxs])

            cursor = self.connection.cursor()
            # table_id = table_name[6:].replace("_", "-")

            if not random_select:
                select_data_sql = f"select {select_cols} from {table_name} limit {select_cnt}"
            else:
                select_data_sql = f'''
                    SELECT {select_cols}
                    FROM {table_name}, 
                        (SELECT RAND(0) * (SELECT GREATEST(0, (MAX({AUTO_INCREMENT_PKEY})-{select_cnt})) FROM {table_name}) AS mid) AS tmp
                    WHERE {table_name}.{AUTO_INCREMENT_PKEY} >= tmp.mid
                    ORDER BY {AUTO_INCREMENT_PKEY} ASC
                    LIMIT {select_cnt};
                '''
            cursor.execute(select_data_sql)
            rows = cursor.fetchall()

            entities = [[] for _ in range(col_num)]
            row_idx = 0
            for row in rows:
                for col_idx, cell_value in zip(col_idxs, row):
                    if cell_value != None:
                        entities[col_idx].append([[row_idx, col_idx], [0, str(cell_value)]])
                row_idx += 1

            cursor.close()
            return entities
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None
