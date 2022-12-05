import os, io
import random as rnd
import string
import datetime
from datetime import timedelta

NULL_VALUE_OFF = 0  # if = 0  sometimes it has empty value


NULL_VALUE_STR = r'\N'  # represent null value

file_path_tsv = r'C:\Users\NAbdullin\Documents\Servers\ClickHouseDev\Docker\import_data'
file_name_tsv = r'syntetic.tsv'
file_rowcount_tsv = 10

c_rows_count = 3
c_files_count = 1
file_path_sql = r'C:\Users\NAbdullin\Documents\Servers\ClickHouseDev\SQL'
file_name_sql = r'create_table.sql'
file_name_code = r'create_code.py'

def sql_table_rows_generator(table_name:str, col_int_count: int, col_str_count: int, col_date_count: int):
    cols_count_all = col_int_count + col_str_count + col_date_count
    col_int_count_fact = 1
    col_str_count_fact = 0
    col_date_count_fact = 0
    cols_count_all_fact = 1
    rows = [f'create table {table_name} (col01 UInt64, ']
    code_rows = []
    while cols_count_all_fact <= cols_count_all:
        case = rnd.randrange(0, 3)
        col_type_str = ''
        if case == 0:
            if col_int_count_fact <= col_int_count:
                col_type_str = 'Nullable(UInt64)'
                code_rows.append("s = s + get_col_value_int()")
                col_int_count_fact +=1
        elif case == 1:
            if col_str_count_fact <= col_str_count:
                col_type_str = 'Nullable(String)'
                code_rows.append("s = s + get_col_value_str()")
                col_str_count_fact +=1
        elif case == 2:
            if col_date_count_fact <= col_date_count:
                col_type_str = 'Nullable(DateTime)'
                code_rows.append("s = s + get_col_value_date()")
                col_date_count_fact +=1
        if col_type_str:
            cols_count_all_fact = col_int_count_fact + col_str_count_fact + col_date_count_fact
            col_name = f'col{cols_count_all_fact:02}'
            rows.append(f'{col_name} {col_type_str}, ')
            # print(f'{now:%Y-%m-%d %H:%M}')
    rows[-1] = rows[-1].replace(',', '')
    rows.append(f') ENGINE = MergeTree() PRIMARY KEY(col01);')

    return rows, code_rows

def sql_write_file(sql_arr):
    try:
        file_write = open(os.path.join(file_path_sql, file_name_sql), 'w')
        file_write.write(' \n'.join(sql_arr))
        print(f"file ''{file_name_sql}'' {len(sql_arr)} rows wrote")
    finally:
        file_write.close()

def code_write_file(code_arr):
    try:
        file_write = open(os.path.join(file_path_sql, file_name_code), 'w')
        file_write.write(' \n'.join(code_arr))
        print(f"file ''{file_name_code}'' {len(code_arr)} rows wrote")
    finally:
        file_write.close()


def get_col_value_int():
    case = rnd.randrange(NULL_VALUE_OFF, 3)
    val = NULL_VALUE_STR
    if case:
        val = str(rnd.randrange(0, 999999999999))
    val_str = val + '\t'
    return val_str

def get_col_value_date():
    case = rnd.randrange(NULL_VALUE_OFF, 3)
    val = NULL_VALUE_STR
    if case:
        val = get_rnd_date().strftime("%Y-%m-%d %H:%M:%S")
    val_str = val + '\t'
    return val_str

def get_col_value_str():
    str_length = rnd.randrange(30)
    case = rnd.randrange(NULL_VALUE_OFF, 3)
    val = NULL_VALUE_STR
    if case:
        val = ''.join(rnd.choices(string.ascii_uppercase + string.digits, k=str_length))
    val_str = val + '\t'
    return val_str

def get_rnd_date():
    """
    This function will return a random datetime
    """
    int_delta = (365*3 * 24 * 60 * 60) + rnd.randrange(60)
    random_second = rnd.randrange(int_delta)
    return datetime.datetime.today() - timedelta(seconds=random_second)

def tsv_file_generator():
    try:
        file_write = io.open(os.path.join(file_path_tsv, file_name_tsv), 'w', newline='\n')
        for i in range(file_rowcount_tsv):
            s = str(i) + '\t'
            s = s + get_col_value_date()
            s = s + get_col_value_int()
            s = s + get_col_value_str()
            s = s + get_col_value_date()
            s = s + get_col_value_str()
            s = s + get_col_value_str()
            s = s + get_col_value_int()
            s = s + get_col_value_str()
            s = s + get_col_value_date()
            s = s[:-1]
            file_write.write(s + '\n')
        print(f"There are {file_rowcount_tsv} rows wrote in file '{file_name_tsv}'")
    finally:
        file_write.close()
