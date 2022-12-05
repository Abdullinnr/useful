import os
import devide
import generator

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

# devide.devide_start()

def sql_code_generator():
    sql, code = generator.sql_table_rows_generator('table_name', 2, 3, 4)
    print(sql)
    generator.sql_write_file(sql)
    generator.code_write_file(code)

generator.tsv_file_generator()

