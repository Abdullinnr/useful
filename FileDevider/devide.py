import os

c_rows_count = 3
c_files_count = 1
file_path = r'C:\Users\NAbdullin\Documents\Servers\ClickHouseDev\Docker\import_data'
file_name = r'hits_100m_obfuscated_v1.tsv'

def devide_start():
    print(f"row count in one file: {c_rows_count}, splitted files max: {c_files_count}")
    files_fact = 0
    file_read = open(os.path.join(file_path, file_name), mode='rb')

    try:
        for file_num in range(c_files_count):
            file_write_name = f"{files_fact + 1}_{file_name}"
            arr = []
            for i in range(c_rows_count):
                row_str = file_read.readline()
                if row_str:
                    arr.append(row_str)
                else:
                    break
            if len(arr):
                try:
                    file_write = open(os.path.join(file_path, file_write_name), 'wb')
                    for wstr in arr:
                        file_write.write(wstr)
                    files_fact +=1
                    print(f"file #{files_fact} ''{file_write_name}'' {len(arr)} rows wrote")
                finally:
                    file_write.close()

    finally:
        file_read.close()
