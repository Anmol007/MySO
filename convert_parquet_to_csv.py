import os
import pyarrow.parquet as pq

DATA_DIR = "<dir_where_parquet_files_are_stored>"
TARGET_DIR = "<dir_where_csv_files_need_to_be_stored>"

file_names = "<list_of_file_names_of_parquet_files>"

for filename in file_names:
	csv_path = os.path.join(TARGET_DIR, '{}.csv'.format(filename.split('.parquet')[0]))
	parquet_file = pq.ParquetFile(os.path.join(DATA_DIR, filename))
	num_row_groups = parquet_file.num_row_groups
	for row_group in range(num_row_groups):
		table1 = parquet_file.read_row_group(row_group)
		df = table1.to_pandas()
		df.to_csv(path_or_buf=csv_path, mode='a', index = False)
		print("\rProcessed {}/{} row groups in {}".format(
			row_group + 1, num_row_groups, filename
		), end='', flush=True)
	print('Processed: {}'.format(filename))
	print("Created {}".format(csv_path))
