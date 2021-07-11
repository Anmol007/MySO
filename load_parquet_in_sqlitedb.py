import os
import sqlite3
from datetime import datetime
import pyarrow.parquet as pq

DATA_DIR = "<PATH_TO_PARQUET_FILES>""
TARGET_DIR = "<PATH_TO_SAVE_THE_RESULTS>"
file_names = "<list_of_file_names_of_parquet_files>"

db_path = os.path.join(TARGET_DIR, "<NAME_OF_DB>_{}.sqlite"format(datetime.now().strftime("%Y%m%d")))

# Create a new SQLite Database for the herd
sqlite_connection = sqlite3.connect(db_path)
sqlite_connection.execute('PRAGMA journal_mode = WAL')
sqlite_connection.commit()

with sqlite_connection as conn:
    # iterate through the files
	for filename in file_names:
		parquet_file = pq.ParquetFile(os.path.join(DATA_DIR, filename))
		num_row_groups = parquet_file.num_row_groups
        # for perfomarance benifits iterate through row groups of parquet files
        for row_group in range(num_row_groups):
			table1 = parquet_file.read_row_group(row_group)
			df = table1.to_pandas()
			df.to_sql(
                filename.split('.parquet')[0], conn,
                if_exists='append',
                index = False
                )
			print("\rProcessed {}/{} row groups in {}".format(
            row_group + 1, num_row_groups, filename
            ), end='', flush=True)

		print('Processed: {}'.format(filename))

# close sqlite connection
sqlite_connection.close()
print("Created {}".format(db_path))
