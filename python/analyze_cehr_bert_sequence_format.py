import pandas as pd
import pyarrow.parquet as pq
import pyarrow
import os

folder = 'D:/omopSynthea/cehr-bert/patient_sequence'
pfile = pq.read_table(os.path.join(folder, 'part-00000-c0fda67a-757c-41ba-8c31-a69d1f7bf530-c000.snappy.parquet'))
x = pfile.to_pandas()
print(x.columns)
for column in x.columns:
  print(f"column: {column}")
  print(x[column][1])
