import csv
import glob

unique_rows = set()
path = "*.csv"
all_files = glob.glob(path)

for file in all_files:
    with open(file, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            unique_rows.add(tuple(row))
            
print(len(unique_rows))
