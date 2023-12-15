import os
import re

report_folder = "/home/nhdminh/airflow/DMS_daily"
pattern = "^\d+$"

for f in sorted(os.listdir(report_folder)):
    if len(re.findall(pattern, f)) > 0:
        ff = f

print(ff)
