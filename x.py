import pendulum
from datetime import datetime, timezone
import pytz
import os
import datetime as dt

print(pendulum.datetime(2023, 3, 31, 2, 30, 0, 0, tz="Asia/Bangkok"))
noww = dt.datetime.now(pytz.timezone("Asia/Bangkok")).date().strftime(format="%Y%m%d")
download_path = "/home/nhdminh/airflow/DMSdownload"
print(noww)
try:
    os.mkdir(download_path + "/" + noww)
except Exception as e:
    print(e)
    pass
