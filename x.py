import pendulum
from datetime import datetime, timezone
import pytz

print(pendulum.datetime(2023, 3, 31, 2, 30, 0, 0, tz="Asia/Bangkok"))
noww = datetime.now(pytz.timezone("Asia/Bangkok")).date()
print(noww)
