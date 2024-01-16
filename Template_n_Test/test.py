import os


print(os.path.abspath(""))
parent_path = os.path.abspath(os.path.join(os.path.abspath(""), os.pardir))
download_path = f"{parent_path}/DMS_daily"
print(parent_path)
print(os.path.join(os.path.abspath(""), os.pardir))

download_path = f"{parent_path}/DMS_daily"

print(download_path)
