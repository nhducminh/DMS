import os


def listfilename(folder):
    flist =[]
    for f in os.listdir(folder):
        if os.path.isdir(f"{folder}/{f}"):
            flist = flist + listfilename(f"{folder}/{f}")
        else:
            flist.append(f)
    return flist




print(os.path.abspath(""))
parent_path = os.path.abspath(os.path.join(os.path.abspath(""), os.pardir))
download_path = f"{os.path.abspath('')}/DMS_sharepoint/"

print(download_path)
filelist = listfilename(download_path)
print(filelist)