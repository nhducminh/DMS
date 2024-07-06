import pandas as pd

flist1 = pd.read_csv('flist.csv')['0'].to_list()
flist2 = pd.read_csv('fnamelist.csv')['0'].to_list()


cp_flist1 = flist1.copy()
print(len(cp_flist1))
for f1 in flist1:
    if f1[str.rfind(f1, "/") + 1 :] in flist2:
        print(f1)
        
        cp_flist1.pop(cp_flist1.index(f1))
print(len(cp_flist1))


cp_flist2 = flist2.copy()
print(len(cp_flist2))
for f2 in flist2:
    if f2[str.rfind(f2, "/") + 1 :] in flist2:
        try:
            cp_flist2.pop(cp_flist1.index(f2))
        except:
            pass
print(len(cp_flist2))