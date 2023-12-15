import requests
import pandas as pd
import datetime as dt
noww = dt.date.today()
print(noww)
# API endpoint and parameters
for x in range (1,5):
    url = 'https://api-production.2nong.vn/v0/products'
    params = {
        'page': x,
        'is_active': 1,
        'limit': 20,
        'special_id': 2,
        'sort[updated_at]': 'desc',
        'name': '',
        'city_id': '',
        'date': ''
    }

    # Fetching data from the API
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        
        # Flatten the 'info' field if it contains nested data
        df = pd.json_normalize(data['data'], sep='_')
        
        print(df)  # Displaying the first few rows of the DataFrame
    else:
        print("Failed to fetch data from the API")


print(len(df))
df.to_csv("2nongxxxxxxx.csv",index=False)


dfList = df.explode('info').reset_index(drop=True)
# dfList = dfList.explode('info').reset_index(drop=True)
temp = pd.DataFrame(dfList['info'].values.tolist())
# temp.to_csv("temp.csv",index=False)
dfList = dfList.drop(columns='info', axis=1).join(temp,rsuffix="_temp")
dfList.to_csv(f"2nong/{noww}.csv",index=False)
print(list(dfList))
print(len(dfList))