from json.tool import main
import os
import pandas as pd


root_dir = 'F:\Self\A\py3\data\data'
all_stocks_path = os.path.join(root_dir, "all_stocks.csv")
a = os.listdir(root_dir)
temp = []
for stock in a:
    if(stock.startswith('sh') or stock.startswith('sz')):
        temp.append({'股票代码': stock.replace('.csv', '')})
df = pd.DataFrame(temp)
print(temp[0])
try:
    df.to_csv(all_stocks_path, index=False)
except Exception as e:
    print(e)
