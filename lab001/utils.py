from json.tool import main
import os
import numpy as np
import pandas as pd


def fun1():
    # 汇总需要爬取的股票代码
    root_dir = 'F:/Self/A/py3/data/data'
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


def fun2():
    # 股东按时间及排名排序
    root_dir = 'F:/Self/A/py3/data/sdltgd/'
    file_list = os.listdir(root_dir)
    for file in file_list:
        try:
            path = os.path.join(root_dir, file)
            print(path)
            df = pd.read_csv(path)
            data = df.sort_values(by=['END_DATE', 'HOLDER_RANK'])
            result = pd.DataFrame(data)
            result.to_csv(path, index=False)
        except Exception as e:
            print(e)


fun2()
