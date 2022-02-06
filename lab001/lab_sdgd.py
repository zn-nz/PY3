import json
import os.path
import pandas as pd
import time
from urllib.request import urlopen

from common.A import get_all_stocks, get_gd_url


root_dir = 'F:\Self\A\py3\data{}'
all_stocks_path = os.path.join(root_dir, "all_stocks.csv")
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
}
# A股股票行情
stocksUrl = 'http://38.push2.eastmoney.com/api/qt/clist/get?cb=jQuery1124010265238627388706_1644058020401&pn={page}&pz={pageSize}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&fields=f12,f14&_=1644058020428'

# 十大股东
sdgdUrl = get_gd_url()
sleep_time = 10


def get_sdgd(all_stocks=[['sh600036', '中国交建']]):
    years = [2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012]
    days = ['12-31',  '09-30', '06-30', '03-31']
    stock_index = 0
    year_index = 0
    day_index = 0
    while(stock_index < len(all_stocks)):
        temp_result = []
        code = all_stocks[stock_index][0]

        while(year_index < len(years)):
            year = years[year_index]

            while(day_index < len(days)):
                day = days[day_index]
                try:
                    new_url = sdgdUrl.format(code=code, year=year, day=day)
                    print(new_url)
                    content = urlopen(new_url).read().decode(encoding="utf-8")
                    day_index += 1
                    sdgd_data = json.loads(content)['sdgd']
                    if (len(sdgd_data) > 0):
                        temp_result.extend(sdgd_data)
                    print('已拉取【{}】{}-{}，{}条信息'.format(code,
                          year, day, len(sdgd_data)))
                except Exception as e:
                    print('抓取数据报错：', code, year, day, '报错内容：', e)
                time.sleep(sleep_time/2)

            year_index += 1
            day_index = 0

        df = pd.DataFrame(temp_result)
        temp = os.path.join(root_dir.format('/sdgd'), "{}.csv".format(code))
        df.to_csv(temp, index=False)

        stock_index += 1
        year_index = 0


def main():
    allStocks = get_all_stocks()
    get_sdgd(allStocks)


main()
