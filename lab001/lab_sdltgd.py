import json
import os.path
import pandas as pd
import time
from urllib.request import urlopen

from common.A import get_all_stocks, get_gd_url, get_date, get_year, get_date_list


root_dir = 'F:\Self\A\py3\data{}'
floder_path = '\sdltgd'
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
}

# 十大流通股东
sdltgdUrl = get_gd_url(True)
sleep_time = 10


def get_sdgd(all_stocks=['sh600000'], start_stock=False):
    years = get_year()
    days = get_date()
    stock_index = 0
    year_index = 0
    day_index = 0
    if(start_stock):
        stock_index = all_stocks.index(start_stock)

    while(stock_index < len(all_stocks)):
        code = all_stocks[stock_index]
        scv_path = os.path.join(root_dir.format(
            floder_path), "{}.csv".format(code))
        stock_data = get_date_list(floder_path, code)
        mode = 'a'
        header = False
        if(len(stock_data) == 0):
            mode = 'w'
            header = True

        while(year_index < len(years)):
            year = years[year_index]

            while(day_index < len(days)):
                day = days[day_index]

                if('{}-{} 00:00:00'.format(year, day) in stock_data):
                    print('{}-{}已爬取过{}'.format(year, day, code))
                    day_index += 1
                else:
                    try:
                        new_url = sdltgdUrl.format(
                            code=code, year=year, day=day)
                        print(new_url)
                        content = urlopen(new_url).read().decode(
                            encoding="utf-8")
                        day_index += 1
                        sdgd_data = json.loads(content)['sdltgd']
                        print('已拉取【{}】{}-{}，{}条信息'.format(code,
                                                          year, day, len(sdgd_data)))
                        if (len(sdgd_data) > 0):
                            df = pd.DataFrame(sdgd_data)
                            df.to_csv(scv_path, mode=mode,
                                      header=header, index=False)
                            print('写入', scv_path)
                            mode = 'a'
                            header = False

                    except Exception as e:
                        print('抓取数据报错：', code, year, day, '报错内容：', e)
                    time.sleep(sleep_time/3)

            year_index += 1
            day_index = 0

        stock_index += 1
        year_index = 0


def main():
    allStocks = get_all_stocks()
    start_stock = 'sh600567'
    get_sdgd(allStocks, start_stock)


main()
