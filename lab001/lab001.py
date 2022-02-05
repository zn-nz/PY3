import json
import os.path
import re
import pandas as pd
import time
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
root_dir = 'F:\Self\A\py3\data'
all_stocks_path = os.path.join(root_dir, "all_stocks.csv")
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
}
# 沪深京三板
stock_fs = {
    'SH': 'm:1+t:2,m:1+t:23',
    'SZ': 'm:0+t:6,m:0+t:80',
    'BJ': 'm:0+t:81+s:2048',

}
# A股股票行情
stocksUrl = 'http://38.push2.eastmoney.com/api/qt/clist/get?cb=jQuery1124010265238627388706_1644058020401&pn={page}&pz={pageSize}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&fields=f12,f14&_=1644058020428'

# 十大股东
sdgdUrl = 'https://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/PageSDLTGD?code={code}&date={year}-{day}'
sleep_time = 10


def format_stock_code(code, code_type=''):
    """
    股票代码format
    默认是600300
    baostock是sh.600300
    tushare是000001.SZ
    with_market是sh600300

    :param code:
    :param code_type:
    :return:
    """

    matchObj = re.findall(r'[0-9]+', code)
    if len(matchObj) == 0:
        raise ValueError('code格式错误，不包含任何数字')
    if len(matchObj[0]) != 6:
        raise ValueError('code格式错误，不是6位数字:{}'.format(matchObj[0]))

    code = matchObj[0]

    region = "bj"
    if code.startswith('3') or code.startswith('0'):
        region = "sz"
    elif code.startswith('6'):
        region = "sh"

    if code_type == 'baostock':
        code = region + "." + code
    elif code_type == 'tushare':
        code = code + "." + region.upper()
    elif code_type == "with_market":
        code = region.lower() + code

    return code


def down_all_stocks():
    page = 1
    pageSize = 500
    total = 3000
    result = []
    while(len(result) < total):
        try:
            new_rul = stocksUrl.format(page=page, pageSize=pageSize)
            content = urlopen(new_rul).read().decode(encoding="utf-8")
            data = re.sub(r'^.+\(|\)\;$', '', content)
            opt = json.loads(data)['data']
            total = opt['total']
            temp = opt['diff']
            result.extend(temp)
            page += 1
            print('已拉取', len(result), '个股票代码')
        except Exception as e:
            print('抓取数据报错，页码：', page,  '报错内容：', e)
        time.sleep(sleep_time)

    df = pd.DataFrame(result)
    df.rename(columns={'f12': '股票代码', 'f14': '股票名称'}, inplace=True)
    df['股票代码'] = df['股票代码'].apply(format_stock_code, code_type="with_market")
    df.to_csv(all_stocks_path, index=False)
    return result


def get_all_stocks():
    try:
        df = pd.read_csv(all_stocks_path).values.tolist()
        return df
    except:
        result = down_all_stocks()
        return result


def get_sdgd(all_stocks=[['sh601800', '中国交建']]):
    result = []
    years = [2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012]
    days = ['12-31',  '09-30', '06-30', '03-31']
    stock_index = 0
    year_index = 0
    day_index = 0
    while(stock_index < len(all_stocks)):
        while(year_index < len(years)):
            while(day_index < len(days)):
                try:
                    code = all_stocks[stock_index][0]
                    year = years[year_index]
                    day = days[day_index]
                    new_url = sdgdUrl.format(code=code, year=year, day=day)
                    print(new_url)
                    content = urlopen(new_url).read().decode(encoding="utf-8")
                    sdgd_data = json.loads(content)['sdltgd']
                    print(content)
                    print(sdgd_data)
                    print('已拉取股票代码',  code, year, day)
                    day_index += 1
                except Exception as e:
                    print('抓取数据报错：', code, year, day, '报错内容：', e)
                time.sleep(sleep_time/2)

            year_index += 1
            day_index = 0

        stock_index += 1
        year_index = 0


def main():
    allStocks = get_all_stocks()
    get_sdgd(allStocks)


main()
