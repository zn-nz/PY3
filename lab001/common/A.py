
from fileinput import filename
import os.path
import re
import json
import pandas as pd
import time
from urllib.request import urlopen


sleep_time = 10
root_dir = 'F:\Self\A\py3\data{}'
all_stocks_path = os.path.join(root_dir.format('\data'), "all_stocks.csv")
# A股股票行情
stocksUrl = 'http://38.push2.eastmoney.com/api/qt/clist/get?cb=jQuery1124010265238627388706_1644058020401&pn={page}&pz={pageSize}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&fields=f12,f14&_=1644058020428'
stock_fs = {
    'SH': 'm:1+t:2,m:1+t:23',
    'SZ': 'm:0+t:6,m:0+t:80',
    'BJ': 'm:0+t:81+s:2048',
}


def get_year():
    return [2012, 2013, 2014, 2015, 2016,  2017, 2018, 2019, 2020, 2021]


def get_date():
    return ['03-31', '06-30', '09-30',  '12-31']


def get_date_list(floder_path, code):
    file_name = root_dir.format('{}\{}.csv'.format(floder_path, code))
    try:
        stock_data = pd.read_csv(file_name)['END_DATE'].tolist()
        return stock_data
    except Exception as e:
        print('未找到{}.csv文件'.format(code), e)
        return []


def get_gd_url(lt=False):
    route = 'PageSDGD'
    if(lt):
        route = 'PageSDLTGD'
    return 'https://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/'+route+'?code={code}&date={year}-{day}'


def get_all_stocks():
    try:
        df = pd.read_csv(all_stocks_path)['股票代码'].tolist()
        return df
    except:
        result = down_all_stocks()
        return result


def down_all_stocks():
    """
    A股所有股票代码导出
    :return:
    """
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
