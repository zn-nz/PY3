import os.path
import re
import pandas as pd
import time
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup

root_dir = "/Users/li/Workspace/github.com/quant-py/lab/lab045/temp"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:95.0) Gecko/20100101 Firefox/95.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
}
sleep_time = 5


def crawl_index(index_code):
    """
    :param index_code:
    :return:
    """
    # 获取最新成分页数
    total_page = crawl_index_new(index_code, page=1)
    print("new_url total_page={}".format(total_page))

    for page in range(2, total_page + 1):
        crawl_index_new(index_code, page=page)
        print("new_url finish[{}/{}]".format(page, total_page))
        time.sleep(sleep_time)

    # 获取历史成分页数
    total_page = crawl_index_history(index_code, page=1)
    print("history_url total_page={}".format(total_page))
    for page in range(2, total_page + 1):
        crawl_index_history(index_code, page=page)
        print("history_url finish[{}/{}]".format(page, total_page))
        time.sleep(sleep_time)


def crawl_index_new(index_code, page):
    """
    最新成分
    http://stock.jrj.com/share,sh000016,nzxcf.shtml
    http://stock.jrj.com/share,sh000016,nzxcf_2.shtml
    :param index_code:
    :param page: 页码
    :return:
    """

    if page == 1:
        new_url = "http://stock.jrj.com/share,{},nzxcf.shtml".format(
            index_code)
    else:
        new_url = "http://stock.jrj.com/share,{},nzxcf_{}.shtml".format(
            index_code, page)

    content = get_content_from_internet(new_url, headers)
    content = content.decode("gbk")

    save_dir = os.path.join(root_dir, "html", index_code, "new")
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    filepath = os.path.join(save_dir, "{}.html".format(page))
    with open(filepath, 'wt', encoding='utf-8') as f:
        f.write(content)

    # 爬取第一页的时候返回总页码
    if page == 1:
        soup = BeautifulSoup(content, 'lxml')

        total_page = int(soup.select(
            "body > div.body > div.warp > div.main > table > tbody > tr > td.m > div > div > p > a:nth-last-child(2)")[
            0].text)
        return total_page


def crawl_index_history(index_code, page):
    """
    历史成分
    http://stock.jrj.com/share,sh000016,nlscf.shtml
    http://stock.jrj.com/share,sh000016,nlscf_2.shtml
    :param index_code:
    :param page:
    :return:
    """
    if page == 1:
        new_url = "http://stock.jrj.com/share,{},nlscf.shtml".format(
            index_code)
    else:
        new_url = "http://stock.jrj.com/share,{},nlscf_{}.shtml".format(
            index_code, page)

    content = get_content_from_internet(new_url, headers)
    content = content.decode("gbk")

    save_dir = os.path.join(root_dir, "html", index_code, "history")
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    filepath = os.path.join(save_dir, "{}.html".format(page))
    with open(filepath, 'wt', encoding='utf-8') as f:
        f.write(content)

    # 爬取第一页的时候返回总页码
    if page == 1:
        soup = BeautifulSoup(content, 'lxml')

        total_page = int(soup.select(
            "body > div.body > div.warp > div.main > table > tbody > tr > td.m > div > div > p > a:nth-last-child(2)")[
            0].text)
        return total_page


def parse_index(index_code):
    """
    解析html，生成csv
    :param index_code:
    :return:
    """
    new_dir = os.path.join(root_dir, "html", index_code, "new")
    filepaths = [os.path.join(new_dir, x) for x in os.listdir(new_dir)]

    stocks = []
    for filepath in filepaths:
        stocks.extend(parse_index_new(filepath))

    history_dir = os.path.join(root_dir, "html", index_code, "history")
    filepaths = [os.path.join(history_dir, x) for x in os.listdir(history_dir)]
    for filepath in filepaths:
        stocks.extend(parse_index_history(filepath))

    df = pd.DataFrame(stocks)
    df.rename(columns={"stock_code": "股票代码",
              "in_date": "纳入时间", "out_date": "剔除时间"}, inplace=True)
    df['股票代码'] = df['股票代码'].apply(format_stock_code, code_type="with_market")

    save_dir = os.path.join(root_dir, "csv")
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    filepath = os.path.join(save_dir, "{}.csv".format(index_code))
    df.to_csv(filepath, index=False)


def parse_index_new(filepath):
    """
    解析最新成分
    :param filepath:
    :return:
    """
    with open(filepath, 'rt', encoding='utf-8') as f:
        data = f.read()

    soup = BeautifulSoup(data, 'lxml')

    stocks = []
    for s in soup.select("#contenttable > tbody > tr:nth-child(n+2)"):
        stock_code = s.select("td:nth-child(1)")[0].text
        in_date = s.select("td:nth-child(3)")[0].text

        stocks.append({"stock_code": stock_code, "in_date": in_date})

    return stocks


def parse_index_history(filepath):
    """
    解析历史成分
    :param filepath:
    :return:
    """
    with open(filepath, 'rt', encoding='utf-8') as f:
        data = f.read()

    soup = BeautifulSoup(data, 'lxml')

    stocks = []
    for s in soup.select("#contenttable > tbody > tr:nth-child(n+2)"):
        stock_code = s.select("td:nth-child(1)")[0].text
        in_date = s.select("td:nth-child(3)")[0].text
        out_date = s.select("td:nth-child(4)")[0].text

        stocks.append({"stock_code": stock_code,
                      "in_date": in_date, "out_date": out_date})

    return stocks


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


def get_content_from_internet(url, headers=None, max_try_num=10, sleep_time=5):
    """
    使用python自带的urlopen函数，从网页上抓取数据
    :param url: 要抓取数据的网址
    :param max_try_num: 最多尝试抓取次数
    :param sleep_time: 抓取失败后停顿的时间
    :return: 返回抓取到的网页内容
    """
    get_success = False  # 是否成功抓取到内容
    # 抓取内容
    for i in range(max_try_num):
        try:
            if headers is not None:
                req = Request(url=url, headers=headers)
            else:
                req = Request(url=url)
            content = urlopen(req).read()

            get_success = True
            break
        except Exception as e:
            print('抓取数据报错，次数：', i + 1, '报错内容：', e)
            time.sleep(sleep_time)

    # 判断是否成功抓取内容
    if get_success:
        return content
    else:
        raise ValueError('使用urlopen抓取网页数据不断报错，达到尝试上限，停止程序，请尽快检查问题所在')


def main():
    index_codes = ["sh000016", "sh000906"]
    index_codes = ["sh000016"]

    for index_code in index_codes:
        crawl_index(index_code)
        parse_index(index_code)


if __name__ == '__main__':
    main()
