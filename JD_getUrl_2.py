import requests
from bs4 import BeautifulSoup
import pandas as pd
import conndb
import datetime
import time
import json

dit = []
data = []
now_time = datetime.datetime.now().strftime('%Y-%m')
from_station = 'jd'


def getUrl_2(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'
        }

        resp = requests.get(url, headers=headers)
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'lxml')

        maxPage = getMaxPage(url)

        for i in range(1, maxPage + 1):
            link = url + '&page=' + str(i)

            resp_menu = requests.get(link, headers=headers)
            resp_menu.encoding = 'utf-8'
            soup = BeautifulSoup(resp_menu.text, 'lxml')

            ul = soup.find('ul', class_='gl-warp clearfix')

            for li in ul.find_all('li'):
                div = li.find('div', class_='p-img')
                book_link = 'https:' + div.find('a').attrs['href']
                write('----------------------------------------')
                write(url+' '+book_link+' '+str(i))
                getInfoByLink(book_link)
    except ConnectionError as conn_e:
        write('conn_error ----------- sleeping')
        time.sleep(60 * 10)
        write('--------------return crawing-------------')
    except Exception as e:
        write(e)


def getInfoByLink(book_link):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'
        }
        book_resp = requests.get(book_link, headers=headers)
        book_resp.encoding = 'gbk'
        soup = BeautifulSoup(book_resp.text, 'lxml')
        if soup.find('div', class_='p-author').find('a') is not None:
            book_author = soup.find('div', class_='p-author').find('a').text.replace('\n', '').replace(' ', '')
        else:
            book_author = ''
        if soup.find('div', class_='sku-name') is not None:
            book_name = soup.find('div', class_='sku-name').text.replace('\n', '').replace(' ', '').replace('\'','\\\'')
        else:
            book_name = ''
        ul = soup.find('ul', class_='p-parameter-list')
        book_Isbn = ''
        book_cbs = ''
        book_id = ''
        for li in ul.find_all('li'):
            text = li.text
            if 'ISBN' in text:
                book_Isbn = text.replace('ISBN', '').replace('\n', '').replace(' ', '').replace(':', '').replace('：','')
            if '出版社' in text:
                book_cbs = text.replace('出版社', '').replace('\n', '').replace(' ', '').replace(':', '').replace('：', '').replace('\'','\\\'')
            if '商品编码' in text:
                book_id = text.replace('商品编码', '').replace(' ', '').replace('\n', '').replace(':', '').replace('：', '')
        book_price = getBookPrice(book_id)

        if not (book_link in data['BLINK'].values):
            sta = conndb.exe_update(cur,
                                    "INSERT INTO JD_BOOK(ISBN, BNAME,BPUBLISHER,UPLOADTIME,BPRICE,BAUTHOR,BLINK,FROMSTATION) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                        book_Isbn, book_name, book_cbs, now_time, book_price, book_author,
                                        book_link, from_station))
            conndb.exe_commit(cur)
            if sta == 1:
                write(
                    book_name + ' ' + book_price + ' ' + book_author + ' ' + book_cbs + ' ' + book_Isbn + ' ' + now_time)
                write('------------------------------------')
            else:
                write('crawing failed')
        else:
            write(book_link + ' already exist')
    except Exception as e:
        write(e)
        return


def getMaxPage(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'
    }

    resp = requests.get(url, headers=headers)
    resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'lxml')

    span = soup.find('span', class_='p-skip')
    max_page = span.find('em').find('b').text
    write(max_page)
    return int(max_page)


def getBookPrice(book_id):
    try:
        url = 'https://p.3.cn/prices/mgets'
        headers = {
            'skuids': 'J_' + str(book_id)
        }
        jsContent = requests.get(url, headers).text
        d = json.loads(jsContent)
        return d[0]['p']
    except Exception as e:
        return ''


def write(word):
    with open("./JDBook_log.txt", "a", encoding='utf-8') as f:
        f.write(str(word))
        f.write('\n')
        f.close()
    # print(word)


if __name__ == '__main__':
    write('---------start JD_getUrl_2.py at' + str(datetime.datetime.now()) + '------------------')

    conn, cur = conndb.conn_db()

    dit = conndb.exe_query(cur, "SELECT * FROM JD_BOOK WHERE UPLOADTIME = '%s'" % (now_time))

    data = pd.DataFrame(dit, columns=['BID', 'ISBN', 'BNAME', 'BPUBLISHER', 'UPLOADTIME', 'BPRICE', 'BAUTHOR', 'BLINK',
                                      'FROMSTATION'])

    linkData = pd.read_csv('./JD_KIND.csv')
    for index, row in linkData.iterrows():
        print(str(index) + ' ' + str(row['KID']) + ' ' + row['KNAME'] + ' ' + row['KLINK'])
        if index < 22:
            continue
        write(str(index) + ' ' + str(row['KID']) + ' ' + row['KNAME'] + ' ' + row['KLINK'])
        getUrl_2(row['KLINK'])
        write('------------')
    getInfoByLink('https://item.jd.com/12256911.html')
    conndb.exe_commit(cur)
    conndb.conn_close(conn, cur)

    write('---------finish JD_getUrl_2.py at' + str(datetime.datetime.now()) + '------------------')
