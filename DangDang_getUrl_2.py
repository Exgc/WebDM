import requests
from bs4 import BeautifulSoup
import pandas as pd
import conndb
import datetime
import time

dit = []
data = []
now_time = datetime.datetime.now().strftime('%Y-%m')
from_station = 'dangdang'


def getUrl_2(url):
    try:

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'
        }

        resp = requests.get(url, headers=headers)
        resp.encoding = 'GB2312'
        soup = BeautifulSoup(resp.text, 'lxml')

        maxPage = getMaxPage(url)
        write(maxPage)

        for i in range(1, maxPage + 1):
            # for i in range(1,2):
            link = 'http://category.dangdang.com/' + url.replace('http://category.dangdang.com/', 'pg' + str(i) + '-')

            resp_menu = requests.get(link, headers=headers)
            resp.encoding = 'GB2312'
            soup = BeautifulSoup(resp_menu.text, 'lxml')

            ul = soup.find('ul', class_='bigimg')

            for li in ul.find_all('li'):
                a = li.find('a', class_='pic')
                book_link = a.attrs['href']
                book_resp = requests.get(book_link, headers=headers)
                resp.encoding = 'GB2312'
                soup = BeautifulSoup(book_resp.text, 'lxml')
                write('-----------------------------------------')
                write(url + '      ' + book_link + '       ' + str(i))
                try:
                    if li.find('p', class_='name') != '':
                        book_name = li.find('p', class_='name').text.replace('\'', '\\\'')
                    else:
                        book_name = ''
                    # print(book_name)

                    book_price = li.find('p', class_='price').find('span', class_='search_now_price').text
                    book_info = li.find('p', class_='search_book_author')
                    if book_info.find('a', dd_name='单品作者') is None:
                        book_author = ''
                    else:
                        book_author = book_info.find('a', dd_name='单品作者').attrs['title'].replace('\'', '\\\'')
                    # print(book_author)

                    if book_info.find('a', dd_name='单品出版社') is None:
                        book_cbs = ''
                    else:
                        book_cbs = book_info.find('a', dd_name='单品出版社').attrs['title'].replace('\'', '\\\'')
                    # print(book_cbs)
                    book_Isbn = getIsbn(book_link)
                    # print(book_Isbn)
                    write(
                        book_link + ' ' + book_name + ' ' + book_author + ' ' + book_price + ' ' + book_Isbn + ' ' + book_cbs)
                    if not (book_link in data['BLINK'].values):
                        sta=conndb.exe_write(book_Isbn, book_name, book_cbs, now_time, book_price, book_author,
                                                    book_link, from_station,1)
                        if sta == 1:
                            write(
                                book_name + ' ' + book_price + ' ' + book_author + ' ' + book_cbs + ' ' + book_Isbn + ' ' + now_time)
                        else:
                            write('crawing failed')
                    else:
                        write(book_link + ' already exist')
                except ConnectionError as conn_e:
                    write('conn_error ----------- sleeping')
                    time.sleep(60 * 10)
                    write('--------------return crawing-------------')
                except Exception as e:
                    write(e)
    except Exception as e:
        write(e)


def getMaxPage(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'
    }

    resp = requests.get(url, headers=headers)
    resp.encoding = 'GB2312'
    soup = BeautifulSoup(resp.text, 'lxml')

    ul = soup.find('ul', dd_name='底部翻页')

    max_page = 1
    if ul is not None:
        for li in ul.find_all('li'):
            try:
                max_page = max(int(li.text), max_page)
            except Exception as e:
                continue

    return max_page


def getIsbn(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'
    }

    resp = requests.get(url, headers=headers)
    resp.encoding = 'GB2312'
    soup = BeautifulSoup(resp.text, 'lxml')
    ul = soup.find('ul', class_='key clearfix')
    if ul is None:
        # print(soup)
        print(url)
        print('-------------------------------')
    else:
        for li in ul.find_all('li'):
            if "国际标准书号ISBN" in li.text:
                return li.text.replace('国际标准书号ISBN', '').replace(' ', '').replace(':', '').replace('：', '')


def write(word):
    with open("./DangDangBook_log.txt", "a", encoding='utf-8') as f:
        f.write(str(word))
        f.write('\n')
        f.close()
    # print(word)


if __name__ == '__main__':
    write('---------start DangDang_getUrl_2.py at' + str(datetime.datetime.now()) + '------------------')

    conn, cur = conndb.conn_db()
    dit = conndb.exe_query(cur,
                           "SELECT * FROM DANGDANG_BOOK WHERE UPLOADTIME = '%s'" % (
                               now_time))
    print(1)
    data = pd.DataFrame(dit, columns=['BID', 'ISBN', 'BNAME', 'BPUBLISHER', 'UPLOADTIME', 'BPRICE',
                                      'BAUTHOR', 'BLINK',
                                      'FROMSTATION'])
    linkData = pd.read_csv('./BOOK_KIND.csv')
    # print(linkData)
    for index, row in linkData.iterrows():
        print(str(index) + '  ' + str(row['KID']) + ' ' + row['KNAME'] + ' ' + row['KLINK'])
        if index < 73 or index >= 100:
            continue
        write(str(index) + ' ' + str(row['KID']) + ' ' + row['KNAME'] + ' ' + row['KLINK'])
        getUrl_2(row['KLINK'])
        write('------------')
    # getUrl_2('http://category.dangdang.com/cp01.10.03.00.00.00.html')
    conndb.conn_close(conn, cur)

    write('---------finish DangDang_getUrl_2.py at' + str(datetime.datetime.now()) + '------------------')
