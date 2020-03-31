import datetime

import requests
from bs4 import BeautifulSoup
import conndb
import pandas as pd



def getUrl_1():
    url = 'http://book.dangdang.com/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'
    }

    resp = requests.get(url, headers=headers)
    resp.encoding = 'GB2312'
    soup = BeautifulSoup(resp.text, 'lxml')

    div = soup.find('div', class_='con flq_body')

    dit = conndb.exe_query(cur, 'SELECT * FROM DANGDANG_KIND')

    data = pd.DataFrame(dit, columns=['KID', 'KNAME', 'KLINK'])
    # print(data)
    for a in div.find_all('a'):
        try:
            link = a.attrs['href']
            name = a.text.replace(' ', '').replace('\n', '')

            if 'http://category.dangdang.com/' in link:
                if not (link in data['KLINK'].values):
                    sta = conndb.exe_update(cur,
                                            "INSERT INTO DANGDANG_KIND(KNAME,KLINK) VALUES('%s','%s')" % (name, link))
                    if sta == 1:
                        write(name + ' ' + link)
                    else:
                        write('crawing failed')
                else:
                    write(name+' '+link + ' already exist')

        except Exception as e:
            write(e)


def main():
    conn, cur = conndb.conn_db()

    getUrl_1()

    conndb.exe_commit(cur)
    conndb.conn_close(conn, cur)

def write(word):
    with open("./DangDangKind_log.txt", "a", encoding='utf-8') as f:
        f.write(word)
        f.write('\n')
        f.close()

if __name__ == '__main__':
    write('---------start DangDang_getUrl_1.py at' + str(datetime.datetime.now()) + '------------------')
    conn, cur = conndb.conn_db()

    getUrl_1()
    sta=conndb.exe_query(cur,"SELECT * FROM DANGDANG_KIND")
    if not sta:
        write(1)
    for row in sta:
        print(row)
    conndb.exe_commit(cur)
    conndb.conn_close(conn, cur)

    write('---------finish DangDang_getUrl_1.py at' + str(datetime.datetime.now()) + '------------------')
