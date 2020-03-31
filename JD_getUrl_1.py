import requests
from bs4 import BeautifulSoup
import conndb
import pandas as pd


def getUrl_1():
    url = 'https://list.jd.com/list.html?cat=1713,9291'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'
    }

    dit = conndb.exe_query(cur, 'SELECT * FROM JD_KIND')

    data = pd.DataFrame(dit, columns=['KID', 'KNAME', 'KLINK'])
    print(data)

    resp = requests.get(url, headers=headers)
    resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'lxml')

    ul = soup.find('ul', class_='menu-drop-list')
    for li in ul.find_all('li'):
        try:
            a = li.find('a')
            link = 'https://list.jd.com' + a.attrs['href']
            name = a.attrs['title']
            if not (link in data['KLINK']):
                sta = conndb.exe_updata(cur, "INSERT INTO JD_KIND(KNAME,KLINK) VALUES('%s','%s')" % (name, link))
                if sta == 1:
                    print(name + ' ' + link)
                else:
                    print('crawing failed')
            else:
                print(name + ' ' + link + ' already exist')
        except Exception as e:
            print(e)

def main():
    conn, cur = conndb.conn_db()

    getUrl_1()

    conndb.exe_commit(cur)
    conndb.conn_close(conn, cur)

if __name__ == '__main__':
    conn, cur = conndb.conn_db()

    getUrl_1()

    conndb.exe_commit(cur)
    conndb.conn_close(conn, cur)
