import pymysql


def conn_db():
    conn = pymysql.connect(
        host='172.81.239.10',
        user='root',
        password='password',
        db='web',
        charset='utf8'
    )

    cur = conn.cursor()
    return conn, cur


def exe_update(cur, sql):
    reConnect(cur)
    sta = cur.execute(sql)
    return sta


def exe_write(book_Isbn, book_name, book_cbs, now_time, book_price, book_author, book_link, from_station, path):
    try:
        with open("./DangDangBook_BOOK_" + str(path) + ".txt", "a", encoding='utf-8') as f:
            f.write("'%s','%s','%s','%s','%s','%s','%s','%s'" % (
                book_Isbn, book_name, book_cbs, now_time, book_price, book_author,
                book_link, from_station))
            f.write('\n')
            f.close()
            return 1
    except Exception as e:
        return 0


def exe_query(cur, sql):
    reConnect(cur)
    cur.execute(sql)
    return cur


def exe_commit(cur):
    cur.connection.commit()


def conn_close(conn, cur):
    cur.close()
    conn.close()


def reConnect(cur):
    try:
        cur.connection.ping()
    except Exception as e:
        cur.connection()
