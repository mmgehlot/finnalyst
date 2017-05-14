from DBManager import DBManager

db = DBManager()
cn = db.GetDatabaseConnection()

def getWatchList(userID):
    watchlist = []
    cur = cn.cursor()
    cur.execute("""select * from watchlist where username = %s""",(userID,))
    for r in cur:
        data = { "symbol" : r[1], "name" : r[2] }
        watchlist.append(data)

    cur.close()
    cn.close()
    return watchlist

def addWatchList(userID,companyID,companyName,stockindex):
    cur = cn.cursor()
    cur.execute("""insert into watchlist(userID,companyID,companyName,stockindex) values (%s,%s,%s,%s)""",
                (userID,companyID,companyName,stockindex))
    rowid = cur.lastrowid
    print(rowid)
    cn.commit()
    cur.close()
    cn.close()

def removeWatchList(userID,companyID):
    cur = cn.cursor()
    cur.execute("""delete from watchlist where userID = %s and companyID = %s """,
                (userID,companyID))
    rowid = cur.lastrowid
    print(rowid)
    cn.commit()
    cur.close()
    cn.close()
