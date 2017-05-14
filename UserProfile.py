from DBManager import DBManager
from datetime import datetime

db = DBManager()
cn = db.GetDatabaseConnection()

def createUser(username,password,emailid):
    cur = cn.cursor()
    dt = datetime.now()
    cur.execute("""insert into user(username, password, emailid, dateCreated) VALUES (%s,%s,%s,%s)""",
                (str(username), str(password), str(emailid), dt))
    rowid = cur.lastrowid
    cn.commit()
    cur.close()

