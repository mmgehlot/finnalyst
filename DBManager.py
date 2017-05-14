import json
import mysql.connector
from mysql.connector import errorcode

class DBManager:
    def GetDatabaseConnection(self):
        try:
          cnx = mysql.connector.connect(user='<user-name',
                                        password ='<password>',
                                        host = '<host-address>',
                                        database = '<db-name>')
          return cnx

        except mysql.connector.Error as err:
          if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
          elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
          else:
            print(err)
        else:
          cnx.close()


def getList(db,tblName):
    temp = []
    con = db.GetDatabaseConnection()
    session = con.cursor()
    session.execute('select * from ' + tblName)

    for r in session:
        if(r[1] == None or r[2] == None or r[4] == None or r[3] == None):
            pass
        else:
            row = {"symbol" : r[1], "industry" : r[4], "name" : r[2], "sector" : r[3]}
            temp.append(row)
    # print(temp)
    temp =  [{"count" : str(len(temp)),"data" : temp}]
    return json.dumps(temp)
