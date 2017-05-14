
import tornado.ioloop
import socket
import tornado.web
import urllib2
import logging
import json
# from cassandra.cluster import Cluster
from threading import Timer
from kafka import KafkaProducer

rcount=0
i=0

class StockMarket:
    tickerlist = ""

    def __init__(self):
        self.result = []
        self.tickers = []

    # def fetchTicker(self):
    #     session = self.GetDatabaseConnection('finnalyst')
    #     # producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))#bootstrap_servers=['localhost:9092'])
    #
    #     temp = session.execute('select symbol from nasdaq')
    #     resultset = temp
    #     tempResultSet = temp
    #
    #     for row in tempResultSet:
    #         self.tickers.append(row[0])
    #
    #     self.tickerlist = ",".join(self.tickers[1:100])
    #     #print(self.tickerlist)
    #
    #     rcount = session.execute('select count(symbol)as cn from nasdaq')
    #
    #     rowCount = rcount
    #     recCount = rowCount[0].cn - 1

    # def GetDatabaseConnection(self,keyspace):
    #     # cassandra_cluster = Cluster()
    #     return cassandra_cluster.connect(keyspace)

    def getMarketData(self,ind):
        # self.fetchTicker()
        #print(ind, tickerlist)
        res = urllib2.urlopen('http://finance.google.com/finance/info?client=ig&q=' + ind + ',' + 'GOOGL')
        # print(data)
        # for d in data:
        #     print(d["id"])
        return res

    def startMarket(self,index):
        dataSet = self.getMarketData(index)#resultset[i].symbol)
        if(dataSet != None):
            for d in dataSet:
                data = d
                res = json.dumps({"id" : data['id'], "ticker" : data['t'], "index" : data['e'],
                            "lt_price" : data['l_cur'], "lt_time" : data['ltt'],
                            "lt_date" : data['lt_dts'], "CinPrice" : data['c'], "CinPer" : data['cp']})
                #producer.send('nasdaq',res)
                self.result.append(res)
        return self.result

