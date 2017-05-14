from DBManager import *
import urllib2
from threading import Thread
from threading import Event
from kafka import KafkaProducer

global latestStockPrice
latestStockPrice = []

def fetchTicker(index):
    global tickers
    tickers = []
    db = DBManager()

    # session = con.cursor()
    # session.execute('select symbol from ' + index)
    #
    # for row in session:
    #     tickers.append(row[1])
    #
    # count = session.execute('select count(symbol)as cn from ' + index)
    # tickerCount = count[0].cn - 1
    # # print(tickerCount)
    data = getList(db, 'nasdaq')
    data = json.loads(data)
    for r in data:
        for j in r["data"]:
            tickers.append(j["symbol"])
        obj = {"count": r["count"], "tickerList": tickers}
    return obj

class Nasdaq(Thread):

    def __init__(self, event):
        Thread.__init__(self)
        self.stopped = event

    def run(self):
        i = 0
        data = fetchTicker('nasdaq')
        tickerCount = int(data["count"])
        # tickers = data["tickerList"]
        remainder = tickerCount % 100
        StockPrice = []
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        # print(tickerCount)
        while not self.stopped.wait(5):
            if(i <= tickerCount):
                tickerlist = ",".join(tickers[ i : i + 100])
                tickerlist = tickerlist.replace(" ","")
                i = i + 100
            else:
                tickerlist = ",".join(tickers[tickerCount - remainder : remainder])
                tickerlist = tickerlist.replace(" ", "")

            # print(str(i) + " : " + tickerlist)
        try:
            response = urllib2.urlopen('http://www.google.com/finance/info?infotype=infoquoteall&q=NASDAQ:'+ tickerlist)
            page = response.read()
            l = len(page) - 2
            data = page[5:l]
            data = data.replace("\n","")
            data = "[" + data + "]"
            data = json.loads(data)
            producer.send("nasdaq",data)
            # print data
            # StockPrice.append(data + ",")

            if(i > tickerCount):
                i = 0
                # print(StockPrice)
                # r = str(StockPrice)
                # r = r[2:len(r) - 3]
                # r = "[" + r + "]"
                # # producer.send("nasdaq",r)
                # # json.dumps(r,indent=2)
                StockPrice = []
        except urllib2.HTTPError:
            pass

class Nyse(Thread):
    def __init__(self, event):
        Thread.__init__(self)
        self.stopped = event

    def run(self):
        i = 0
        data = fetchTicker('nyse')
        tickers = data["tickerList"]
        # print tickers
        tickerCount  = data["count"]
        # print tickerCount
        remainder = int(tickerCount) % 100
        # StockPrice = []
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        while not self.stopped.wait(5):
            if(i <= tickerCount):
                tickerlist = ",".join(tickers[ i : i + 100])
                tickerlist = tickerlist.replace(" ","")
                i = i + 100
            else:
                tickerlist = ",".join(tickers[tickerCount - remainder : remainder])
                tickerlist = tickerlist.replace(" ", "")

            # print(str(i) + " : " + tickerlist)
            try:
                response = urllib2.urlopen('http://www.google.com/finance/info?infotype=infoquoteall&q=NYSE'+ tickerlist)
                page = response.read()
                l = len(page) - 2
                data = page[5:l]
                data = data.replace("\n","")
                data = "[" + data + "]"
                data = json.loads(data)
                producer.send("nyse",data)
                # print data
                # StockPrice.append(data + ",")

                if(i > tickerCount):
                    i = 0
                    # print(StockPrice)
                    # r = str(StockPrice)
                    # r = r[2:len(r) - 3]
                    # r = "[" + r + "]"
                    # print(r)
                    StockPrice = []
            except urllib2.HTTPError:
                pass



def main():
    # LatestStockPrice('nasdaq')
    stopFlagNasdaq = Event()
    stopFlagNyse = Event()

    thread = Nasdaq(stopFlagNasdaq)
    thread.start()

    thread = Nyse(stopFlagNyse)
    thread.start()

if __name__ == '__main__':
    main()