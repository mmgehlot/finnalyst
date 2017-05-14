import urllib, time, datetime
import json

class Quote(object):
    DATE_FMT = '%Y-%m-%d'
    TIME_FMT = '%H:%M:%S'

    def __init__(self):
        self.record = []
        self.jsonrecord = []
        self.symbol = ''
        self.exchange = ''
        self.date, self.time, self.open_, self.high, self.low, self.close, self.volume = ([] for _ in range(7))

    def append(self, dt, open_, high, low, close, volume):
        self.record.append((self.symbol, dt.date().strftime('%Y-%m-%d'), dt.time().strftime('%H-%M-%S'), str(float(open_)), str(float(high)), str(float(low)), str(float(close)), str(int(volume))))
        # print self.record

    def to_csv(self):
        r = []
        for x in self.record:
            r.append({"symbol": x[0], "date": x[1], "time": x[2], "open": x[3],
                      "high": x[4], "low": x[5], "close": x[6], "volume": x[7]})

        temp = json.dumps(r)
        return temp

    def __repr__(self):
        return self.to_csv()

class GoogleIntradayQuote(Quote):
    ''' Intraday quotes from Google. Specify interval seconds and number of days '''
    def __init__(self, symbol,exchange, interval_seconds=300, num_days=5):
        super(GoogleIntradayQuote, self).__init__()
        self.symbol = symbol.upper()
        self.exchange = exchange.upper()
        url_string = "http://www.google.com/finance/getprices?q={0}&x={1}".format(self.exchange,self.symbol)
        url_string += "&i={0}&p={1}d&f=d,o,h,l,c,v".format(interval_seconds, num_days)
        csv = urllib.urlopen(url_string).readlines()
        for bar in xrange(7, len(csv)):
            if csv[bar].count(',') != 5: continue
            offset, close, high, low, open_, volume = csv[bar].split(',')
            if offset[0] == 'a':
                day = float(offset[1:])
                offset = 0
            else:
                offset = float(offset)
            open_, high, low, close = [float(x) for x in [open_, high, low, close]]
            dt = datetime.datetime.fromtimestamp(day + (interval_seconds * offset))
            self.append(dt, open_, high, low, close, volume)
