from __future__ import absolute_import, print_function
import json
import collections
import threading
import urllib2
import os
import re
from datetime import datetime
from DBManager import DBManager
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer
import tornado.websocket
import tornado.web
from tweepy import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler

removal_list = ['\\','/',',','(',')','!',':','.','#','']
stop = stopwords.words('english')

message_history = collections.deque([], maxlen=100)

class TweetWebSocket(tornado.websocket.WebSocketHandler):
    # Keep track of open sockets globally, so that we can
    # communicate with them conveniently.
    open_sockets = set()

    @classmethod
    def write_to_all(cls, message):
        removable = set()
        # print(message)
        for ws in cls.open_sockets:
            if not ws.ws_connection or not ws.ws_connection.stream.socket:
                removable.add(ws)
            else:
                ws.write_message(message)

        for ws in removable:
            cls.open_sockets.remove(ws)

    def open(self):
        # We don't want these sockets to be buffered.
        self.set_nodelay(True)
        type(self).open_sockets.add(self)

class SentimentWebSocket(tornado.websocket.WebSocketHandler):
    # Keep track of open sockets globally, so that we can
    # communicate with them conveniently.
    open_sockets = set()

    @classmethod
    def write_to_all(cls, message):
        removable = set()
        # print(message)
        for ws in cls.open_sockets:
            if not ws.ws_connection or not ws.ws_connection.stream.socket:
                removable.add(ws)
            else:
                ws.write_message(message)

        for ws in removable:
            cls.open_sockets.remove(ws)

    def open(self):
        # We don't want these sockets to be buffered.
        self.set_nodelay(True)
        type(self).open_sockets.add(self)

class TwitterStream(StreamListener):
    def __init__(self):
        db = DBManager()
        self.cn = db.GetDatabaseConnection()
        self.liveTweetlist = []
        self.tweetlist = []
        self.printafterMin()

    def on_data(self, data):
        # print data
        try:
            json_data = json.loads(data)
            x = json_data['text']
            id = json_data['id']
            # id = id.encode('utf-8')
            x = u''.join(x).encode('utf-8').strip()
            tmpData = {"id": str(id), "text": x}
            if tmpData:
                # print(tmpData)
                TweetWebSocket.write_to_all(tmpData)

            x = self.cleanText(x)
            data = {u"text" : x}
            self.tweetlist.append(data)
        except KeyError:
            pass

    def on_error(self, status_code):
        print(status_code)

    def getSentiment(self):
        # if not self.tweetlist:
        try:
            data = str(self.tweetlist)
            data = data.replace("{u'","{'")
            x = '{"data":' + data + '}'
            # print(x)
            regex = '\"polarity\":([0-4])'
            response = urllib2.urlopen('http://www.sentiment140.com/api/bulkClassifyJson?appid=pmprakhargenius@gmail.com?text=', x)
            page = response.read()
            t1 = re.findall(regex,page)
            if t1:
                # print(t1)
                return t1

        except KeyError:
            print("Error")
            pass

    def printafterMin(self):
        threading.Timer(1.0,self.printafterMin).start()
        try:
            score = self.getSentiment()
            # print(score)
            if score!=None:
                sum=0
                zero=two=four=0
                for n in score:
                    if int(n) == 0:
                        zero+=1
                    if int(n) == 2:
                        two+=1
                    if int(n) == 4:
                        four+=1
                if (zero >= two) and (zero >= four):
                    max = 0
                elif (two >= zero) and (two >= four):
                    max = 2
                else:
                    max = 4

                dt = datetime.now()

                data = {
                    "when" : str(dt),
                    "zero" : str(zero),
                    "two" : str(two),
                    "four" : str(four)
                }

                SentimentWebSocket.write_to_all(data)
        except KeyError:
            pass

            # cur = self.cn.cursor()
            # dt = datetime.now()
            # cur.execute("""insert into sentiment(datetimestamp,zero,two,four) VALUES (%s,%s,%s,%s)""",
            #             (dt, str(zero), str(two), str(four)))
            # rowid = cur.lastrowid
            # print(rowid)
            # self.cn.commit()
            # cur.close()

            # print(max)
        # print(self.tweetlist)
        self.tweetlist = []

    def cleanText(self,x):
        if len(x) != 0:
            #Unicode remover
            regex03 = 'u[a-zA-Z0-9]{4}'

            k = re.sub(regex03,'',str(x))
            text = re.sub(r"http\S+", "", str(k))
            text = text.decode('utf-8')

            # removes emoticons and other symbols
            try:
                # UCS-4
                highpoints = re.compile(u'[\U00010000-\U0010ffff]')
            except re.error:
                # UCS-2
                highpoints = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')
            text = highpoints.sub('', text)

            tknzr = TweetTokenizer(reduce_len=True)
            a = tknzr.tokenize(text)

            # Pnctuations remover
            c = [i for i in a if i not in removal_list]
            c = " ".join(c)
            c = [i for i in a if i.isalnum()]# not in removal_list]
            c = " ".join(c)
            return c

