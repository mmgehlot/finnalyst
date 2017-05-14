import gnp
import tornado.web
from kafka import KafkaConsumer

from DailyStockData import StockMarket
from LiveStockStream import *
from KafkaConsumerStream import *
from DBManager import *
from IntraDayStockData import *
from tornado.escape import json_encode
from TwitterDataStream import *
from auth0.v3.authentication import GetToken
from auth0.v3.authentication import Users
import requests


API_AUDIENCE = ""
AUTH0_CALLBACK_URL = ''
AUTH0_CLIENT_ID = ''
AUTH0_CLIENT_SECRET = ''
AUTH0_DOMAIN = ''


class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("user")

class LoginHandler(BaseHandler):
    # @tornado.gen.coroutine
    def get(self):
        self.render('/static/login.html')

    @tornado.gen.coroutine
    def post(self):
        incorrect = self.get_secure_cookie("incorrect")
        if incorrect and int(incorrect) > 20:
            self.write('<center>blocked</center>')
            return

        getusername = tornado.escape.xhtml_escape(self.get_argument("username"))
        getpassword = tornado.escape.xhtml_escape(self.get_argument("password"))
        self.set_secure_cookie("incorrect", "0")
        if "demo" == getusername and "demo" == getpassword:
            self.set_secure_cookie("user", self.get_argument("username"))
            self.set_secure_cookie("incorrect", "0")
            self.redirect(self.reverse_url("main"))
        else:
            incorrect = self.get_secure_cookie("incorrect") or 0
            increased = str(int(incorrect) + 1)
            self.set_secure_cookie("incorrect", increased)
            self.write("""<center>
                            Something Wrong With Your Data (%s)<br />
                            <a href="/">Go Home</a>
                          </center>""" % increased)

class LogoutHandler(BaseHandler):
    def get(self):
        # self.clear_cookie("user")
        r = requests.post("https://finnalyst.auth0.com/v2/logout")

        # self.redirect("static/Logout.html")

class Dashboard(tornado.web.RequestHandler):
    @tornado.web.authenticated
    def get(self):
        self.render("index.html")

class IntradayHandler(tornado.web.RequestHandler):
    def get(self):
        obj = StockMarket()
        recordSet = obj.startMarket('Nasdaq')
        # print(recordSet)
        record = str(recordSet)
        record = record[2:len(record) - 2]
        self.content_type = 'application/json'
        self.write(json_encode(record))
        # self.write("Hello")

class UserProfile(tornado.web.RequestHandler):
    def get(self):
        pass

class  HistoricalDataHandler(tornado.web.RequestHandler):
    def get(self):
        symbol = self.request.arguments.values()[0]
        interval = self.request.arguments.values()[1]
        days = self.request.arguments.values()[2]
        symbol = str(symbol[0])
        interval = str(interval[0])
        days = str(days[0])
        arg = self.get_query_arguments

        if arg:
            q = GoogleIntradayQuote('nasdaq',symbol, float(interval), int(days))
            # print(q)
            # for r in q:
            #     print(r["data"])
            record = str(q)
            record = record[1:len(record) - 1]
            self.content_type = 'application/json'
            self.write(json_encode(json.loads("[" + record + "]")))

class NewsServer(tornado.web.RequestHandler):
    def post(self):
        query = self.get_query_argument("query")
        # print(query)
        c = gnp.get_google_news_query(query)
        # data = json.loads(c)
        # x = u''.join(data).encode('utf-8').strip()
        # j = json.dumps(x)
        self.content_type = 'application/json'
        self.write(json_encode(c))

class CompanyList(tornado.web.RequestHandler):
    def post(self):
        query = self.get_query_argument("index")
        if query:
            db = DBManager()
            q = getList(db,query)
            record = str(q)
            record = record[1:len(record) - 1]
            self.content_type = 'application/json'
            self.write(json_encode(json.loads("[" + record + "]")))

    def get(self):
        query = self.get_query_argument("index")
        if query:
            db = DBManager()
            q = getList(db,query)
            record = str(q)
            record = record[1:len(record) - 1]
            self.content_type = 'application/json'
            self.write(json_encode(json.loads("[" + record + "]")))

# def on_shutdown():
#     print('Shutting down')
#     IOLoop.instance().stop()


class SSOHandler(tornado.web.RequestHandler):
    def get(self):
        code = self.get_argument("code")
        get_token = GetToken(AUTH0_DOMAIN)
        auth0_users = Users(AUTH0_DOMAIN)

        url = "https://finnalyst.auth0.com/oauth/token"
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        data = {
            "client_id" : AUTH0_CLIENT_ID,
            "redirect_uri" : "http://54.153.126.234:8888/static/index.html",
            "client_secret": AUTH0_CLIENT_SECRET,
            "code": code,
            "grant_type": 'authorization_code',
        }
        r = requests.post(url,data = json.dumps(data) ,headers = headers)
        print(r.json())
        # token = get_token.authorization_code(AUTH0_CLIENT_ID,
        #                                      AUTH0_CLIENT_SECRET, code, AUTH0_CALLBACK_URL)
        # print(token)
        # user_info = auth0_users.userinfo(token['access_token'])
        # session[constants.PROFILE_KEY] = json.loads(user_info)
        return self.redirect('/static/index.html')



def main():
    # Start kafka producer
    stopFlagNasdaq = Event()
    stopFlagNyse = Event()
    thread = Nasdaq(stopFlagNasdaq)
    thread.start()
    thread = Nyse(stopFlagNyse)
    thread.start()

    # Start the kafka consumer thread.
    kafka_consumer_nasdaq = Consumer(KafkaConsumer('nasdaq'))
    kafka_consumer_nyse = Consumer(KafkaConsumer('nyse'))
    kafka_consumer_nasdaq.start()
    kafka_consumer_nyse.start()
    #
    try:
        listener = TwitterStream()
        auth = OAuthHandler("<consumer-key>","<consumer-secret") # provide consumer key and consumer-secret here
        auth.set_access_token("<access-key","<access-secret>") #provide access-key and access-secret

        stream = Stream(auth,listener)
        stream.filter(languages=["en"],track=['music'], async=True)

    except (KeyError) as e:
        pass

    STATIC_DIRNAME = "static"
    settings = {
        "static_path": os.path.join(os.path.dirname(__file__), STATIC_DIRNAME),
        "static_url_prefix": "/static/",
        "cookie_secret": "thisissecret",
        "login_url": "/login",
        'template_path': os.path.join("static"),
        'static_path': os.path.join("static"),
        'debug': True,
        "xsrf_cookies": False,
    }

    application = tornado.web.Application([
        (r"/stockws", KafkaWebSocket),
        (r"/tweetws", TweetWebSocket),
        (r"/stws", SentimentWebSocket),
        tornado.web.url(r"/getHistoricalData", HistoricalDataHandler),
        tornado.web.url(r"/intradaydata", IntradayHandler),
        tornado.web.url(r"/getLatestNews", NewsServer),
        tornado.web.url(r"/getCompanyList", CompanyList),
        tornado.web.url(r"/static/index\.html", Dashboard, name="main"),
        tornado.web.url(r'/static/login\.html', LoginHandler, name="login"),
        tornado.web.url(r'/callback', SSOHandler, name="SSO"),
        tornado.web.url(r'/logout', LogoutHandler, name="logout")
    ], **settings)

    print("Server started. listening at port 8888!")
    application.listen(8888)
    # ioloop = IOLoop()
    # signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(on_shutdown))
    # ioloop.start()
    tornado.ioloop.IOLoop.current().start()

if __name__=="__main__":
    main()
