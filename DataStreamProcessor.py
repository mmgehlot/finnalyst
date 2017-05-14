from __future__ import print_function

import json
import re
import urllib2
from kafka import KafkaProducer
from tweepy import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler
from pyspark import SparkContext
from nltk import TweetTokenizer
from nltk.corpus import stopwords
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

removalList = ['!', '#', '"', '%', '$', "'", '&', ')', '(', '+', '*', '-', ',', '/', '.', ';', ':', '=', '<', '?', '>', '@', '[', ']', '\\', '_', '^', '`', '{', '}', '|', '~']
stopWordList = stopwords.words('english')

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

class TwitterStream(StreamListener):
    def on_data(self, data):
        json_data = json.loads(data)
        x = json_data['text']
        x = u''.join(x).encode('utf-8').strip()
        if x:
            producer.send('testTopic', x)
        # id = json_data['id']
        # tmpData = {"id": str(id), "text": x}
        # if tmpData:
        #     print(tmpData)
            # TweetWebSocket.write_to_all(tmpData)
        #     producer.send('twitter',tmpData)

def getSentiment(x):
    if x != None:
        x = '{"data": [{"text":'+str(x)+'}]}'
        regex = '\"polarity\":([0-4])'
        response = urllib2.urlopen('http://www.sentiment140.com/api/bulkClassifyJson?appid=pmprakhargenius@gmail.com?text=', x)
        page = response.read()
        t1 = re.findall(regex,page)
        return t1

def cleanText(x):
    # x = json.loads(x)
    # tmp = x
    # x = x["text"]
    #
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
        # c = {"id" : tmp["id"], "text" : c}
        return c

# if __name__ == "__main__":
#     if len(sys.argv) != 3:
#         print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
#         exit(-1)

def processTweets(rdd):
    rdd.map(lambda tweet: writetoKafka(tweet))

def writetoKafka(cleanTweet):
    cleanTweet = cleanText(cleanTweet)
    score = getSentiment(cleanTweet)
    producer("testTopic" , {"score" : score, "tweet" : cleanTweet})

sc = SparkContext(appName="SparkKafkaStreaming")
ssc = StreamingContext(sc, 1)

try:
    listener = TwitterStream()
    auth = OAuthHandler('', '')
    auth.set_access_token('',
                          '')

    stream = Stream(auth, listener)
    stream.filter(languages=["en"], track=['Nasdaq', 'NYSE','Stock market','stock exchange','stock price','music'], async=True)

except (KeyError, KeyboardInterrupt) as e:
    pass

#brokers, topic = sys.argv[1:]
kvs = KafkaUtils.createDirectStream(ssc, ['testTopic'], {"metadata.broker.list": "localhost:9092"})
lines = kvs.map(lambda x: x[1])

CleanedTweet = lines.flatMap(lambda line : cleanText(line))
# ScoredTweet = CleanedTweet.flatMap()
CleanedTweet.foreachRDD(processTweets)

ssc.start()
ssc.awaitTermination()