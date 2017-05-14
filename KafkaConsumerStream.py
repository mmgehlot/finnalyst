from __future__ import absolute_import, print_function
import collections
import threading
import os

import tornado.websocket

message_history = collections.deque([], maxlen=100)
cwd = os.getcwd()

class KafkaWebSocket(tornado.websocket.WebSocketHandler):
    open_sockets = set()

    @classmethod
    def write_to_all(cls, message):
        removable = set()
        for ws in cls.open_sockets:
            if not ws.ws_connection or not ws.ws_connection.stream.socket:
                removable.add(ws)
            else:
                ws.write_message(message)

        for ws in removable:
            cls.open_sockets.remove(ws)

    def open(self):
        self.set_nodelay(True)
        type(self).open_sockets.add(self)

class Consumer(threading.Thread):
    daemon = True

    def __init__(self, kafka_consumer):
        self._consumer = kafka_consumer
        super(Consumer, self).__init__()

    def run(self):
        for message in self._consumer:
            message = str(message)
            n1 = message.index("value=", 1, len(message))
            n2 = message.index("checksum=", n1, len(message))
            message = message[n1 + 7:n2 - 3]
            message_history.append(message)
            KafkaWebSocket.write_to_all(message)