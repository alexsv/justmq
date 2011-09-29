from circuits import Debugger, Event
from circuits.net.sockets import TCPServer, TCPClient
from time import time

class MQServer(TCPServer):
    clients = {}
    
    def connect(self, sock, host, port):
        self.clients[sock] = []
        self.write(sock, 'HELLO')
        
    def read(self, sock, msg):
        splitted = msg.split(':')
        if splitted:
            cmd = splitted[0]
            if cmd == 'SUBSCRIBE':
                self.clients[sock].extend(splitted[1].split(','))
                #print self.clients[sock]
            elif cmd == 'MSG':
                (channels, msg) = (splitted[1], splitted[2])
    
    def disconnect(self, sock):
        del self.clients[sock]

class Client(TCPClient):
    
    _subscribe = ''
    _prev_time = 0
    
    def __init__(self, **kwargs):
        super(Client, self).__init__(**kwargs)
        if 'channels' in kwargs:
            self._subscribe = kwargs['channels']
    
    def ready(self, *args):
        self.connect('127.0.0.1', 9000)
        
    def process(self, *args):
        if time.now() - self._prev_time > 2:
            self._prev_time = time.now()
            print 'PROCESS'
        self.fire(Event(), 'process')
    
    def read(self, data):
        splitted = data.split(':')
        if splitted:
            cmd = splitted[0]
            if cmd == 'HELLO':
                msg = 'SUBSCRIBE:%s' % (self._subscribe,)
                self.write(msg)
                self.fire(Event(), 'process')
            elif cmd == 'MSG':
                self.fire(Event(message=splitted[1]))
    
    def subscribe(self, channels):
        self._subscribe = channels

    def send(self, channels, msg):
        self.write()


SOCKET = ('0.0.0.0', 9000)

(MQServer(SOCKET)
 +Client(channels='logger,manager')
# +Client(channels='logger')
 +Debugger()
).run()
