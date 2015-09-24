import socket, sys, ssl, uuid, thread, json, re
from functools import partial

class Susi:
    def __init__(self,addr,port,cert,key):
        s = None
        for res in socket.getaddrinfo(addr, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                s = socket.socket(af, socktype, proto)
            except socket.error as msg:
                s = None
                continue
            try:
                s.connect(sa)
            except socket.error as msg:
                s.close()
                s = None
                continue
            break
        if s is None:
            print 'could not open socket'
            sys.exit(1)
        self.rawSocket = s
        self.secureSocketContext = ssl.create_default_context()
        self.secureSocketContext.load_cert_chain(cert,key)
        self.secureSocketContext.check_hostname = False
        self.secureSocketContext.verify_mode = ssl.CERT_NONE
        self.secureSocket = self.secureSocketContext.wrap_socket(self.rawSocket)
        self.data = ""
        self.finishCallbacks = {}
        self.consumers = []
        self.processors = []
        self.publishProcesses = {}

    def send(self, msg):
        self.secureSocket.sendall(msg)

    def receive(self,cb):
        while True:
            chunk = self.secureSocket.recv(1024)
            if chunk == '':
                raise RuntimeError("socket connection broken")
            for chr in chunk:
                if chr == '\n':
                    cb(self.data)
                    self.data = ""
                else:
                    self.data += chr

    def dispatch(self,msg):
        doc = json.loads(msg)
        if doc["type"] == "ack" or doc["type"] == "dismiss":
            if self.finishCallbacks.has_key(doc["data"]["id"]):
                self.finishCallbacks[doc["data"]["id"]](doc["data"])
                del self.finishCallbacks[doc["data"]["id"]]
        elif doc["type"] == "consumerEvent":
            event = doc["data"]
            for c in self.consumers:
                if re.match(c[1],event["topic"]):
                    c[2](event)
        elif doc["type"] == "processorEvent":
            event = doc["data"]
            publishProcess = ([],0)
            for p in self.processors:
                if re.match(p[1],event["topic"]):
                    publishProcess[0].append(p[2])
            self.publishProcesses[event["id"]] = publishProcess
            self.ack(event)

    def ack(self,event):
        process = self.publishProcesses[event["id"]]
        if process[1] >= len(process[0]):
            packet = {
                "type": "ack",
                "data": event
            }
            self.send(json.dumps(packet)+"\n")
            del self.publishProcesses[event["id"]]
            return
        nextIdx = process[1]
        self.publishProcesses[event["id"]] = (process[0],nextIdx+1)
        process[0][nextIdx](event)

    def dismiss(self,event):
        packet = {
            "type": "dismiss",
            "data": event
        }
        self.send(json.dumps(packet)+"\n")
        del self.publishProcesses[event["id"]]

    def run(self):
        self.receive(self.dispatch)

    def publish(self,event,finishCallback):
        id = None
        if event.has_key("topic"):
            if not event.has_key("id"):
                id = str(uuid.uuid4())
                event["id"] = id
            else:
                id = event["id"]
            self.finishCallbacks[id] = finishCallback
            packet = {
                "type": "publish",
                "data": event
            }
            self.send(json.dumps(packet)+"\n")

    def registerConsumer(self,topic,consumer):
        id = str(uuid.uuid4())
        self.consumers.append((id,topic,consumer))
        self.send(json.dumps({"type":"registerConsumer","data":{"topic":topic}})+"\n")
        return id

    def registerProcessor(self,topic,processor):
        id = str(uuid.uuid4())
        self.processors.append((id,topic,processor))
        self.send(json.dumps({"type":"registerProcessor","data":{"topic":topic}})+"\n")
        return id

    def unregisterConsumer(self,id):
        for i in range(0,len(self.consumers)):
            if self.consumers[i][0] == id:
                self.consumers.pop(i)
                break

    def unregisterProcessor(self,id):
        for i in range(0,len(self.processors)):
            if self.processors[i][0] == id:
                self.processors.pop(i)
                break

if __name__ == "__main__":
    susi = Susi("localhost",4000,"cert.pem","key.pem")

    def processor(susi,field,value,event):
        event["payload"][field] = value
        susi.ack(event)

    susi.registerProcessor("foobar",partial(processor,susi,"one",1))
    susi.registerProcessor("foobar",partial(processor,susi,"two",2))
    susi.registerProcessor("foobar",partial(processor,susi,"three",3))

    def consumer(susi,event):
        print("consumer: "+json.dumps(event))

    susi.registerConsumer(".*",partial(consumer,susi))

    def finish(susi,event):
        print("result: "+json.dumps(event))

    susi.publish({
        "topic":"foobar",
        "payload": {}
    },partial(finish,susi));

    susi.run();
