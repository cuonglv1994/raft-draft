import timer
import asyncio
import random
import time

def test():
    print('timer tester')

def interval_rand():
    interval = random.randrange(1,10)
    print(interval)
    return interval


class PeerProtocol(asyncio.DatagramProtocol):

    def __init__(self, queue, loop = None, destination = None, ):
        #super().__init__()
        self.loop = loop or asyncio.get_event_loop()
        self.queue = queue
        self.destination = destination

    def __call__(self):
        return self



    def connection_made(self, transport):
        print('connection made')
        self.transport = transport

        # if self.destination:
        #     self.periodic_send = timer.Timer(interval_rand, self.send_data)
        #     self.periodic_send.start()

    async def send_data(self):
        while True:
            if self.destination:
                msg = await self.queue.get()
                print(msg)
                self.transport.sendto(msg, self.destination)


    def datagram_received(self, data, addr):
        print(data)
        print(addr)
        #self.queue.put(bytes("random data {}".format(interval_rand())))


queue = asyncio.Queue()
loop_test=asyncio.get_event_loop()
asyncio.sleep(5,queue.put("random data {}".format(interval_rand()).encode()))
async def start(loop_test, address, destination = None):

    protocol = PeerProtocol(queue=queue, loop = loop_test, destination = destination)

    transport, _ = await asyncio.Task(loop_test.create_datagram_endpoint(protocol,address),loop=loop_test)

async def data_gen():
    while True:
        await asyncio.sleep(5,queue.put("random data {}".format(interval_rand()).encode()))
        #asyncio.ensure_future(queue.put(bytes("random data {}".format(interval_rand()))))

#start(loop_test)
loop_test.create_task(start(loop_test, ('127.0.0.1',3000), ('127.0.0.1',3001)))
loop_test.create_task(start(loop_test, ('127.0.0.1',3001), ('127.0.0.1',3000)))
loop_test.create_task(data_gen())
loop_test.run_forever()








