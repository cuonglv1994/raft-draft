import asyncio

class PeerProtocol(asyncio.DatagramProtocol):

    def __init__(self, queue, loop = None):
        #super().__init__()
        self.loop = loop or asyncio.get_event_loop()
        self.queue = queue

    def __call__(self):
        return self

    async def start(self):
        while not self.transport.is_closing():
            msg = await self.queue.get()
            print(msg)
            self.transport.sendto(msg['data'].encode(), msg['destination'])

    def connection_made(self, transport):
        print('connection made')
        self.transport = transport
        asyncio.ensure_future(self.start(), loop=self.loop)

    def datagram_received(self, data, addr):
        print(data.decode)
        print(addr)

    def connection_lost(self, exc):
        print('Connection lost: {}'.format(exc))