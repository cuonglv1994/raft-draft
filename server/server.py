import state as state
import storage as storage
import asyncio
import network as network

class Node:
    def __init__(self, address, cluster_nodes):
        self.id = address.replace(':', '-')
        self.address = address
        self.ip, self.port = self.address.split(':')

        # cluster_nodes: list of node, ['ip:port','ip:port',....]
        self.cluster_nodes = cluster_nodes

        self.commit_idx = 0
        self.last_applied = 0

        self.current_term = 0
        self.voted_for = None
        self.logs = storage.Log(self.id)

        self.state = state.Follower(self)

        self.loop = asyncio.get_event_loop()
        self.queue = asyncio.Queue()

    async def start(self):
        protocol = network.PeerProtocol(queue=self.queue, handler = self.handler, loop=self.loop)

        transport, _ = await self.loop.create_task(self.loop.create_datagram_endpoint(protocol,
                                                                                    local_addr=(self.ip,self.port)))

    def handler(self, data):
        #print('handler')
        #print(data)
        getattr(self.state,'on_receive_{}'.format(data['type']))(data)

    def to_candidate(self):
        self.state.stop()
        self.state = state.Candidate(self)
        #print(self.status)

    def to_follower(self):
        self.state.stop()
        self.state = state.Follower(self)

    def to_leader(self):
        self.state.stop()
        self.state = state.Leader(self)


