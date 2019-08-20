import state as state
import storage as storage
import asyncio
import network as network


test = Node('127.0.0.1:3000',['127.0.0.1:3001'])


print(test.status)

loop= asyncio.get_event_loop()
loop.run_forever()

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

        self.status = state.Follower(self)

        self.loop = asyncio.get_event_loop()
        self.queue = asyncio.Queue()

    async def start(self):
        protocol = network.PeerProtocol(queue=self.queue, loop=self.loop)

        transport, _ = await asyncio.create_task(self.loop.create_datagram_endpoint(protocol,
                                                                                    local_addr=(self.ip,self.address)))

    def to_candidate(self):
        self.status = state.Candidate(self)
        #print(self.status)

    def to_follower(self):
        self.status = state.Follower(self)

    def to_leader(self):
        self.status = state.Leader(self)


