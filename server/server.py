import asyncio
from server.state import Follower, Candidate, Leader
from server.storage import Log, PersistentNodeInfo, SimulatedStateMachine
from server.network import PeerProtocol


class Node:
    def __init__(self, address, cluster_nodes):
        self.id = address
        self.address = address
        self.ip, self.port = self.address.split(":")

        # cluster_nodes: list of node, ["ip:port","ip:port",....]
        self.cluster_nodes = set(cluster_nodes)
        self.cluster_nodes.remove(address)

        self.commit_idx = 0
        self.last_applied = 0

        self.node_persistent_state = PersistentNodeInfo(self.id)
        self.log = Log(self.id)

        self.loop = asyncio.get_event_loop()
        self.queue = asyncio.Queue()
        self.state_machine = SimulatedStateMachine(self.id)

        self.state = Follower(self)

    async def start(self):
        protocol = PeerProtocol(queue=self.queue, handler = self.handler, loop=self.loop)

        transport, _ = await self.loop.create_task(self.loop.create_datagram_endpoint(protocol,
                                                                                      local_addr=(self.ip, self.port)))

    def handler(self, data):
        getattr(self.state, "on_receive_{}".format(data["type"]))(data)

    def to_candidate(self):
        self.state.stop()
        self.state = Candidate(self)
        self.state.start()


    def to_follower(self):
        self.state.stop()
        self.state = Follower(self)
        self.state.start()

    def to_leader(self):
        self.state.stop()
        self.state = Leader(self)
        self.state.start()


