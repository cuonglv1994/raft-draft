

class Node:
    def __init__(self,address, cluster_nodes):
        self.id = address.replace(':','-')
        self.address = address
        self.ip, self.port = self.address.split(':')

        # cluster_nodes: list of node, ['ip:port','ip:port',....]
        self.cluster_nodes = cluster_nodes

        self.commit_idx = 0
        self.last_applied = 0



