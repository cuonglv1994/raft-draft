import server.server as server
import asyncio
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--node_addr',
                    required=True,
                    help='Node address, also acts as node id',
                    metavar='IPv4:port'
                    )
parser.add_argument('--cluster_nodes',
                    required=True,
                    help='List of all cluster nodes',
                    metavar='node1_ip:port,node2_ip:port,.....'
                    )
args = parser.parse_args()

if __name__ == '__main__':

    loop = asyncio.get_event_loop()

    cluster_nodes = [node.strip() for node in args.cluster_nodes.split(',')]

    node = server.Node(args.node_addr, cluster_nodes)

    loop.create_task(node.start())
    loop.run_forever()

