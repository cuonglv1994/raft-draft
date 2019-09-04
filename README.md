# raft-draft

Simple implementation of Raft Consensus Algorithm
[https://raft.github.io]

1. Completed features:
- Leader Election
- Log Replication (with simple state machine which can do only assignment operation)

How to run:
- For single node: 
`python3 node.py --node_addr [ip:port] --cluster_nodes [ip1:port,ip2:port,ip3:port]`

with ip:port pair must be one of the pairs in cluster_nodes

- For 3 nodes cluster test:
`bash simple_cluster.sh`

Press Ctrl - C to stop all nodes

TODO:
- Client interaction
- Test scenario
- Log compaction and Membership changes