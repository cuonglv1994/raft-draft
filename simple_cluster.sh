#!/bin/bash

trap "exit" INT TERM ERR
trap "kill 0" EXIT

echo "creating node 127.0.0.1:3000"
python3 node.py --node_addr 127.0.0.1:3000 --cluster_nodes 127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 &

echo "creating node 127.0.0.1:3001"
python3 node.py --node_addr 127.0.0.1:3001 --cluster_nodes 127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 &

echo "creating node 127.0.0.1:3002"
python3 node.py --node_addr 127.0.0.1:3002 --cluster_nodes 127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 &

wait