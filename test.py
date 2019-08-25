import server.server as server

import multiprocessing
import asyncio

test_0 = server.Node('127.0.0.1:3000',['127.0.0.1:3001','127.0.0.1:3002'])

test_1 = server.Node('127.0.0.1:3001',['127.0.0.1:3000','127.0.0.1:3002'])

test_2 = server.Node('127.0.0.1:3002',['127.0.0.1:3001','127.0.0.1:3001'])



def run_node(node):
    loop = asyncio.get_event_loop()

    loop.create_task(node.start())
    loop.run_forever()


if __name__ == '__main__':
    try:
        p1 = multiprocessing.Process(target=run_node,args=(test_0,)).start()
        p2 = multiprocessing.Process(target=run_node, args=(test_1,)).start()
        p3 = multiprocessing.Process(target=run_node, args=(test_2,)).start()
    except KeyboardInterrupt:
        p1.terminate()
        p2.terminate()
        p3.terminate()



# while True:
#     time.sleep(3)
#     print(test.status)
