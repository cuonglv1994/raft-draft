import server.server as server

import multiprocessing
import asyncio

node = server.Node('127.0.0.1:3001',['127.0.0.1:3000','127.0.0.1:3001','127.0.0.1:3002'])


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    loop.create_task(node.start())
    loop.run_forever()



# while True:
#     time.sleep(3)
#     print(test.status)
