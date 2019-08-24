import server.state
import server.server as server
import time
import asyncio

test_0 = server.Node('127.0.0.1:3000',['127.0.0.1:3001','127.0.0.1:3002'])

test_1 = server.Node('127.0.0.1:3001',['127.0.0.1:3000','127.0.0.1:3002'])

test_2 = server.Node('127.0.0.1:3002',['127.0.0.1:3001','127.0.0.1:3001'])




#print(test.state)

loop= asyncio.get_event_loop()

loop.create_task(test_0.start())
loop.create_task(test_1.start())
loop.create_task(test_2.start())

loop.run_forever()



# while True:
#     time.sleep(3)
#     print(test.status)
