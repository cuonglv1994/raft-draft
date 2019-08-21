from functools import wraps
import timer as timer
import random
import asyncio



def interval_rand():
    interval = random.randrange(5,30)
    #print(interval)
    return interval

def is_majority(amount, total):
    return amount > (total // 2)

def validate_term(func):
    @wraps(func)
    def on_receive_function(self, data):
        if self.node.current_term < data['term']:
            if not isinstance(self.node.state,Follower):
                self.node.to_follower()
                self.node.current_term = data['term']
                self.node.handler(data)
                return
            return func(self, data)
        elif self.node.current_term >= data['term']:
            #return

            return func(self, data)

    return on_receive_function


def validate_commit_idx(func):
    @wraps(func)
    def on_receive_function(self, data):

        return func(self, data)

    return on_receive_function


class BaseState:
    def __init__(self):
        # self.current_term = 0
        # self.voted_for = None
        # self.logs = []
        pass




    @validate_term
    def on_receive_append_entries(self, data):
        """

        :param data:
        data['term'] : int
        data['leader_id'] : str
        data['prev_log_idx'] : int
        data['prev_log_term'] : int
        data['entries'] : dict(
                            {
                            'term' : int,
                            'idx'  : int,
                            'cmd'  : str
                            }
                                )
        data['leader_commit']: int
        data['sender'] : str

        :return:
        response['term'] : int
        response['success'] : bool
        """

    @validate_term
    def on_receive_append_entries_response(self, data):
        """

        :param data:
        data['term']: int
        data['success']: bool

        :return: None
        """

    @validate_term
    def on_receive_vote_request(self, data):
        """
        :param data:
        data['term']: int
        data['candidate_id']: str
        data['last_log_idx'] : int
        data['last_log_term']: int

        :return:
        response['term']: int
        response['vote_granted']: bool
        """

    @validate_term
    def on_receive_vote_request_response(self, data):
        """
        :param data:
        data['term'] : int
        data['vote_granted']: bool
        :return: None
        """





## to become decorator function
#def validate_term():


class Follower(BaseState):
   # def __init__(self, *args, **kwargs):
    def __init__(self, node):

        self.node = node

        self.election_timer = timer.Timer(interval_rand, self.change_to_candidate)

        self.start()


    def change_to_candidate(self):
        self.node.to_candidate()

    def start(self):
        self.election_timer.start()

    def stop(self):
        self.election_timer.stop()

    @validate_term
    def on_receive_append_entries(self, data):
        if data['term'] > self.node.current_term:
            self.node.current_term = data['term']

        response = {}
        if self.node.logs.last_log_idx() == data['prev_log_idx']:
            if self.node.logs.last_log_term() == data['prev_log_term']:
                if len(data['entries']) > 0:
                    self.node.logs.write(data['entries']['term'], data['entries']['idx'], data['entries']['cmd'])

                self.election_timer.reset()

                response = {
                    'type' : 'append_entries_response',
                    'term' : self.node.current_term,
                    'success' : True
                }

            else:
                'TODO: delete entries'

        else:
            response = {
                    'type' : 'append_entries_response',
                    'term' : self.node.current_term,
                    'success' : False
                }

        asyncio.ensure_future(self.node.queue.put(
            {
                "data": response,
                "destination": data['sender']
            }
        ))



    @validate_term
    def on_receive_vote_request(self, data):
        response ={}
        if self.node.current_term >= data['term']:
            response = {
                'type': 'vote_request_response',
                'term':  self.node.current_term,
                'vote_granted': False
            }

        elif self.node.current_term < data['term']:
            if self.node.logs.last_log_term() <= data['last_log_term'] and \
               self.node.logs.last_log_idx() <= data['last_log_idx']:

                self.node.current_term = data['term']
                self.node.voted_for = data['candidate_id']
                response = {
                    'type': 'vote_request_response',
                    'term': self.node.current_term,
                    'vote_granted': True
                }

        asyncio.ensure_future(self.node.queue.put(
            {
                "data": response,
                "destination": data['sender']
            }
        ))



class Candidate(BaseState):
    def __init__(self, node):
        self.node = node
        self.vote = 0
        self.election_timer = timer.Timer(interval_rand,self.node.to_candidate)
        self.start()

    def start(self):
        self.send_vote_request()
        self.election_timer.start()

    def stop(self):
        self.election_timer.stop()


    def on_receive_vote_request_response(self, data):
        if data['vote_granted']:
            self.vote += 1
            if is_majority(self.vote, len(self.node.cluster_nodes)):
                self.election_timer.stop()
                self.node.to_leader()



    def send_vote_request(self):
        self.node.current_term += 1
        self.node.voted_for = self.node.id
        self.vote += 1
        data = {
            "type": 'vote_request',
            "term": self.node.current_term,
            "candidate_id": self.node.id,
            "last_log_idx": self.node.logs.last_log_idx(),
            "last_log_term": self.node.logs.last_log_term()
        }

        for destination in self.node.cluster_nodes:
            asyncio.ensure_future(self.node.queue.put({
                "data":data,
                "destination": (destination.split(':')[0], int(destination.split(':')[1]))
            }))


        #print(data)

class Leader(BaseState):
    def __init__(self, node):
        self.node = node
        self.follower_log = []
        self.heartbeat_timer = timer.Timer(2, self.send_append_entries_request)
        self.start()

    def start(self):
        self.send_append_entries_request()
        self.heartbeat_timer.start()

    def stop(self):
        self.heartbeat_timer.stop()

    def on_receive_append_entries(self, data):
        pass

    def on_receive_vote_request(self, data):
        pass

    def on_receive_append_entries_response(self, data):
        pass

    def send_append_entries_request(self, entry = None):
        data = {
            "type": 'append_entries',
            "term": self.node.current_term,
            "leader_id": self.node.id,
            "prev_log_idx": self.node.logs.last_log_idx(),
            "prev_log_term": self.node.logs.last_log_term(),
            "entries": {} if entry is None else entry,
            "leader_commit": 1
        }

        for destination in self.node.cluster_nodes:
            asyncio.ensure_future(self.node.queue.put({
                "data": data,
                "destination": (destination.split(':')[0], int(destination.split(':')[1]))
            }))

