from functools import wraps
import timer as timer
import random

def interval_rand():
    interval = random.randrange(1,10)
    #print(interval)
    return interval

def validate_term(func):
    @wraps(func)
    def on_receive_function(self, data):

        return func(self, data)

    return on_receive_function


def validate_commit_idx(func):
    @wraps(func)
    def on_receive_function(self, data):

        return func(self, data)

    return on_receive_function


class BaseState:
    def __init__(self, state):
        # self.current_term = 0
        # self.voted_for = None
        # self.logs = []

        self.state = state


    @validate_term
    def on_receive_append_entries(self, data):
        """

        :param data:
        data['term'] : int
        data['leader_id'] : str
        data['prev_log_idx'] : int
        data['prev_log_term'] : int
        data['entries'] : str. TODO: list of string
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


class Follower:
   # def __init__(self, *args, **kwargs):
    def __init__(self, node):

        self.node = node
        self.election_timer = timer.Timer(interval_rand, self.change_to_candidate)
        self.election_timer.start()

    def change_to_candidate(self):
        self.node.to_candidate()

    @validate_term
    def on_receive_append_entries(self):
        pass


    @validate_term
    def on_receive_vote_request(self):
        pass


class Candidate():
    def __init__(self, node):
        self.node = node
        self.election_timer = timer.Timer(interval_rand,self.send_vote_request)
        self.send_vote_request()
        self.election_timer.start()

    def on_receive_append_entries(self):
        pass

    def on_receive_vote_request(self):
        pass

    def send_vote_request(self):
        self.node.current_term += 1
        self.node.voted_for = self.node.id
        data = {
            "term": self.node.current_term,
            "candidate_id": self.node.id,
            "last_log_idx": self.node.logs.last_log_idx(),
            "last_log_term": self.node.logs.last_log_term()
        }

        for destination in self.node.cluster_nodes:
            self.node.queue.put({
                "data":data,
                "destination": (destination.split(':')[0], destination.split(':')[1])
            })


        print(data)

class Leader():
    def __init__(self, *args, **kwargs):
        pass

    def on_receive_append_entries(self):
        pass

    def on_receive_vote_request(self):
        pass

    def on_receive_append_entries_response(self):
        pass

