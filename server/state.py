from functools import wraps
from server.timer import Timer
import random
import asyncio


def interval_rand():
    interval = random.randrange(5, 10)
    return interval


def get_addr_tuple(destination):
    if isinstance(destination, str):
        try:
            addr, port = destination.split(":")
            port = int(port)
            return (addr, port)
        except IndexError:
            return None
    return None


def get_node_id(addr):
    if isinstance(addr, tuple) and len(addr) == 2:
        return '{}:{}'.format(addr[0], addr[1])
    else:
        return addr


def validate_term(func):
    @wraps(func)
    def on_receive_function(self, data):
        if self.node.node_state.term < data["term"]:
            if not isinstance(self.node.state, Follower):
                self.node.node_state.update_info(term= data["term"])
                self.change_to_follower()
                self.node.handler(data)
                return
            return func(self, data)
        elif self.node.node_state.term >= data["term"]:
            return func(self, data)

    return on_receive_function


class BaseState:
    def __init__(self, node):
        self.node = node

    def change_to_follower(self):
        self.stop()
        self.node.to_follower()

    def stop(self):
        """
        Stop current state before change to another state
        """

    def is_majority(self, amount):
        return amount > ((len(self.node.cluster_nodes) + 1) // 2)

    @validate_term
    def on_receive_append_entries(self, data):
        """

        :param data:
        data["term"] : int
        data["leader_id"] : str
        data["prev_log_idx"] : int
        data["prev_log_term"] : int
        data["entries"] : dict({
                            "term" : int,
                            "idx"  : int,
                            "cmd"  : str
                            })
        data["leader_commit"]: int
        data["sender"] : str

        :return:
        response["term"] : int
        response["success"] : bool
        """

    @validate_term
    def on_receive_append_entries_response(self, data):
        """

        :param data:
        data["term"]: int
        data["success"]: bool

        :return: None
        """

    @validate_term
    def on_receive_vote_request(self, data):
        """
        :param data:
        data["term"]: int
        data["candidate_id"]: str
        data["last_log_idx"] : int
        data["last_log_term"]: int

        :return:
        response["term"]: int
        response["vote_granted"]: bool
        """

    @validate_term
    def on_receive_vote_request_response(self, data):
        """
        :param data:
        data["term"] : int
        data["vote_granted"]: bool
        :return: None
        """


class Follower(BaseState):
    def __init__(self, node):
        super().__init__(node)

        self.election_timer = Timer(interval_rand(), self.change_to_candidate)

        self.start()

    def change_to_candidate(self):
        self.stop()
        self.node.to_candidate()

    def start(self):
        self.election_timer.start()

    def stop(self):
        self.election_timer.stop()

    @validate_term
    def on_receive_append_entries(self, data):
        if data["term"] > self.node.node_state.term:
            self.node.node_state.update_info(term=data["term"])

        if self.node.log.get_entry(data["prev_log_idx"])["term"] == data["prev_log_term"]:

            if self.node.log.last_log_idx() > data["prev_log_idx"]:
                self.node.log.delete_from_idx(data["prev_log_idx"])

            if data["entries"]:
                self.node.log.write(data["entries"]["term"], data["entries"]["command"])

            if self.node.commit_idx < data["leader_commit"]:
                self.node.commit_idx = min(data["leader_commit"], self.node.log.last_log_idx)

            self.election_timer.reset()

            response = {
                "type": "append_entries_response",
                "term": self.node.node_state.term,
                "success": True
            }

        else:
            response = {
                    "type": "append_entries_response",
                    "term": self.node.node_state.term,
                    "success": False
                }

        asyncio.ensure_future(self.node.queue.put(
            {
                "data": response,
                "destination": data["sender"]
            }
        ))

    @validate_term
    def on_receive_vote_request(self, data):

        if self.node.node_state.term < data["term"] and \
           self.node.log.last_log_idx() <= data["last_log_idx"] and \
           self.node.log.last_log_term() <= data["last_log_term"]:

            self.node.node_state.update_info(data["term"], data["candidate_id"])
            response = {
                    "type": "vote_request_response",
                    "term": self.node.node_state.term,
                    "vote_granted": True
            }

        else:
            response = {
                    "type": "vote_request_response",
                    "term": self.node.node_state.term,
                    "vote_granted": False
            }

        asyncio.ensure_future(self.node.queue.put(
            {
                "data": response,
                "destination": data["sender"]
            }
        ))


class Candidate(BaseState):
    def __init__(self, node):
        super().__init__(node)

        self.vote = 0
        self.election_timer = Timer(interval_rand(), self.node.to_candidate)
        self.start()

    def start(self):
        self.send_vote_request()
        self.election_timer.start()

    def stop(self):
        self.election_timer.stop()

    @validate_term
    def on_receive_vote_request_response(self, data):
        if data["vote_granted"]:
            self.vote += 1
            if self.is_majority(self.vote):
                self.stop()
                self.node.to_leader()

    def send_vote_request(self):
        self.node.node_state.update_info(term=(self.node.node_state.term + 1), voted_for=self.node.id)
        self.vote += 1
        data = {
            "type": "vote_request",
            "term": self.node.node_state.term,
            "candidate_id": self.node.id,
            "last_log_idx": self.node.log.last_log_idx(),
            "last_log_term": self.node.log.last_log_term()
        }

        for destination in self.node.cluster_nodes:
            asyncio.ensure_future(self.node.queue.put({
                "data": data,
                "destination": get_addr_tuple(destination)
            }))


class Leader(BaseState):
    def __init__(self, node):
        super().__init__(node)

        self.next_idx = {follower: self.node.log.last_log_idx() + 1 for follower in self.node.cluster_nodes}
        self.match_idx = {follower: 0 for follower in self.node.cluster_nodes}

        self.waiting_response = {}
        self.rid = 0

        self.heartbeat_timer = Timer(0.3*interval_rand(), self.send_heartbeat)
        self.start()

    def start(self):
        self.send_heartbeat()
        self.heartbeat_timer.start()

    def stop(self):
        self.heartbeat_timer.stop()

    @validate_term
    def on_receive_append_entries_response(self, data: dict):
        if not data["success"]:
            self.next_idx[get_node_id(data["sender"])] -= 1
            self.send_append_entries_request(data["sender"], True)
        else:
            if self.next_idx[get_node_id(data["sender"])] > self.node.log.last_log_idx():
                return
            self.match_idx[get_node_id(data["sender"])] = self.next_idx[get_node_id(data["sender"])]
            self.next_idx[get_node_id(data["sender"])] += 1
            self.send_append_entries_request(data["sender"], False)

    #def send_append_entries_request(self, entry: dict, destination: str):
    def send_append_entries_request(self, destination: str, heartbeat=True):
        data = {
            "type": "append_entries",
            "term": self.node.node_state.term,
            "leader_id": self.node.id,
            "prev_log_idx": self.next_idx[get_node_id(destination)] - 1,
            "prev_log_term": self.node.log.get_entry(self.next_idx[get_node_id(destination)] - 1)["term"],
            "entries": {} if heartbeat else self.node.log.get_entry(self.next_idx[get_node_id(destination)]),
            "leader_commit": self.node.commit_idx
        }

        # self.waiting_response[data["rid"]] = {"data": data, "destination": destination}

        asyncio.ensure_future(self.node.queue.put({
            "data": data,
            "destination": get_addr_tuple(destination)
        }))

    def send_heartbeat(self):
        for node in self.node.cluster_nodes:
            self.send_append_entries_request(node, True)

