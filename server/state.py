from functools import wraps
from server.timer import Timer
import random
import asyncio


def interval_rand():
    interval = random.randrange(10, 20)
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
        if self.node.term < data["term"]:
            if not isinstance(self.node.state, Follower):
                self.node.persistent_state_update(term=data["term"])
                self.node.to_follower()
                self.node.handler(data)
                return
            return func(self, data)
        elif self.node.term >= data["term"]:
            return func(self, data)

    return on_receive_function


def apply_state_machine_command(func):
    @wraps(func)
    def on_receive_function(self, data):
        func(self, data)
        if self.node.commit_idx > self.node.last_applied:
            for idx in range(self.node.last_applied + 1, self.node.commit_idx + 1):
                self.node.command_execution(self.node.log.get_entry(idx)["command"], idx)
                self.node.last_applied = idx
    return on_receive_function


class BaseState:
    def __init__(self, node):
        self.node = node

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

    @apply_state_machine_command
    @validate_term
    def on_receive_append_entries(self, data):

        if self.node.term <= data["term"]:
            if self.node.term < data["term"]:
                self.node.persistent_state_update(term=data["term"], voted_for=data["leader_id"])

            if self.node.log.get_entry(data["prev_log_idx"])["term"] == data["prev_log_term"]:

                if self.node.log.last_log_idx > data["prev_log_idx"]:
                    self.node.log.delete_from_idx(data["prev_log_idx"])

                if data["entries"]:
                    self.node.log.write_entry(data["entries"]["term"], data["entries"]["command"])

                if self.node.commit_idx < data["leader_commit"]:
                    self.node.commit_idx = min(data["leader_commit"], self.node.log.last_log_idx)

                success = True

            else:

                success = False

            self.election_timer.reset()

        else:

            success = False

        response = {
            "type": "append_entries_response",
            "term": self.node.term,
            "success": success
        }

        asyncio.ensure_future(self.node.queue.put(
            {
                "data": response,
                "destination": data["sender"]
            }
        ))

    @validate_term
    def on_receive_vote_request(self, data):

        if self.node.term <= data["term"]:

            if self.node.log.last_log_term != data["last_log_term"]:
                up_to_date = self.node.log.last_log_term < data["last_log_term"]
            else:
                up_to_date = self.node.log.last_log_idx <= data["last_log_idx"]

            if up_to_date:
                self.node.persistent_state_update(data["term"], data["candidate_id"])

            response = {
                    "type": "vote_request_response",
                    "term": self.node.term,
                    "vote_granted": up_to_date
            }

        else:
            response = {
                    "type": "vote_request_response",
                    "term": self.node.term,
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
        self.candidate_timer = Timer(interval_rand(), self.restart_election)

    def start(self):
        self.send_vote_request()
        self.candidate_timer.start()

    def stop(self):
        self.candidate_timer.stop()

    def restart_election(self):
        self.stop()
        self.start()

    @validate_term
    def on_receive_vote_request_response(self, data):
        if data["vote_granted"]:
            self.vote += 1
            if self.is_majority(self.vote):
                self.node.to_leader()

    @validate_term
    def on_receive_append_entries(self, data):
        self.node.persistent_state_update(term=data["term"])

        self.node.to_follower()
        self.node.handler(data)

    def send_vote_request(self):
        self.node.persistent_state_update(term=(self.node.term + 1),
                                          voted_for=self.node.id)

        data = {
            "type": "vote_request",
            "term": self.node.term,
            "candidate_id": self.node.id,
            "last_log_idx": self.node.log.last_log_idx,
            "last_log_term": self.node.log.last_log_term
        }

        for destination in self.node.cluster_nodes:
            asyncio.ensure_future(self.node.queue.put({
                "data": data,
                "destination": get_addr_tuple(destination)
            }))


class Leader(BaseState):
    def __init__(self, node):
        super().__init__(node)

        # self.next_idx = {follower: self.node.log.last_log_idx + 1 for follower in self.node.cluster_nodes}
        # self.match_idx = {follower: 0 for follower in self.node.cluster_nodes}

        self.next_idx = {}
        self.match_idx = {}

        self.heartbeat_timer = Timer(0.1*interval_rand(), self.send_heartbeat)

    def start(self):

        self.write_noop_entry()

        self.next_idx = {follower: self.node.log.last_log_idx + 1 for follower in self.node.cluster_nodes}
        self.match_idx = {follower: 0 for follower in self.node.cluster_nodes}

        self.send_heartbeat()
        self.heartbeat_timer.start()

    def stop(self):
        self.heartbeat_timer.stop()

    @apply_state_machine_command
    @validate_term
    def on_receive_append_entries_response(self, data):
        if not data["success"]:
            self.next_idx[get_node_id(data["sender"])] = max(1, self.next_idx[get_node_id(data["sender"])] - 1)
        else:
            if self.next_idx[get_node_id(data["sender"])] > self.node.log.last_log_idx:
                return
            self.match_idx[get_node_id(data["sender"])] = self.next_idx[get_node_id(data["sender"])]
            self.next_idx[get_node_id(data["sender"])] += 1
            self.commit_idx_update(data)

    def commit_idx_update(self, data):
        entry_idx = self.match_idx[get_node_id(data["sender"])]
        entry_term = self.node.log.get_entry(self.match_idx[get_node_id(data["sender"])])["term"]

        if entry_idx > self.node.commit_idx:
            if entry_term == self.node.term:
                rep_count = 1
                for idx in self.match_idx.values():
                    if idx >= entry_idx:
                        rep_count += 1

                if self.is_majority(rep_count):
                    self.node.commit_idx = entry_idx

    def write_noop_entry(self):

        # Call at Leader state initialization
        # Create a new, no-op log entry with current term to fulfill entry commitment condition(s)

        self.node.log.write_entry(term=self.node.term,
                                  command={"var": "no-op", "val": ""})

    def send_append_entries_request(self, destination):

        # Send append entry to follower, one entry per request.
        # If follower log doesn't have latest log,
        # append entry will send entry whether matching log idx is found or not.
        # For the sake of simplicity.

        data = {
            "type": "append_entries",
            "term": self.node.term,
            "leader_id": self.node.id,
            "prev_log_idx": self.next_idx[get_node_id(destination)] - 1,
            "prev_log_term": self.node.log.get_entry(self.next_idx[get_node_id(destination)] - 1)["term"],
            "entries": {} if self.next_idx[get_node_id(destination)] > self.node.log.last_log_idx
                    else self.node.log.get_entry(self.next_idx[get_node_id(destination)]),
            "leader_commit": self.node.commit_idx
        }

        # self.waiting_response[data["rid"]] = {"data": data, "destination": destination}

        asyncio.ensure_future(self.node.queue.put({
            "data": data,
            "destination": get_addr_tuple(destination)
        }))

    def send_heartbeat(self):
        for node in self.node.cluster_nodes:
            self.send_append_entries_request(node)

