from functools import wraps


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
    def __init__(self):
        self.current_term = 0
        self.voted_for = None
        self.logs = []

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


class Follower(BaseState):
    def __init__(self, *args, **kwargs):
        super().__init__()
        pass

    def on_receive_append_entries(self):
        pass

    def on_receive_vote_request(self):
        pass


class Candidate():
    def __init__(self, *args, **kwargs):
        pass

    def on_receive_append_entries(self):
        pass

    def on_receive_vote_request(self):
        pass

    def send_vote_request(self):
        pass

class Leader():
    def __init__(self, *args, **kwargs):
        pass

    def on_receive_append_entries(self):
        pass

    def on_receive_vote_request(self):
        pass

    def on_receive_append_entries_response(self):
        pass

