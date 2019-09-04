import json
import datetime
import os


class PersistentStorage:
    def __init__(self, path):
        self.path = path
        dir_path = os.path.dirname(self.path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        # ensure file exist
        open(self.path, "a").close()

        self.cache = self.read()

    def read(self):
        with open(self.path, "r") as f:
            return [json.loads(line) for line in f.readlines() if line.strip()]

    def dump_cache(self):
        with open(self.path, "w") as storage:
            for data in self.cache:
                storage.writelines(json.dumps(data) + "\n")

    def write(self, data: dict):
        with open(self.path, "a") as storage:
            storage.write(json.dumps(data) + "\n")


class Log(PersistentStorage):

    """
    Raft
    Log, format  #2
    Entries:
    log entry idx: line num / idx in log list
    {term: < int >, command: < command >}
    {term: < int >, command: < command >}
    .....
    """

    def __init__(self, node_id):
        super().__init__("storage/raft_{}_entries.log".format(node_id))

    def write_entry(self, term, command):

        entry = {
            "term": term,
            "command": command
        }

        self.cache.append(entry)

        self.write(entry)

        return entry

    def delete_from_idx(self, idx):
        self.cache = self.cache[:idx]
        self.dump_cache()

    def get_entry(self, log_idx):
        try:
            if log_idx <= 0:
                raise IndexError
            return self.cache[log_idx - 1]
        except IndexError:
            return {"term": 0, "command": ''}

    @property
    def last_log_idx(self):
        return len(self.cache)

    @property
    def last_log_term(self):
        try:
            return self.cache[-1]["term"]
        except IndexError:
            return 0


class PersistentNodeInfo(PersistentStorage):
    def __init__(self, node_id):
        super().__init__("storage/raft_{}_node_state.log".format(node_id))
        self.term = 0
        self.voted_for = None
        for k, v in self.latest_info().items():
            if k != 'timestamp':
                setattr(self, k, v)

    def update_info(self, term=None, voted_for=None):

        self.term = term if term else self.term
        self.voted_for = voted_for if voted_for else self.voted_for

        self.write({"term": self.term,
                    "voted_for": self.voted_for,
                    "timestamp": datetime.datetime.timestamp(datetime.datetime.now())
                    })

    def latest_info(self):
        try:
            return self.cache[-1]
        except IndexError:
            return {"term": 0, "voted_for": None, "timestamp": 0}
