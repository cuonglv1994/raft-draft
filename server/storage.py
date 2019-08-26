import ast
import datetime


class PersistentStorage:
    def __init__(self, filename: str):
        self.filename = filename

        # ensure file exist
        open(self.filename, "a").close()

        self.cache = self.read()

    def read(self):
        with open(self.filename, "r") as f:
            return [ast.literal_eval(line) for line in f.readlines()]

    def dump_cache(self):
        with open(self.filename, "w") as storage:
            for data in self.cache:
                storage.writelines(str(data) + "\n")

    def write(self, data: dict):
        with open(self.filename, "a") as storage:
            storage.write(str(data) + "\n")


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

    def __init__(self,node_id : str):
        super().__init__("./raft_{}_entries.log".format(node_id))

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
            return self.cache[log_idx - 1]
        except IndexError:
            return {"term": -1, "command": ''}

    def last_log_idx(self):
        return len(self.cache)

    def last_log_term(self):
        try:
            return self.cache[-1]["term"]
        except IndexError:
            return 0


class PersistentNodeInfo(PersistentStorage):
    def __init__(self, node_id):
        super().__init__("./raft_{}_node_state.log".format(node_id))
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






