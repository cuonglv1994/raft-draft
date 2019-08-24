import ast
import datetime


class PersistentStorage:
    def __init__(self, filename: str):
        self.filename = filename

        # ensure file exist
        open(self.filename, 'a').close()

        self.cache = self.read()

    def read(self):
        with open(self.filename, 'r') as f:
            return [ast.literal_eval(line) for line in f.readlines()]

    def write(self, data: dict):
        with open(self.filename,'a') as storage:
            storage.writelines(str(data))
        return data


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
        super().__init__('./raft_{}_entries.log'.format(node_id))

    def write_entry(self, term, command):

        entry = {
            "term": term,
            "command": command
        }

        self.cache.append(entry)

        self.write(entry)

        return entry

    def last_log_idx(self):
        return len(self.cache)

    def last_log_term(self):
        try:
            return self.cache[-1]['term']
        except IndexError:
            return 0


class PersistentNodeInfo(PersistentStorage):
    def __init__(self, node_id):
        super().__init__('./raft_{}_node_state.log'.format(node_id))
        self.term, self.voted_for, _ = self.latest_info()

    def update_info(self):
        self.write({'term': self.term,
                    'voted_for': self.voted_for,
                    'timestamp': datetime.datetime.timestamp(datetime.datetime.now())
                    })

    def latest_info(self):
        try:
            return self.cache[-1]
        except IndexError:
            return {'term': 0, 'voted_for': None}







