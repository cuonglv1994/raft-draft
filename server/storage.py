import json


class Log:
    """
    Raft Log
    Entries:
    {term: <int>, idx: <int>, command: <command>}
    {term: <int>, idx: <int>, command: <command>}
    .....
    """

    def __init__(self,node_id):
        self.filename = './raft_{}_entries.log'.format(node_id)

        open(self.filename,'a').close()

        self.cache = self.read()

    def read(self):
        with open(self.filename) as f:
            return [json.loads(line) for line in f.readlines()]

    def write(self, term, idx, command):
        with open(self.filename, 'a') as f:
            entry = {
                "term": term,
                "idx": idx,
                "command": command
            }

            f.write(json.dumps(entry) + "\n")
        self.cache.append(entry)
        return entry

    def last_log_idx(self):
        try:
            return self.cache[-1]['idx']
        except IndexError:
            return 0

    def last_log_term(self):
        try:
            return self.cache[-1]['term']
        except IndexError:
            return 0








