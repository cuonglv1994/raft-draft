from server.storage import PersistentStorage


class SimpleStateMachine(PersistentStorage):

    def __init__(self, node_id):
        super().__init__("./raft_{}_state_machine.log".format(node_id))
        if not self.cache:
            self.cache = [{'last_applied': 0}]

    def apply(self, command, idx):

        #
        # command must follow this format
        # { "var":"var name", "val": "value of var"}
        #

        if not isinstance(command, dict):
            return
        elif 'var' in command.keys() and 'val' in command.keys():
            if command['var'] != 'no-op':
                self.cache[0][command['var']] = command['val']
            self.cache[0]['last_applied'] = idx

        self.dump_cache()

    @property
    def last_applied(self):
        return self.cache[0]['last_applied']
