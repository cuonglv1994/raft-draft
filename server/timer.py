import asyncio


class Timer:
    """Timer for scheduling tasks """
    def __init__(self, interval, callback):
        self.interval = interval
        self.callback = callback
        self.loop = asyncio.get_event_loop()
        self.handler = None

        self.active = False

    def start(self):
        self.active = True
        self.handler = self.loop.call_later(self.get_interval(), self._run)

    def stop(self):
        self.active = False
        self.handler.cancel()

    def reset(self):
        self.stop()
        self.start()

    def _run(self):
        if self.active:
            self.callback()
            self.handler = self.loop.call_later(self.get_interval(), self._run)

    def get_interval(self):
        return self.interval() if callable(self.interval) else self.interval
