import time
import math


class Mark(object):
    def __init__(self, name, milliseconds_since_start):
        self.name = name
        self.milliseconds_since_start = milliseconds_since_start

    def __repr__(self):
        return '<Mark %s: %i>' % (self.name, self.milliseconds_since_start)


class Profiler(object):
    @staticmethod
    def _get_milliseconds():
        return int(math.floor(round(time.time() * 1000)))

    def __init__(self):
        self.marks = []
        self.start_timestamp = self._get_milliseconds()

    def mark(self, name):
        self.marks.append(Mark(name, self.start_timestamp - self._get_milliseconds()))
