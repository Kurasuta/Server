import time


class Mark(object):
    def __init__(self, perf_count, process_time, caption):
        self.perf_count = perf_count
        self.process_time = process_time
        self.caption = caption

    def __repr__(self):
        return '<Mark %s perf_count=%s process_time=%s>' % (self.caption, self.perf_count, self.process_time)


class PerformanceTimer(list):
    def __init__(self, logger):
        super().__init__()
        self.logger = logger
        self.mark('__init__')

    def mark(self, caption):
        mark = Mark(time.perf_counter(), time.process_time(), caption)
        self.logger.debug(mark)
        self.append(mark)


class SubTimer(object):
    def __init__(self, super_timer):
        self.super_timer = super_timer
        self.super_caption = '' if isinstance(super_timer, NullTimer) else self.super_timer[-1].caption

    def mark(self, caption):
        self.super_timer.mark('%s_%s' % (self.super_caption, caption))


class NullTimer(object):
    def mark(self, caption):
        pass
