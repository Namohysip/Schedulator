try:
    import Queue as Q  # ver. < 3.0
except ImportError:
    import queue as Q

class Job_Queue(object):
    def __init__(self, priority, job):
        self.priority = priority
        self.job = job
        return

    def __cmp__(self, other):
        return cmp(self.priority, other.priority)