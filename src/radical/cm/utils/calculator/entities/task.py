import radical.utils as ru


class Task(object):

    def __init__(self, ops=0, no_uid=False):

        self._uid = None
        if not no_uid:
            self._uid = ru.generate_id('task', mode=ru.ID_PRIVATE)

        self._ops = ops
        self._start_time = None
        self._end_time = None
        self._exec_core = None

    @property
    def uid(self):
        return self._uid

    @property
    def ops(self):
        return self._ops

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def exec_core(self):
        return self._exec_core

    @ops.setter
    def ops(self, val):
        self._ops = val

    @start_time.setter
    def start_time(self, val):
        self._start_time = val

    @end_time.setter
    def end_time(self, val):
        self._end_time = val

    @exec_core.setter
    def exec_core(self, val):
        self._exec_core = val

    def to_dict(self):

        return {'uid': self._uid,
                'ops': self._ops,
                'start_time': self._start_time,
                'end_time': self._end_time,
                'exec_core': self._exec_core
                }

    def from_dict(self, entry):

        self._uid = entry['uid']
        self._ops = entry['ops']
        self._start_time = entry['start_time']
        self._end_time = entry['end_time']
        self._exec_core = entry['exec_core']
