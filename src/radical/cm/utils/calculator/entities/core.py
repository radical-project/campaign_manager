import radical.utils as ru
import numpy as np


class Core(object):

    def __init__(self, perf=0, distribution='uniform', var=0, no_uid=False):

        self._uid = None
        if not no_uid:
            self._uid = ru.generate_id('core', mode=ru.ID_PRIVATE)
        self._perf = perf
        self._util = list()
        self._task_history = list()
        self._dist = distribution
        self._var = var

    @property
    def uid(self):
        return self._uid

    @property
    def perf(self):
        return self._perf

    @property
    def util(self):
        return self._util

    @property
    def task_history(self):
        return self._task_history

    @property
    def dist(self):
        return self._dist

    @property
    def var(self):
        return self._var

    @perf.setter
    def perf(self, val):
        self._perf = val

    @util.setter
    def util(self, val):
        self._util = val

    @task_history.setter
    def task_history(self, val):
        self._task_history = val

    @dist.setter
    def dist(self, distribution):
        self._dist = distribution

    @var.setter
    def var(self, var):
        self._var = var

    def execute(self, env, task):

        if self._var:
            if self._dist == 'uniform':
                tmp_perf = np.random.uniform(low=self._perf - self._var,
                                        high=self._perf + self._var,)
            elif self._dist == 'normal':
                tmp_perf = np.random.normal(self._perf, self._var)
        else:
            tmp_perf = self._perf
        dur = task.ops / tmp_perf

        task.start_time = env.now
        task.end_time = task.start_time + dur
        self._util.append([task.start_time, task.end_time])
        self._task_history.append(task.uid)
        yield env.timeout(dur)
        task.exec_core = self._uid

    def to_dict(self):

        return {'uid': self._uid,
                'perf': self._perf,
                'var': self._var,
                'dist': self._dist,
                'util': self._util,
                'task_history': self._task_history
                }

    def from_dict(self, entry):

        self._uid = entry['uid']
        self._perf = entry['perf']
        self._util = entry['util']
        self._task_history = entry['task_history']
        self._dist = entry['dist']
        self._var = entry['var']
