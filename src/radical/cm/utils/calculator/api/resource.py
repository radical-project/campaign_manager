import radical.utils as ru
import numpy as np
from ..entities.core import Core
from ..exceptions import CalcTypeError


class Resource(object):

    def __init__(self, num_cores=1, perf_dist='uniform',
                 dist_mean=10, temporal_var=0, spatial_var=0,
                 no_uid=False):

        # Initialize
        self._uid = None
        self._dist_options = ['uniform', 'normal']
        self._core_list = list()

        if not isinstance(num_cores, int):
            raise CalcTypeError(expected_type=int,
                                actual_type=type(num_cores),
                                entity='num_cores'
                                )
        if not isinstance(perf_dist, str):
            raise CalcTypeError(expected_type=str,
                                actual_type=type(num_cores),
                                entity='perf_dist'
                                )

        if perf_dist not in self._dist_options:
            raise ValueError("possible distributions are %s" %
                             (','.join(self._dist_options)))

        if not (isinstance(dist_mean, int) or isinstance(dist_mean, float)):
            raise CalcTypeError(expected_type='int or float',
                                actual_type=type(dist_mean),
                                entity='dist_mean'
                                )

        if not (isinstance(temporal_var, int) or isinstance(temporal_var, float)):
            raise CalcTypeError(expected_type='int or float',
                                actual_type=type(temporal_var),
                                entity='temporal_var'
                                )

        if not (isinstance(spatial_var, int) or isinstance(spatial_var, float)):
            raise CalcTypeError(expected_type='int or float',
                                actual_type=type(spatial_var),
                                entity='spatial_var'
                                )

        self._num_cores = num_cores
        self._perf_dist = perf_dist
        self._dist_mean = dist_mean
        self._temp_var = temporal_var
        self._spat_var = spatial_var

        if not no_uid:
            self._uid = ru.generate_id('resource')

    @property
    def uid(self):
        return self._uid

    @property
    def num_cores(self):
        return self._num_cores

    @property
    def core_list(self):
        return self._core_list

    def create_core_list(self):

        # Select N samples from the selected distribution. Currently the code
        # creates a set of samples based on the temporal variance on the temporal
        # variance. These samples are then used as means to get the set of cores
        # used for the emulation.

        # FIXME: Based on spatial mean, a spatial variance and a distribution, a
        # set of resources is created. If these resources are dynamic and their 
        # performance varies over time, then each resource's performance needs
        # to be updated based on a distirution and on different moments in time.
        if self._perf_dist == 'uniform':
            samples = list(np.random.uniform(low=self._dist_mean - self._spat_var,
                                        high=self._dist_mean + self._spat_var,
                                        size=self._num_cores))
        elif self._perf_dist == 'normal':
            samples = list(np.random.normal(self._dist_mean, self._spat_var,
                                            self._num_cores))
    

        # Create N execution units with the selected samples
        # some sample in the non uniform distribution might be negative. Those
        # samples should be discarded or folded around 0?
        if not self._core_list:
            self._core_list = [Core(abs(samples[i]), distribution=self._perf_dist,
                                    var=self._temp_var)
                               for i in range(self._num_cores)]
        elif self._temp_var:
            for ind, core in enumerate(self._core_list):
                core.perf = abs(samples[ind])

    def to_dict(self):

        core_list_as_dict = list()
        for core in self._core_list:
            core_list_as_dict.append(core.to_dict())

        return {
            'uid': self._uid,
            'num_cores': self._num_cores,
            'perf_dist': self._perf_dist,
            'dist_mean': self._dist_mean,
            'temp_var': self._temp_var,
            'spat_var': self._spat_var,
            'core_list': core_list_as_dict
        }

    def from_dict(self, entry):

        self._uid = entry['uid']
        self._num_cores = entry['num_cores']
        self._perf_dist = entry['perf_dist']
        self._dist_mean = entry['dist_mean']
        self._temp_var = entry['temp_var']
        self._spat_var = entry['spat_var']

        for core in entry['core_list']:
            c = Core(no_uid=True)
            c.from_dict(core)
            self._core_list.append(c)

    def reset(self):

        for n in self._core_list:
            n.task = None
            n.util = list()
