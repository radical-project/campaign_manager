"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""

import radical.utils as ru
from calculator import Resource
from .base import Enactor

class EmulatedEnactor(Enactor):
    '''
    The Emulated enactor is responsible to execute workflows on emulated 
    resources. The Enactor takes as input a list of tuples <workflow,resource> 
    and executes the workflows on their selected resources. 

    *Parameters:*

    *num_resources:* The number of resources that are available to execute 
                   workflows on.

    *dist:* Distribution from which the performance of a resource is drawn.
          Currently supports uniform and normal distributions. *Default
          value:* uniform

    *mean_perf:* Average performance of all resources.

    *spatial_var:* The variance of the distribution that resources are being
                 drawn. *Default value* 0 (selects homogeneous resources)

    *temp_var:* The temporal variance of each resource. *Default value* 0 
              (selects static resources). In addition, the distribution is dist.
    '''

    def __init__(self, num_resources, mean_perf, spatial_var=0, temp_var=0,
                 dist='uniform'):

        super(EmulatedEnactor, self).__init__()
        self._resources = dict()
        resources = Resource(num_cores=num_resources,
                                   perf_dist=dist,
                                   dist_mean=mean_perf,
                                   temporal_var=temp_var,
                                   spatial_var=spatial_var)
        resources.create_core_list()
        for idx, resource in enumerate(resources.core_list):
            self._resources[idx] = resource

        self._uid = ru.generate_id('rcm.enactor', mode=ru.ID_PRIVATE)
    
    def query_resources(self):
        '''
        Provides performance information on resources
        '''
        
        query = dict()
        for idx, resource in self._resources.items():
            tmp = resource.to_dict()
            query[idx] = tmp['perf']
        
        return query

    def _execute(self, workflow, resource):
        '''
        Method executes receives a workflow and a resource. It is responsible to
        start the execution of the workflow and return a endopint to the WMF that
        executes the workflow
        '''

