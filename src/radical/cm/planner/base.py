"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""

import radical.utils as ru

class Planner(object):
    '''
    The planner receives a campaign, a set of resources, and an execution time
    estimation for each workflow per resource, and calculates a plan. The plan is 
    a list of tuples. Each tuple defines at least:
    Workflow: A workflow member of the campaign
    Resource: The resource on which the workflow will be executed.

    Each planning class should always implement a plan method. This method 
    should calculate and return the execution plan. Each class can overleoad the
    basic tuple with additional information based on what the planner is supposed
    to do.
    '''

    def __init__(self, campaign, resources, tx_est):
        self._campaign = campaign
        self._resources = resources
        self._tx_est = tx_est
        self._plan = list()
        self._logger = ru.Logger(name='radical.cm', level='DEBUG')

    def plan(self):
        
        raise NotImplementedError('Plan method is not implemented')