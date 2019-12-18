"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
from __future__ import division
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

    def __init__(self, campaign, resources, num_oper):
        self._campaign = campaign
        self._resources = resources
        self._num_oper = num_oper
        self._plan = list()
        self._logger = ru.Logger(name='radical.cm', level='DEBUG')


    def _calc_est_tx(self, cmp_oper, resources):
        '''
        Calculate the execution time of each workflow on all resources.
        '''

        est_tx = list()
        for wf_oper in cmp_oper:
            tmp_est_tx = list()
            for resource in resources:
                tmp_est_tx.append(float(wf_oper / resource))

            est_tx.append(tmp_est_tx)

        return est_tx

    def plan(self, campaign=None, resources=None, num_oper=None):
        '''
        The planning method
        '''

        raise NotImplementedError('Plan method is not implemented')