"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
import os
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

    def __init__(self, campaign, resources, num_oper, sid=None):
        self._campaign = campaign
        self._resources = resources
        self._num_oper = num_oper
        self._plan = list()
        self._uid = ru.generate_id('planner.%(counter)04d', mode=ru.ID_CUSTOM,
                                    ns=sid)
        path = os.getcwd() + '/' + sid
        self._logger = ru.Logger(name=self._uid, level='DEBUG', path=path)


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

    def plan(self, campaign=None, resources=None, num_oper=None, start_time=0,
             **kargs):
        '''
        The planning method
        '''

        raise NotImplementedError('Plan method is not implemented')

    def replan(self, campaign=None, resources=None, num_oper=None, start_time=0):
        '''
        The planning method
        '''
        if campaign and resources and num_oper:
            self._logger.debug('Replanning')
            self._plan = self.plan(campaign=campaign, resources=resources,
                                   num_oper=num_oper, start_time=start_time)
        else:
            self._logger.debug('Nothing to plan for')

        return self._plan
