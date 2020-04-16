"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
from .base import Planner


class HeftPlanner(Planner):
    '''
    This class implemements a campaign planner based on Heterogeneous Earliest
    Finish Time (HEFT) scheduling algorithm. 

    For reference:
    H. Topcuoglu, S. Hariri, and Min-You Wu. Performance-effective and 
    low-complexity task scheduling for heterogeneous computing. 
    IEEE Transactions on Parallel and Distributed Systems, March 2002.

    Constractor parameters:
    campaign: A list of workflows
    resources: A list of resources, whose performance is given in operations per second
    num_oper: The number of operations each workflow will execute

    The class implements a plan method that return a plan, a list of tuples. 

    Each tuple will have the workflow, selected resource, starting time and
    estimated finish time.
    '''
    def __init__(self, campaign, resources, num_oper, sid=None):

        super(HeftPlanner, self).__init__(campaign=campaign,
                                          resources=resources,
                                          num_oper=num_oper, sid=sid)

        # Calculate the estimated execution time of each workflow on to each 
        # resource. This table will be used to calculate the plan.
        # est_tx holds this table. The index of the table is
        # <workflow_idx, resource_idx>, and each entry is the estimated
        # execution time of a workflow on a resource.
        # TODO: not all workflows can run in a resource
        res_perf = list()
        for resource in self._resources:
            res_perf.append(resource['performance'])

        self._est_tx = self._calc_est_tx(cmp_oper=self._num_oper,
                                         resources=res_perf)

    def plan(self, campaign=None, resources=None, num_oper=None, start_time=None,
             **kargs):
        '''
        This method implements the basic HEFT algorithm. It returns a list of tuples
        Each tuple contains: Workflow ID, Resource ID, Start Time, End Time.

        The plan method takes as input a campaign, resources and num_oper in case
        any of these has changed. They default to `None`

        *Returns:*
            list(tuples)
        '''

        tmp_cmp = campaign if campaign else self._campaign
        tmp_res = resources if resources else self._resources
        tmp_nop = num_oper if num_oper else self._num_oper
        res_perf = list()
        for resource in tmp_res:
            res_perf.append(resource['performance'])

        self._est_tx = self._calc_est_tx(cmp_oper=tmp_nop,
                                         resources=res_perf)

        # Reset the plan in case of a recall
        self._plan = list()

        # Calculate the average execution time for all worflows        
        av_est_tx = list()
        for est_tx in self._est_tx:
            av_est_tx.append(sum(est_tx) / len(est_tx))

        # Get the indices of the sorted list.
        av_est_idx_sorted = [i[0] for i in sorted(enumerate(av_est_tx),
                                                  key=lambda x:x[1],
                                                  reverse=True)]

        # This list tracks when a resource whould be available.
        if isinstance(start_time, list):
            resource_free = start_time
        elif isinstance(start_time, float) or isinstance(start_time, int):
            resource_free = [start_time] * len(tmp_res)
        else:
            resource_free = [0] * len(tmp_res)

        for sorted_idx in av_est_idx_sorted:
            wf_est_tx = self._est_tx[sorted_idx]
            min_end_time = float('inf')
            for i in range(len(tmp_res)):
                tmp_str_time = resource_free[i]
                tmp_end_time = tmp_str_time + wf_est_tx[i]
                if tmp_end_time < min_end_time:
                    min_end_time = tmp_end_time
                    tmp_min_idx = i
            self._plan.append((tmp_cmp[sorted_idx],
                               tmp_res[tmp_min_idx],
                               resource_free[tmp_min_idx], 
                               resource_free[tmp_min_idx] +
                                   wf_est_tx[tmp_min_idx]))
            resource_free[tmp_min_idx] = resource_free[tmp_min_idx] + \
                                         wf_est_tx[tmp_min_idx]

        self._logger.info('Derived plan %s', self._plan)
        return self._plan

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
