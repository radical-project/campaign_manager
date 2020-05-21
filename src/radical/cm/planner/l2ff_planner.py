"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""

from .base import Planner


class L2FFPlanner(Planner):
    '''
    This class implements a campaign planner based on Largest workflows to 
    fastest resource first.

    Constractor parameters:
    campaign: A list of workflows
    resources: A list of resources, whose performance is given in operations per second
    num_oper: The number of operations each workflow will execute

    The class implements a plan method that return a plan, a list of tuples. 

    Each tuple will have the workflow, selected resource, starting time and
    estimated finish time.
    '''
    def __init__(self, campaign, resources, num_oper, sid=None):

        super(L2FFPlanner, self).__init__(campaign=campaign,
                                          resources=resources,
                                          num_oper=num_oper, sid=sid)

        res_perf = list()
        for resource in self._resources:
            res_perf.append(resource['performance'])
        self._est_tx = self._calc_est_tx(cmp_oper=self._num_oper,
                                         resources=res_perf)

    def plan(self, campaign=None, resources=None, num_oper=None, start_time=None,
             **kargs):
        '''
        This method implements a random algorithm. It returns a list of tuples
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
        for resource in self._resources:
            res_perf.append(resource['performance'])
        self._est_tx = self._calc_est_tx(cmp_oper=tmp_nop,
                                         resources=res_perf)
        # Reset the plan in case of a recall
        self._plan = list()

        # This list tracks when a resource whould be available.
        if isinstance(start_time, list):
            resource_free = start_time
        elif isinstance(start_time, float) or isinstance(start_time, int):
            resource_free = [start_time] * len(tmp_res)
        else:
            resource_free = [0] * len(tmp_res)

        sorted_nop = sorted([nop for nop in enumerate(tmp_nop)],
                            key=lambda nop:nop[1], reverse=True)
        sorted_res = sorted([res for res in enumerate(tmp_res)],
                            key=lambda res: res[1]['performance'],
                            reverse=True)

        for i in range(len(sorted_nop)):
            sel_res = sorted_res[i % len(sorted_res)][0]
            wf_idx = sorted_nop[i][0]
            wf_est_tx = self._est_tx[wf_idx]
            tmp_str_time = resource_free[sel_res]
            tmp_end_time = tmp_str_time + wf_est_tx[sel_res]
            self._plan.append((tmp_cmp[wf_idx], tmp_res[sel_res],
                               tmp_str_time, tmp_end_time))
            resource_free[sel_res] = tmp_end_time

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
