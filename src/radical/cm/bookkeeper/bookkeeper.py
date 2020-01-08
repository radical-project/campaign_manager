"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""

import radical.utils as ru
from ..planner import HeftPlanner, RandomPlanner

class Bookkeeper(object):

    '''
    This is the Bookkeeping class. It gets the campaign and the resources, calls
    the planner and enacts to the plan.

    *Parameters:*

    *campaign:* The campaign that needs to be executed.
    *resources:* A set of resources.
    *objective:* The campaigns objective
    '''

    def __init__(self, campaign, resources, objective=None, planner='random'):

        self._campaign = campaign
        self._resources = resources
        self._logger = ru.Logger(name='radical.cm.bookkeeper', level='DEBUG')
        if planner.lower() == 'random':
            self._planner = RandomPlanner(campaign=self._campaign,
                                          resources=self._resources,
                                          num_oper=[])  # TODO: fix num_oper
        elif planner.lower() == 'heft':
            self._planner = HeftPlanner((campaign=self._campaign,
                                          resources=self._resources,
                                          num_oper=[])  # TODO: fix num_oper
        else:
            self._logger.warning('Planner %s is not implemented. Rolling to a \
                                  random planner')
            self._planner = RandomPlanner(campaign=self._campaign,
                                          resources=self._resources,
                                          num_oper=[])  # TODO: fix num_oper
        self._enactor = None  # TODO: I need an enactor connected through a cb?

    def execute(self):
        '''
        This is the main method. It calls the planner, sends workflows to the
        enactor and keeps track of the execution.
        '''

        cur_plan = self._planner.plan()

        ## TODO: Verify plan

        for (workflow, resource) in plan:
            self._enector.enact(workflow, resource)
        
