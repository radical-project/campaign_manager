"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
import os
import threading as mt
import radical.utils as ru
from ..planner import HeftPlanner, RandomPlanner
from ..utils import states as st
from ..enactor import EmulatedEnactor

class Bookkeeper(object):

    '''
    This is the Bookkeeping class. It gets the campaign and the resources, calls
    the planner and enacts to the plan.

    *Parameters:*

    *campaign:* The campaign that needs to be executed.
    *resources:* A set of resources.
    *objective:* The campaign's objective
    '''

    def __init__(self, campaign, resources, objective=None, planner='random'):

        self._campaign = {'campaign': campaign,
                          'state': st.NEW
                         }
        self._resources = resources
        self._checkpoints = None
        self._plan = None
        self._objective = objective
        self._unavail_resources = []
        self._execution_state = dict()
        self._exec_state_lock  = ru.RLock('exec_state_lock')
        self._monitor_lock = ru.RLock('monitor_list_lock')
        self._time = 0 # The time in the campaign's world.
        self._workflows_to_monitor = list()
        self._enactor = EmulatedEnactor()
        self._enactor.register_state_cb(self.state_update_cb)


        # Creating a thread to execute the monitoring and work methods.
        self._work_thread = None  # Private attribute that will hold the thread
        self._terminate_work = mt.Event()  # Thread event to terminate.
        self._monitoring_thread = None  # Private attribute that will hold the thread
        self._terminate_monitor = mt.Event()  # Thread event to terminate.

        self._logger = ru.Logger(name='radical.cm.bookkeeper', level='DEBUG')
        self._prof   = ru.Profiler(name='radical.cm.bookkeeper',
                                   path=os.getcwd() + '/')
        
        if planner.lower() == 'random':
            self._planner = RandomPlanner(campaign=self._campaign,
                                          resources=self._resources,
                                          num_oper=[])  # TODO: fix num_oper
        elif planner.lower() == 'heft':
            self._planner = HeftPlanner(campaign=self._campaign,
                                          resources=self._resources,
                                          num_oper=[])  # TODO: fix num_oper
        else:
            self._logger.warning('Planner %s is not implemented. Rolling to a \
                                  random planner')
            self._planner = RandomPlanner(campaign=self._campaign,
                                          resources=self._resources,
                                          num_oper=[])  # TODO: fix num_oper
        
    def _update_checkpoints(self):
        '''
        Create a list of timestamps when workflows may start executing or end.
        TODO: this method does not take into account dynamic resources where
              delays in execution affect the makespan.
        '''

        self._checkpoints = [0]

        for work in self._plan:
            if work[3] not in self._checkpoints:
                self._checkpoints.append(work[3])
            if work[4] not in self._checkpoints:
                self._checkpoints.append(work[4])
        
        self._checkpoints.sort()


    def _verify_objective(self):
        '''
        This private method verifies the objective. It takes as input a plan,
        and an objective, calculates when the campaign will actually finish and
        returns True or False.

        *Parameters*
        plan: a plan e.g.: plan = [('W1', 523, 0, 102.5793499043977),
                                   ('W9', 487, 0, 82.13552361396304),
                                   ('W3', 487, 82.13552361396304, 147.22792607802876),
                                   ('W5', 523, 102.5793499043977, 140.82026768642447),
                                   ('W10', 96, 0, 166.66666666666666),
                                   ('W4', 523, 140.82026768642447, 166.0),
                                   ('W2', 487, 147.22792607802876, 170.22792607802876),
                                   ('W7', 523, 166.0, 185.11854684512429),
                                   ('W8', 487, 170.22792607802876, 180.54620123203287),
                                   ('W6', 96, 166.66666666666666, 179.16666666666666)]
        objective: when the campaign should finish in terms of time.

        TODO: Currently the objective is given as a number in some unit of time.
              The objective can be in a more general representation.
        '''

        # Update the checkpoints based on the current state of the execution and
        # the plan

        if self._checkpoints[-1] > self._objective:
            return False
        else:
            return True

    def state_update_cb(self, workflow_id, new_state):
        '''
        This is a state update callback. This callback is passed to the enactor.
        '''

        with self._exec_state_lock:
            self._execution_state[workflow_id] = new_state

    def work(self):
        
        if self._plan is None:
            with self._exec_state_lock:
                self._campaign['state'] = st.PLANNING
            
            num_oper = [workflow['num_oper'] for workflow in self._campaign['campaign']]

            self._plan = self._planner.plan(campaign=self._campaign['campaign'],
                                            resources=self._resources,
                                            num_oper=num_oper)

        self._update_checkpoints()
        
        ## TODO: Verify plan
        while not self._terminate_work.is_set():
            with self._exec_state_lock:
                self._campaign['state'] = st.EXECUTING

            if self._verify_objective():
                workflows = list()  # Workflows to enact
                resources = list()  # The selected resources
                for (workflow, resource, start_time, est_end_time) in self._plan:
                    # Do not enact to workflows that sould have been executed
                    # already.
                    if start_time == self._time and est_end_time > self._time:
                        workflows.append(workflow)
                        resources.append(resource)
                self._enactor.enact(workflows=workflows, resources=resources)
                self._workflows_to_monitor += workflows
                self._unavail_resources += resources

            else:
                self._logger.error("Objective cannot be satisfied. Ending execution")
                with self._exec_state_lock:
                    self._campaign['state'] = st.FAILED
                self._terminate_work.set()
                

    
    def monitor(self):
        '''
        This method monitors the state of the workflows. If the state is one of
        the final states, it removes the workflow from the monitoring list, and
        releases the resource. Otherwise if appends it to the end.
        '''
        while not self._terminate_monitor.is_set():
            with self._monitor_lock:
                while self._workflows_to_monitor:
                    workflow = self._workflows_to_monitor.pop(0)
                    resource = self._unavail_resources.pop(0)
                    if self._execution_state[workflow['id']] not in st.CFINAL:
                        self._workflows_to_monitor.append(workflow)
                        self._unavail_resources.append(resource)

    def get_makespan(self):
        '''
        Returns the makespan of the campaign based on the current state of
        execution
        '''

        self._update_checkpoints()

        return self._checkpoints[-1]

    def run(self):
        '''
        This method starts two threads for executing the campaign. The first
        thread starts the work method. The second thread the monitoring thread.
        '''
        
        # Populate the execution status dictionaty with workflows
        with self._exec_state_lock:
            for workflow in self._campaign:
                self._execution_state[workflow['id']] = st.NEW

        self._logger.info('Starting work thread')
        self._work_thread = mt.Thread(target=self.work,
                                          name='work-thread')
        self._work_thread.start()
        self._logger.info('Starting monitor thread')
        self._monitoring_thread = mt.Thread(target=self.monitor,
                                          name='monitor-thread')
        self._monitoring_thread.start()