"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""

import time
import threading as mt
import radical.utils as ru

from simpy import Environment
from simpy import Interrupt as simInterrupt

from ..planner import HeftPlanner, RandomPlanner
from ..utils import states as st
from ..enactor import SimulatedEnactor

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
        self._uid = ru.generate_id('rcm.bookkeeper', mode=ru.ID_PRIVATE)
        self._resources = resources
        self._checkpoints = None
        self._plan = None
        self._objective = objective
        self._unavail_resources = []
        self._workflows_state = dict()

        self._exec_state_lock  = ru.RLock('workflows_state_lock')
        self._monitor_lock = ru.RLock('monitor_list_lock')
        self._time = 0 # The time in the campaign's world.
        self._workflows_to_monitor = list()
        self._env = Environment()
        self._enactor = SimulatedEnactor(env=self._env)
        self._enactor.register_state_cb(self.state_update_cb)


        # Creating a thread to execute the monitoring and work methods.
        # One flag for both threads may be enough  to monitor and check.
        self._terminate_event = mt.Event()  # Thread event to terminate.
        self._work_thread = None  # Private attribute that will hold the thread
        self._monitoring_thread = None  # Private attribute that will hold the thread
        
        self._logger = ru.Logger(name=self._uid, level='DEBUG')
        #self._prof   = ru.Profiler(name='radical.cm.bookkeeper',
        #                           path=os.getcwd() + '/')
        
        num_oper = [workflow['num_oper'] for workflow in self._campaign['campaign']]
        if planner.lower() == 'random':
            self._planner = RandomPlanner(campaign=self._campaign['campaign'],
                                          resources=self._resources,
                                          num_oper=num_oper)
        elif planner.lower() == 'heft':
            self._planner = HeftPlanner(campaign=self._campaign['campaign'],
                                          resources=self._resources,
                                          num_oper=num_oper)
        else:
            self._logger.warning('Planner %s is not implemented. Rolling to a \
                                  random planner')
            self._planner = RandomPlanner(campaign=self._campaign['campaign'],
                                          resources=self._resources,
                                          num_oper=num_oper)
        
    def _update_checkpoints(self):
        '''
        Create a list of timestamps when workflows may start executing or end.
        TODO: this method does not take into account dynamic resources where
              delays in execution affect the makespan.
        '''

        self._checkpoints = [0]

        for work in self._plan:
            if work[2] not in self._checkpoints:
                self._checkpoints.append(work[2])
            if work[3] not in self._checkpoints:
                self._checkpoints.append(work[3])
        
        self._checkpoints.sort()


    def _verify_objective(self):
        '''
        This private method verifies the objective. It check the estimated makespan of the campaign and compares it with the objective. If no objective is defined or the campaign is estimated tp satisfy the objective
        it returns `True`, else ot returns `False`.

        TODO: Currently the objective is given as a number in some unit of time.
              The objective can be in a more general representation.
        '''

        if not self._objective:
            return True

        if self._checkpoints[-1] > self._objective:
            return False
        else:
            return True

    def state_update_cb(self, env, workflow_id, new_state):
        '''
        This is a state update callback. This callback is passed to the enactor.
        '''

        with self._exec_state_lock:
            self._workflows_state[workflow_id] = new_state
            if new_state == st.CFINAL:
                env.process.interrupt()

    def work(self):
        '''
        This method is responsible to execute the campaign.
        '''
        
        # There is no need to check since I know there is no plan.
        if self._plan is None:
            with self._exec_state_lock:
                self._campaign['state'] = st.PLANNING
            self._plan = self._planner.plan()
            self._logger.debug('Calculated plan: %s', self._plan)
    
        self._update_checkpoints()
        

        with self._exec_state_lock:
            self._campaign['state'] = st.EXECUTING

        while not self._terminate_event.is_set():
            if self._verify_objective():
                workflows = list()  # Workflows to enact
                resources = list()  # The selected resources
                for (workflow, resource, start_time, est_end_time) in self._plan:
                    # Do not enact to workflows that sould have been executed
                    # already.
                    if start_time == self._env.now and est_end_time > self._env.now:
                        workflows.append(workflow)
                        resources.append(resource)
                        self._logger.debug('Time: %s: Enacting %s workflow on %s resource',
                                           self._env.now, workflow, resource)
                self._enactor.enact(workflows=workflows, resources=resources)
                with self._monitor_lock:
                    self._workflows_to_monitor += workflows
                    self._unavail_resources += resources
                self._env.run()

            else:
                self._logger.error("Objective cannot be satisfied. Ending execution")
                with self._exec_state_lock:
                    self._campaign['state'] = st.FAILED
                    self._terminate_event.set()
                

    
    def monitor(self):
        '''
        This method monitors the state of the workflows. If the state is one of
        the final states, it removes the workflow from the monitoring list, and
        releases the resource. Otherwise if appends it to the end.
        '''
        while not self._terminate_event.is_set():
            with self._monitor_lock:
                while self._workflows_to_monitor:
                    try:
                        workflow = self._workflows_to_monitor.pop(0)
                        resource = self._unavail_resources.pop(0)
                        if self._workflows_state[workflow['id']] not in st.CFINAL:
                            self._workflows_to_monitor.append(workflow)
                            self._unavail_resources.append(resource)
                    except simInterrupt:
                        continue


    def get_makespan(self):
        '''
        Returns the makespan of the campaign based on the current state of
        execution
        '''

        self._update_checkpoints()

        return self._checkpoints[-1]

    def _terminate(self):

        self._logger.info('Start terminating procedure')
        #self._prof.prof('str_bookkeper_terminating', uid=self._uid)
        
        # Terminate enactor as well.
        self._enactor.terminate()

        # Terminate your threads.

        self._terminate_event.set()  # Thread event to terminate.


        if self._work_thread:
            #self._prof.prof('work_bookkeper_terminate', uid=self._uid)
            self._work_thread.join()  # Private attribute that will hold the thread
            #self._prof.prof('work_bookkeper_terminated', uid=self._uid)
        
        if self._monitoring_thread:
            #self._prof.prof('monitor_bookkeper_terminate', uid=self._uid)
            self._monitoring_thread.join()
            #self._prof.prof('monitor_bookkeper_terminated', uid=self._uid)

    def run(self):
        '''
        This method starts two threads for executing the campaign. The first
        thread starts the work method. The second thread the monitoring thread.
        '''
        
        # Populate the execution status dictionary with workflows
        with self._exec_state_lock:
            for workflow in self._campaign['campaign']:
                self._workflows_state[workflow['id']] = st.NEW

        self._logger.info('Starting work thread')
        self._work_thread = mt.Thread(target=self.work,
                                          name='work-thread')
        self._work_thread.start()
        self._logger.info('Starting monitor thread')
        self._monitoring_thread = mt.Thread(target=self.monitor,
                                          name='monitor-thread')
        self._monitoring_thread.start()

        # This waits regardless if workflows are failing or not. This loop can
        # do meaningful work such as checking the state of the campaign. It can
        # be a while true, until something happens.
        self._logger.debug('Time now: %s, checkpoints: %s', self._time, self._checkpoints)
        while self._checkpoints is None:
            continue

        while self._time < self._checkpoints[-1] and \
              self._campaign['state'] not in st.CFINAL:
            self._time = self._env.now

            for workflow in self._campaign['campaign']:
                if self._workflows_state[workflow['id']] is st.FAILED:
                    self._campaign['state'] = st.FAILED
                    break
        
        if self._campaign['state'] not in st.CFINAL:
            self._campaign['state'] = st.DONE

        self._terminate()

    def get_campaign_state(self):

        return self._campaign['state']

    def get_workflows_state(self):

        states = dict()
        for workflow in self._campaign['campaign']:
            states[workflow['id']] = self._workflows_state[workflow['id']]

        return states