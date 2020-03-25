"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""

# Imports from general packages
import threading as mt

# Imports from dependent packages
import radical.utils as ru
from simpy.core import EmptySchedule
from simpy.events import Process

# Imports from this package
from .base import Enactor
from ..utils import states as st
from ..utils.calculator.entities.task import Task


class SimulatedEnactor(Enactor):
    '''
    The Emulated enactor is responsible to execute workflows on emulated 
    resources. The Enactor takes as input a list of tuples <workflow,resource> 
    and executes the workflows on their selected resources. 
    '''

    def __init__(self, env=None):

        super(SimulatedEnactor, self).__init__()

        # List with all the workflows that are executing and require to be
        # monitored. This list is atomic and requires a lock
        self._to_monitor = list()

        # Lock to provide atomicity in the monitoring data structure
        self._monitoring_lock  = ru.RLock('cm.monitor_lock')
        self._cb_lock          = ru.RLock('enactor.cb_lock')
        self._callbacks        = dict()

        # Creating a thread to execute the monitoring method.
        self._monitoring_thread = None  # Private attribute that will hold the thread
        self._terminate_monitor = mt.Event()  # Thread event to terminate.

        self._sim_env = env
        self._run = False

        self._simulation_thread = mt.Thread(target=self._sim_run,
                                            name='monitor-thread')
        self._terminate_simulation = mt.Event()  # Thread event to terminate.


    def enact(self, workflows, resources):
        '''
        Method enact receives a set workflows and resources. It is responsible to
        start the execution of the workflow and set a endpoint to the WMF that
        executes the workflow

        *workflows:* A workflows that will execute on a resource
        *resources:* The resource that will be used.
        '''
        for workflow, resource in zip(workflows, resources):
            # If the enactor has already received a workflow issue a warning and
            # proceed.
            if workflow['id'] in self._execution_status:
                self._logger.info('Workflow %s is in state %s', workflow, 
                                  st._state_dict[self._get_workflow_state(workflow['id'])])
                continue

            try:
                # Create a calculator task. This is equivalent because with
                # the emulated resources, a workflow is a number of operations
                # that need to be executed.
                exec_workflow = Task(workflow['num_oper'], no_uid=True)
                self._logger.info('Enacting workflow %s on resource %s',
                                        workflow['id'], resource)

                # Lock the monitoring list and update it, as well as update
                # the state of the workflow.
                with self._monitoring_lock:
                    self._to_monitor.append(workflow['id'])
                    self._execution_status[workflow['id']] = {'state': st.EXECUTING,
                                                    'endpoint': exec_workflow,
                                                    'exec_thread': None,
                                                    'start_time': self._sim_env.now,
                                                    'end_time': None}
                for cb in self._callbacks:
                    self._callbacks[cb](workflow_id=workflow['id'],
                                        new_state=st.EXECUTING)
                # Execute the task.
                self._sim_env.process(resource['label'].execute(self._sim_env, exec_workflow))

                self._logger.info('Enacted workflow %s on resource %s',
                                   workflow['id'], resource)
            except:
                self._logger.error('Workflow %s could not be executed on resource %s',
                            (workflow, resource))

        # If there is no monitoring tasks, start one.
        if self._monitoring_thread is None:
            self._logger.info('Starting monitor thread')
            self._monitoring_thread = mt.Thread(target=self._monitor,
                                name='monitor-thread')
            self._monitoring_thread.start()

    def _monitor(self):
        '''
        **Purpose**: Thread in the master process to monitor the campaign execution
                     data structure up to date.
        '''

        while not self._terminate_monitor.is_set():
            if self._to_monitor:
                with self._monitoring_lock:
                    # It does not iterate correctly.
                    workflow_id = self._to_monitor.pop(0)
                self._logger.info('Monitoring workflow %s' % workflow_id)
                if workflow_id in self._execution_status:
                    if self._execution_status[workflow_id]['endpoint'].exec_core:
                        with self._monitoring_lock:
                            self._execution_status[workflow_id]['state'] = st.DONE
                            self._execution_status[workflow_id]['end_time'] = self._execution_status[workflow_id]['endpoint'].end_time
                        self._logger.debug('Workflow %s finished: %s', workflow_id, self._execution_status[workflow_id]['end_time'])
                        for cb in self._callbacks:
                            self._callbacks[cb](workflow_id=workflow_id,
                                                new_state=st.DONE)
                    else:
                        with self._monitoring_lock:
                            self._to_monitor.append(workflow_id)

    def _sim_run(self):

        while not self._terminate_simulation.is_set():
            try:
                if self._run:
                    self._logger.debug('Simulation queue: %s',
                                        self._sim_env._queue)

                    while self._sim_env._queue and \
                          not isinstance(self._sim_env._queue[0][3], Process):
                        self._sim_env.step()

                    while self._sim_env._queue and \
                          isinstance(self._sim_env._queue[0][3], Process):
                        self._sim_env.step()
                    self._run = False
            except EmptySchedule:
                continue


    def get_status(self, workflows=None):
        '''
        Get the state of a workflow or workflows.

        *Parameter*
        *workflows:* A workflow ID or a list of workflow IDs

        *Returns*
        *status*: A dictionary with the state of each workflow.
        '''

        status = dict()
        if workflows is None:
            for workflow in self._execution_status:
                status[workflow] = self._execution_status[workflow]['state']
        elif isinstance(workflows, list):
            for workflow in workflows:
                status[workflow] = self._execution_status[workflow]['state']
        else:
            status[workflow] = self._execution_status[workflow]['state']

        return status

    def update_status(self, workflow, new_state):
        '''
        Update the state of a workflow that is executing
        '''

        if workflow not in self._execution_status:
            self._logger.warning('Has not enacted on workflow %s yet.',
                                 workflow, self._get_workflow_state(workflow))
        else:
            self._execution_status[workflow]['state'] = new_state

    def terminate(self):
        '''
        Public method to terminate the Enactor
        '''
        self._logger.info('Start terminating procedure')
        self._terminate_simulation.set()
        self._simulation_thread.join()
        # self._prof.prof('str_terminating', uid=self._uid)
        if self._monitoring_thread:
            # self._prof.prof('monitor_terminate', uid=self._uid)
            self._terminate_monitor.set()
            self._monitoring_thread.join()
            # self._prof.prof('monitor_terminated', uid=self._uid)

    def register_state_cb(self, cb):
        '''
        Registers a new state update callback function with the Enactor.
        '''

        with self._cb_lock:
            cb_name = cb.__name__
            self._callbacks[cb_name] = cb

    def cont(self):
        '''
        Resume execution
        '''

        self._logger.debug('Setting run at %f', self._sim_env.now)
        self._run = True