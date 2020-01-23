"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""

# Imports from general packages
from time import time, sleep
import threading as mt

# Imports from dependent packages
import radical.utils as ru
import radical.entk as re

# Imports from this package
from .base import Enactor
from ..utils import states as st

class RadicalEnTkEnactor(Enactor):
    '''
    The RADICAL-EnTK enactor is responsible to execute workflows on resources via
    RADICAL-EnTK. The Enactor takes as input a list of tuples <workflow,resource> 
    and executes the workflows on their selected resources. 
    '''

    def __init__(self):

        super(EmulatedEnactor, self).__init__()

        # List with all the workflows that are executing and require to be
        # monitored. This list is atomic and requires a lock
        self._to_monitor = list()

        # Lock to provide atomicity in the monitoring data structure
        self._monitoring_lock  = ru.RLock('cm.monitor_lock')

        # Creating a thread to execute the monitoring method.
        self._monitoring_thread = None  # Private attribute that will hold the thread
        self._terminate_monitor = mt.Event()  # Thread event to terminate.

   
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
            if workflow in self._execution_status:
                self._logger.warning('Workflow %s is in state %s', workflow, 
                                     self._get_workflow_state(workflow))
            else:
                try:
                    # Create a calculator task. This is equivalent because with
                    # the emulated resources, a workflow is a number of operations
                    # that need to be executed.
                    exec_workflow = Task(workflow['operations'], no_uid=True)

                    # Lock the monitoring list and update it, as well as update
                    # the state of the workflow.
                    with self._monitoring_lock:
                        self._to_monitor.append(workflow['id'])
                        self._execution_status[workflow['id']] = {'state': st.EXECUTING,
                                                        'endpoint': exec_workflow,
                                                        'start_time': time(),
                                                        'end_time': None}

                    # Execute the task.
                    resource.execute(exec_workflow)
                    
                    # If there is no monitoring tasks, start one.
                    if self._monitoring_thread is None:
                        self._logger.info('Starting monitor thread')
                        self._monitoring_thread = mt.Thread(target=self._monitor,
                                          name='monitor-thread')
                        self._monitoring_thread.start()

                except:
                    self._logger.error('Workflow %s could not be executed on resource %s',
                               (workflow, resource))

    def _monitor(self):
        '''
        **Purpose**: Thread in the master process to monitor the campaign execution
                     data structure up to date.
        '''

        while not self._terminate_monitor.is_set():
            if self._to_monitor:
                with self._monitoring_lock:
                    workflow_id = self._to_monitor.pop(0)
                    if workflow_id in self._execution_status:
                        if self._execution_status[workflow]['endpoint'].exec_core:
                            self._execution_status[workflow]['state'] = st.DONE
                            self._execution_status[workflow]['end_time'] = self._execution_status[workflow]['endpoint'].end_time
            else:
                sleep(1)
          
        
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
        self._prof.prof('str_terminating', uid=self._uid)
        if self._monitoring_thread:
            self._prof.prof('monitor_terminate', uid=self._uid)
            self._terminate_monitor.set()
            self._monitoring_thread.join()
            self._prof.prof('monitor_terminated', uid=self._uid)
        
    def run(self):
        '''
        Public method that starts the enactor.
        '''

        #TODO.