"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""

from .base import Enactor
from calculator.entities.task import Task

import ..utils.states as st

class EmulatedEnactor(Enactor):
    '''
    The Emulated enactor is responsible to execute workflows on emulated 
    resources. The Enactor takes as input a list of tuples <workflow,resource> 
    and executes the workflows on their selected resources. 
    '''

    def __init__(self):

        super(EmulatedEnactor, self).__init__()
        self._to_monitor = list()

    def _execute(self, workflow, resource):
        '''
        Method executes receives a workflow and a resource. It is responsible to
        start the execution of the workflow and return a endpoint to the WMF that
        executes the workflow

        *workflow:* A workflows that will execute on a resource
        *resource:* The resource that will be used.
        '''

        try:
            exec_workflow = Task(workflows['operations'], no_uid=True)
            resource.execute(exec_workflow)
            self._to_monitor.append(exec_workflow)
            return st.EXECUTING
        except:
            self._logger.error('Workflow %s could not be executed on resource %s',
                               (workflow, resource))
    
    def enact(self, plan):
        '''
        Enact on a set of workflows
        '''
        for workflow, resource in plan:
            if workflow in self._execution_status:
                self._logger.warning('Workflow %s is in state %s', workflow, 
                                     self._get_workflow_state(workflow))
            else:
                self._execution_status[workflow] = self._execute(workflow,
                                                                 resource)

    def _monitor(self):
        '''
        This method monitors the execution of workflows
        '''

        while self._to_monitor:
            workflow = self._to_monitor.pop(0)
            if workflow.exec_core:
                self._execution_status[workflow] = st.DONE
          
        
    def get_status_cb(self, workflows=None):
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
                status[workflow] = self._execution_status[workflow]['state']))
        elif isinstance(workflows, list):
            for workflow in workflows:
                status[workflow] = self._execution_status[workflow]['state']))
        else:
            status[workflow] = self._execution_status[workflow]['state']))

        return status

    def update_status_cb(self, workflow, new_state):
        '''
        Update the state of a workflow that is executing
        '''

        if workflow not in self._execution_status:
            self._logger.warning('Has not enacted on workflow %s yet.',
                                 workflow, self._get_workflow_state(workflow))
        else:
            self._execution_status[workflow]['state'] = new_state

    def _get_workflow_state(self, workflow):
        '''
        Get a workflow's update
        '''

        return self._execution_status[workflow]['state']
