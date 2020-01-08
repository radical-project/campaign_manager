"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
import radical.utils as ru


class Enactor(object):
    '''
    The Enactor is responsible to execute workflows on resources. The Enactor
    takes as input a list of tuples <workflow,resource> and executes the 
    workflows on their selected resources. 

    The Enactor offers a set of methods to execute and monitor workflows.

    *Parameters:*

    *workflows*: A list with the workflow IDs that are executing.

    *execution_status*: a hash table table that holds the state, and 
                      execution status.

    *logger*: a logging object.
    '''

    def __init__(self):

        self._worflows = list()  # A list of workflows IDs
        self._execution_status = dict()  # This will create a hash table of workflows
        self._logger = ru.Logger(name='radical.cm.enactor', level='DEBUG')
        self._uid = ru.generate_id('rcm.enactor', mode=ru.ID_PRIVATE)
    

    def _execute(self, workflow, resource):
        '''
        Method executes receives a workflow and a resource. It is responsible to
        start the execution of the workflow and return a endopint to the WMF that
        executes the workflow
        '''

        raise NotImplementedError('_execute is not implemented')

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

    def get_status(self, workflows=None):
        '''
        Get the state of a workflow or workflows
        '''

        if workflows is None:
            status = list()
            for workflow, status in self._execution_status.items():
                status.append((workflow, self._execution_status[workflow]['state']))
        elif isinstance(workflows, list):
            status = list()
            for workflow in workflows:
                status.append((workflow, self._execution_status[workflow]['state']))
        else:
            status = self._execution_status[workflow]['state']

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
