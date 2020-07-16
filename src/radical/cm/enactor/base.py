"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
import os
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

    def __init__(self, sid=None):

        self._worflows = list()  # A list of workflows IDs
        # This will a hash table of workflows. The table will include the
        # following:
        # 'workflowsID': {'state': The state of the workflow based on the WFM,
        #                 'endpoint': Process ID or object to WMF for the specific
        #                             workflow,
        #                 'start_time': Epoch of when the workflow is submitted
        #                               to the WMF,
        #                 'end_time': Epoch of when the workflow finished.}
        self._execution_status = dict()  # This will create a hash table of workflows

        self._uid = ru.generate_id('enactor.%(counter)04d', mode=ru.ID_CUSTOM,
                                    ns=sid)
        path = os.getcwd() + '/' + sid
        name = self._uid

        self._logger = ru.Logger(name=self._uid, path=path, level='DEBUG')
        self._prof   = ru.Profiler(name=name, path=path)


    def enact(self, workflows, resources):
        '''
        Method enact receives a set workflows and resources. It is responsible to
        start the execution of the workflow and set a endpoint to the WMF that
        executes the workflow

        *workflows:* A workflows that will execute on a resource
        *resources:* The resource that will be used.
        '''
        raise NotImplementedError('enact is not implemented')

    def _monitor(self):
        '''
        This method monitors the execution of workflows
        '''

        raise NotImplementedError('_monitor is not implemented')

    def get_status(self, workflows=None):
        '''
        Get the state of a workflow or workflows
        '''

        status = list()
        if workflows is None:
            for workflow, status in self._execution_status.items():
                status.append((workflow, self._execution_status[workflow]['state']))
        elif isinstance(workflows, list):
            status = list()
            for workflow in workflows:
                status.append((workflow, self._execution_status[workflow]['state']))
        else:
            status = (workflow, self._execution_status[workflow]['state'])

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

    def terminate(self):
        '''
        Public method to terminate the Enactor
        '''
        raise NotImplementedError('terminate is not implemented')
