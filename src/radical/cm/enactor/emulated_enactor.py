"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""

from .base import Enactor
import ...utils.states as st


class EmulatedEnactor(Enactor):
    '''
    The Emulated enactor is responsible to execute workflows on emulated 
    resources. The Enactor takes as input a list of tuples <workflow,resource> 
    and executes the workflows on their selected resources. 
    '''

    def __init__(self):

        super(EmulatedEnactor, self).__init__()

    def _execute(self, workflow, resource):
        '''
        Method executes receives a workflow and a resource. It is responsible to
        start the execution of the workflow and return a endpoint to the WMF that
        executes the workflow

        *workflow:* A workflows that will execute on a resource
        *resource:* The resource that will be used.
        '''

        try:
            resource.execute(workflow)
            return st.EXECUTING
        except:
            self._logger.error('Workflow %s could not be executed on resource %s',
                               (workflow, resource))