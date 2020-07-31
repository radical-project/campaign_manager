"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
import os
import threading as mt
import radical.utils as ru

from copy import deepcopy
from time import sleep
from simpy import Environment

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

    def __init__(self, campaign, resources, objective=None, planner='random',
                 sid=None):

        self._campaign = {'campaign': campaign,
                          'state': st.NEW
                         }
        if sid:
            self._sid = sid
        else:
            self._sid = ru.generate_id('rcm.session', mode=ru.ID_PRIVATE)
        self._uid = ru.generate_id('bookkeper.%(counter)04d',
                                   mode=ru.ID_CUSTOM, ns=self._sid)

        self._resources = resources
        self._checkpoints = None
        self._plan = None
        self._objective = objective
        self._unavail_resources = []
        self._workflows_state = dict()

        self._exec_state_lock  = ru.RLock('workflows_state_lock')
        self._monitor_lock = ru.RLock('monitor_list_lock')
        self._time = 0  # The time in the campaign's world.
        self._workflows_to_monitor = list()
        self._est_end_times = dict()
        self._env = Environment()
        self._enactor = SimulatedEnactor(env=self._env, sid=self._sid)
        self._enactor.register_state_cb(self.state_update_cb)


        # Creating a thread to execute the monitoring and work methods.
        # One flag for both threads may be enough  to monitor and check.
        self._terminate_event = mt.Event()  # Thread event to terminate.
        self._work_thread = None  # Private attribute that will hold the thread
        self._monitoring_thread = None  # Private attribute that will hold the thread
        self._cont = False
        self._hold = False

        path = os.getcwd() + '/' + self._sid

        self._logger = ru.Logger(name=self._uid, path=path, level='DEBUG')
        self._prof   = ru.Profiler(name=self._uid, path=path)

        num_oper = [workflow['num_oper'] for workflow in self._campaign['campaign']]
        if planner.lower() == 'random':
            self._planner = RandomPlanner(campaign=self._campaign['campaign'],
                                          resources=self._resources,
                                          num_oper=num_oper, sid=self._sid)
        elif planner.lower() == 'heft':
            self._planner = HeftPlanner(campaign=self._campaign['campaign'],
                                          resources=self._resources,
                                          num_oper=num_oper, sid=self._sid)
        else:
            self._logger.warning('Planner %s is not implemented. Rolling to a \
                                  random planner')
            self._planner = RandomPlanner(campaign=self._campaign['campaign'],
                                          resources=self._resources,
                                          num_oper=num_oper, sid=self._sid)

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
        This private method verifies the objective. It check the estimated 
        makespan of the campaign and compares it with the objective. If no 
        objective is defined or the campaign is estimated to satisfy the objective
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

    def state_update_cb(self, workflow_ids, new_state):
        '''
        This is a state update callback. This callback is passed to the enactor.
        '''
        self._logger.debug('Workflow %s to state %s', workflow_ids, new_state)
        with self._exec_state_lock:
            for workflow_id in workflow_ids:
                self._workflows_state[workflow_id] = new_state
        #if new_state is st.DONE:
        #    self._hold = True

    def work(self):
        '''
        This method is responsible to execute the campaign.
        '''

        # There is no need to check since I know there is no plan.
        self._prof.prof('planning_start', uid=self._uid)
        if self._plan is None:
            with self._exec_state_lock:
                self._campaign['state'] = st.PLANNING
            self._plan = self._planner.plan()
            self._plan = sorted([place for place in self._plan], key=lambda place:place[-1])
            # self._logger.debug('Calculated plan: %s', self._plan)
        self._prof.prof('planning_ended', uid=self._uid)

        self._update_checkpoints()

        with self._exec_state_lock:
            self._campaign['state'] = st.EXECUTING

        self._prof.prof('work_start', uid=self._uid)
        while not self._terminate_event.is_set():
            if not self._verify_objective():
                self._logger.error("Objective cannot be satisfied. Ending execution")
                with self._exec_state_lock:
                    self._campaign['state'] = st.FAILED
                    self._terminate()
            else:
                
                self._prof.prof('work_submit', uid=self._uid)
                workflows = list()  # Workflows to enact
                resources = list()  # The selected resources
                self._logger.debug('Checking workflows %s', self._hold)
                while (not self._cont) or self._hold:
                    continue
                
                for (wf, rc, start_time, est_end_time) in self._plan:
                    # Do not enact to workflows that sould have been executed
                    # already.
                    if start_time == self._time and est_end_time > self._time \
                        and rc not in self._unavail_resources and self._cont:
                        workflows.append(wf)
                        resources.append(rc)
                        self._est_end_times[rc['id']] = est_end_time
                        self._logger.debug('Time: %s: Enacting %s workflow on %s resource. Will end %f',
                                           self._env.now, wf, rc, est_end_time)

                # There is no need to call the enactor when no new things
                # should happen.
                #self._logger.debug('Adding items: %s, %s', workflows, resources)
                if workflows and resources:

                    self._prof.prof('enactor_submit', uid=self._uid)
                    self._enactor.enact(workflows=workflows, resources=resources)
                    self._prof.prof('enactor_submitted', uid=self._uid)
                    with self._monitor_lock:
                        self._workflows_to_monitor += workflows
                        self._unavail_resources += resources
                    #self._logger.debug('Things monitored: %s, %s, %s',
                    #                    self._workflows_to_monitor,
                    #                    self._unavail_resources,
                    #                    self._est_end_times)

                # Inform the enactor to continue until everything ends.
                self._prof.prof('enactor_cont', uid=self._uid)
                remain = True
                for workflow in self._campaign['campaign']:
                    if self._workflows_state[workflow['id']] == st.NEW:
                        remain = False
                self._logger.debug('remain: %s, continue: %s, hold: %s', remain,
                                   self._cont, self._hold)
                if (remain or self._cont) and not self._hold:
                    self._logger.debug("Let's keep going")
                    self._enactor.cont()
                    self._cont = False
                    self._hold = True
                    self._logger.debug('Stop execution')
                    sleep(1)
                else:
                    self._logger.debug('Still running on its own')
                self._prof.prof('work_submitted', uid=self._uid)

    def monitor(self):
        '''
        This method monitors the state of the workflows. If the state is one of
        the final states, it removes the workflow from the monitoring list, and
        releases the resource. Otherwise if appends it to the end.
        '''
        while not self._terminate_event.is_set():
            while self._workflows_to_monitor:
                self._prof.prof('workflow_monitor', uid=self._uid)
                workflows = deepcopy(self._workflows_to_monitor)
                finished = list()
                tmp_start_times = list()
                for i in range(len(workflows)):
                    if self._workflows_state[workflows[i]['id']] in st.CFINAL:
                        resource = self._unavail_resources[i]
                        finished.append((workflows[i], resource))
                        if self._env.now == self._est_end_times[resource['id']]:
                            self._logger.info('Workflow %s finished at expected time',
                                                workflows[i]['id'])
                        else:
                            self._logger.debug('Workflow %s finished %f, expected %f.' +
                                                'Need to Replanning.',
                                                workflows[i]['id'],
                                                self._env.now,
                                                self._est_end_times[resource['id']])

                            # Creates an array with the expected free time of each
                            # resource. The resource that was just freed will
                            # use the time now.
                            for res in self._resources:
                                if res == resource:
                                    tmp_start_times.append(self._env.now)
                                else:
                                    tmp_start_times.append(self._est_end_times[res['id']])

                if finished:
                    with self._monitor_lock:
                        for workflow, resource in finished:
                            self._workflows_to_monitor.remove(workflow)
                            self._unavail_resources.remove(resource)

                if tmp_start_times:
                    self._prof.prof('replan_start', uid=self._uid)
                    # Creates an array of the workflows that have not
                    # started executing yet.
                    tmp_campaign = list()
                    for workflow in self._campaign['campaign']:
                        if self._workflows_state[workflow['id']] == st.NEW:
                            tmp_campaign.append(workflow)

                    tmp_num_oper = [workflow['num_oper'] for workflow in tmp_campaign]
#                    self._logger.debug('Replanning for: %s, %s, %s',
#                                        tmp_start_times,
#                                        tmp_campaign,
#                                        tmp_num_oper)

                    self._prof.prof('replan_run', uid=self._uid)
                    tmp_plan = self._planner.replan(campaign=tmp_campaign,
                            resources=self._resources, num_oper=tmp_num_oper,
                            start_time=tmp_start_times)

                    self._prof.prof('replan_done', uid=self._uid)
                    # If the plan has not change means that the last few
                    # workflows are executing, so nothing to plan for and
                    # the enactor should continue running.
                    self._plan = tmp_plan

                    self._update_checkpoints()

                if finished:
                    self._hold = False
                self._prof.prof('workflow_cfinished', uid=self._uid)

    def get_makespan(self):
        '''
        Returns the makespan of the campaign based on the current state of
        execution
        '''

        self._update_checkpoints()

        return self._checkpoints[-1]

    def _terminate(self):

        self._logger.info('Start terminating procedure')
        self._prof.prof('str_bookkeper_terminating', uid=self._uid)

        # Terminate enactor as well.
        self._enactor.terminate()

        # Terminate your threads.
        self._logger.debug('Enactor terminated, terminating threads')

        self._terminate_event.set()  # Thread event to terminate.
        if self._hold:
            self._hold = False
        self._prof.prof('monitor_bookkeper_terminate', uid=self._uid)
        self._monitoring_thread.join()
        self._prof.prof('monitor_bookkeper_terminated', uid=self._uid)
        self._logger.debug('Monitor thread terminated')

        if not self._cont:
            self._cont = True
        self._prof.prof('work_bookkeper_terminate', uid=self._uid)
        self._work_thread.join()  # Private attribute that will hold the thread
        self._prof.prof('work_bookkeper_terminated', uid=self._uid)
        self._logger.debug('Working thread terminated')

    def run(self):
        '''
        This method starts two threads for executing the campaign. The first
        thread starts the work method. The second thread the monitoring thread.
        '''
        try:
            # Populate the execution status dictionary with workflows
            with self._exec_state_lock:
                for workflow in self._campaign['campaign']:
                    self._workflows_state[workflow['id']] = st.NEW
            self._prof.prof('bookkeper_start', uid=self._uid)
            self._logger.info('Starting work thread')
            self._work_thread = mt.Thread(target=self.work,
                                            name='work-thread')
            self._work_thread.start()
            self._logger.info('Starting monitor thread')
            self._monitoring_thread = mt.Thread(target=self.monitor,
                                            name='monitor-thread')
            self._monitoring_thread.start()
            self._prof.prof('bookkeper_started', uid=self._uid)

            # This waits regardless if workflows are failing or not. This loop can
            # do meaningful work such as checking the state of the campaign. It can
            # be a while true, until something happens.
            self._logger.debug('Time now: %s, checkpoints: %s', self._time, self._checkpoints)
            while self._checkpoints is None:
                continue

            self._prof.prof('bookkeper_wait', uid=self._uid)
            self._cont = True
            while self._campaign['state'] not in st.CFINAL:
                if self._time != self._env.now:
                    self._time = self._env.now
                    self._cont = True
                    self._logger.debug('Time now: %s, checkpoint: %s',
                                    self._time, self._checkpoints[-1])

                # Check if all workflows are in a final state.
                cont = False

                for workflow in self._campaign['campaign']:
                    if self._workflows_state[workflow['id']] is st.FAILED:
                        self._campaign['state'] = st.FAILED
                        break
                    elif self._workflows_state[workflow['id']] not in st.CFINAL:
                        cont = True

                if not cont:
                    self._campaign['state'] = st.DONE

            if self._campaign['state'] not in st.CFINAL:
                self._campaign['state'] = st.DONE
            self._prof.prof('bookkeper_stopping', uid=self._uid)
        except Exception as ex:
            print(ex)
        finally:
            self._terminate()

    def get_campaign_state(self):

        return self._campaign['state']

    def get_workflows_state(self):

        states = dict()
        for workflow in self._campaign['campaign']:
            states[workflow['id']] = self._workflows_state[workflow['id']]

        return states
