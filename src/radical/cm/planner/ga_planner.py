"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
import math

from .base import Planner
from random import random, randint


class GAPlanner(Planner):
    '''
    This class implemements a campaign planner based on a genetic algorithm.

    For reference:
    A. J. Page and T. J. Naughton, "Dynamic task scheduling using genetic 
    algorithms for heterogeneous distributed computing," 19th IEEE International 
    Parallel and Distributed Processing Symposium, Denver, CO, 2005, pp. 8 pp.-.

    Constractor parameters:
    campaign: A list of workflows
    resources: A list of resources, whose performance is given in operations per second
    num_oper: The number of operations each workflow will execute

    The class implements a plan method that return a plan, a list of tuples. 

    Each tuple will have the workflow, selected resource, starting time and
    estimated finish time.
    '''
    def __init__(self, campaign, resources, num_oper):

        super(GAPlanner, self).__init__(campaign=campaign,
                                        resources=resources,
                                        num_oper=num_oper)

        # Calculate the estimated execution time of each workflow on to each 
        # resource. This table will be used to calculate the plan.
        # est_tx holds this table. The index of the table is
        # <workflow_idx, resource_idx>, and each entry is the estimated
        # execution time of a workflow on a resource.
        # TODO: not all workflows can run in a resource
        res_perf = list()
        for resource in self._resources:
            res_perf.append(resource['performance'])

        self._population = None
         

    def _initialize_population(self, workflows, resources):
        '''
        This method creates the initial population. The population is 
        ''' 

    def _selection(self):
        '''
        This method uses the weighted roulette wheel method for 
        selection.
        '''

        slots = [0]
        fitness_sum = sum(self._fitness)
        for ind_fitness in self._fitness:
            slots.append(ind_fitness / fitness_sum + slots[-1])
        
        selection = random()
        i = 0
        while selection > slot[i]:
            i += 1

        return self._population[i-1]

    def _crossover(self, parent1, parent2):
        '''
        This method implements the cycle crossover method
        '''

        seen = [0] * len(parent1)
        cycles = []
        child1 = [-1] * len(parent1)
        child2 = [-1] * len(parent1)
        i=0
        for i in range(len(parent1)):
            if seen[i] == 0:
                str_point = i
                tmp_cycle = [parent1[i]]
                ptr = parent2[parent1[i] - 1] - 1
                while parent1[ptr] not in tmp_cycle:
                    seen[ptr] = 1
                    tmp_cycle.append(parent1[ptr])
                    ptr = parent2[parent1[ptr] - 1] - 1
                cycles.append(tmp_cycle)

        for i in range(len(cycles)):
            for elem in cycles[i]:
                if i % 2 == 0:
                    idx = parent1.index(elem)
                    child1[idx] = parent1[idx]
                    child2[idx] = parent2[idx]
                else:
                    idx = parent1.index(elem)
                    child1[idx] = parent2[idx]
                    child2[idx] = parent1[idx]

        return child1, child2

    def _mutate(self, chromosome):
        '''
        This method implements the swap mutation.
        '''
        idx1 = randint(0,len(chromosome))
        while chromosome[idx1] == -1:
            idx1 = randint(0,len(chromosome))

        idx2 = randint(0,len(chromosome))
        while chromosome[idx2] == -1 or idx2 == idx1:
            idx2 = randint(0,len(chromosome))
        
        chromosome[idx1], chromosome[idx2] = chromosome[idx2], chromosome[idx1]

        return chromosome

    def _rebalancing(self):
        '''
        This method implements the rebalancing procedure
        '''
        pass

    def _fitness(self, batch, start_times):
        '''
        This methods calculates the fitness of the individuals in  the population.
        '''

        total_operations = 0
        tmp_oper = []
        for workflow in batch:
            tmp_oper.append(workflow['num_oper'])
            total_operations += workflow['num_oper']
        
        total_rate = 0
        res_perf = []
        for resource in self._resources:
            res_perf.append(resource['performance'])
            total_rate += resource['performance']

        est_txs = self._calc_est_tx(tmp_oper, res_perf)

        abs_term = total_operations / total_rate + sum(start_times)

        fitness = []

        for workflow in batch:
            total_dist = 0
            for j in range(len(self._resources)):
                term = sum([est_tx[j] for est_tx in est_txs)
                total_dist += math.pow(abs(abs_term - (workflow['num_oper'] + term)), 2)
            error = math.sqrt(total_dist)
            fitness.append(1 / error)

        return fitness


    def _rebalancing(self):
        '''
        This method implements the rebalancing heuristic.
        '''

        pass


    def plan(self, campaign=None, resources=None, num_oper=None, start_time=None,
             deadline=None, max_iter=1000):
        '''
        This method implements the basic HEFT algorithm. It returns a list of tuples
        Each tuple contains: Workflow ID, Resource ID, Start Time, End Time.

        The plan method takes as input a campaign, resources and num_oper in case
        any of these has changed. They default to `None`

        *Returns:*
            list(tuples)
        '''

        tmp_cmp = campaign if campaign else self._campaign
        tmp_res = resources if resources else self._resources
        tmp_nop = num_oper if num_oper else self._num_oper

        initial_pop = self._initialize_population(tmp_cmp, tmp_res)

        while True:
            self._crossover()
            self._mutate()
            self._select()

            tmp_makespan = self._get_makespan(plan)

            if deadline is not None and tmp_makespan < deadline:
                break
            elif iter == max_iter or tmp_makespan > curr_makespan:
                break
            self._plan = tmp_plan
            curr_makespan = tmp_makespan

        self._plan = plan
        self._logger.info('Derived plan %s', self._plan)
        return self._plan

    def replan(self, campaign=None, resources=None, num_oper=None, start_time=0):
        '''
        The planning method
        '''
        if campaign and resources and num_oper:
            self._logger.debug('Replanning')
            self._plan = self.plan(campaign=campaign, resources=resources,
                                   num_oper=num_oper, start_time=start_time)
        else:
            self._logger.debug('Nothing to plan for')

        return self._plan