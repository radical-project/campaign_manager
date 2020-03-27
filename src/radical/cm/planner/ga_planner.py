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
    population_size: The size of the population

    The class implements a plan method that return a plan, a list of tuples. 

    Each tuple will have the workflow, selected resource, starting time and
    estimated finish time.
    '''
    def __init__(self, campaign, resources, num_oper, population_size=20):

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
        self._population_size = population_size
         
    def _encode_schedule(self, schedule):
        '''
        This method encodes a schedule to the algorithm's enconding. A schedule
        is a list of lists, such as
        
        [[1, 3, 5, 6],
         [2, 7],
         [4, 8, 9]
        ]
        Each list represents the workflow IDs each resources has assigned to
        execute.

        Encoding looks like:
        
        [1,3,5,6,-1,2,7,-1,4,8,9],

        where number are workflow IDs, and -1 are delimiters between resources.
        '''

        encoding = []
        for sched in schedule:
            for work_id in sched:
                encoding.append(work_id)
            enconding.append(-1)
        
        return enconding

    def _decode_schedule(self, enconding):
        '''
        This method decodes to a schedule.
        '''

        schedule = []
        proc_sched = []
        for work_id in enconding:
            if work_id == -1:
                schedule.append(proc_sched)
                proc_sched = []
            else:
                proc_sched.append(work_id)
            
        schedule.append(proc_sched)
        return schedule

    def _initialize_population(self, workflows, resources, population_size=None):
        '''
        This method creates the initial population. The population is 
        ''' 
        chromosome = []

    def _selection(self):
        '''
        This method uses the roulette wheel method for selection. The selection
        process will return half of the population.
        '''
        

        slots = [0]
        fitness_sum = sum(self._fitness)
        for ind_fitness in self._fitness:
            slots.append(ind_fitness / fitness_sum + slots[-1])
        
        sel_idx = []
        for i in range(self._population_size / 2):
            selection = random()
            i = 0
            while selection > slot[i]:
                i += 1
            sel_idx.append(i)

        selected = [self._population[i] for i in sel_idx]
        return selected

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

    def _fitness(self, batch):
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

        abs_term = total_operations / total_rate

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

        self._population = self._initialize_population(tmp_cmp, tmp_res)
        curr_fitness = self._fitness(self._population)
        sorted_fitness = sorted(enumerate(curr_fitness), key=lambda x: x[1])

        while True:
            self._population = self._sort_population()
            parents = self._select()
            children = self._crossover(parents)
            children = self._mutate(chldren)
            # Replace half of the individuals with the worst fitness
            for i in range(self._population_size / 2):
                self._population[sorted_fitness[i][0]] = children[i]
            # Get the one with the best fitness and check it
            curr_fitness = self._fitness(self._population)
            sorted_fitness = sorted(enumerate(curr_fitness), key=lambda x: x[1])
            best_individual = self._population[sorted_fitness[-1][0]]
            tmp_makespan = self._get_makespan(best_individual)

            if deadline is not None and tmp_makespan < deadline:
                break
            elif iter == max_iter or tmp_makespan > curr_makespan:
                break
            self._plan = self._get_plan(best_individual)
            curr_makespan = tmp_makespan

        self._plan = self._get_plan(best_individual)
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