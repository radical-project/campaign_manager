"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
"""
import math
import os
from copy import deepcopy

from .base import Planner
from random import random, randint, seed


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
    def __init__(self, campaign, resources, num_oper, population_size=20,
                 random_init=0.5, sid=None):

        super(GAPlanner, self).__init__(campaign=campaign,
                                        resources=resources,
                                        num_oper=num_oper,
                                        sid=sid)

        # Calculate the estimated execution time of each workflow on to each 
        # resource. This table will be used to calculate the plan.
        # est_tx holds this table. The index of the table is
        # <workflow_idx, resource_idx>, and each entry is the estimated
        # execution time of a workflow on a resource.
        # TODO: not all workflows can run in a resource
        res_perf = list()

        self._population = []
        self._population_size = population_size
        self._fitness = []
        self._random_init = random_init
        tmp_oper = []
        for workflow in self._campaign:
            tmp_oper.append(workflow['num_oper'])

        # total_rate = 0
        for resource in self._resources:
            res_perf.append(resource['performance'])

        self._abs_fitness_term = 0
        self._est_txs = self._calc_est_tx(tmp_oper, res_perf)
        self._deadline = None
        self._max_gen = 100

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
            encoding.append(-1)

        return encoding[:-1]


    def _decode_schedule(self, encoding):
        '''
        This method decodes to a schedule.
        '''

        schedule = []
        proc_sched = []
        for work_id in encoding:
            if work_id == -1:
                schedule.append(proc_sched)
                proc_sched = []
            else:
                proc_sched.append(work_id)

        schedule.append(proc_sched)
        return schedule


    def _initialize_population(self, workflows, resources, random_init,
                               start_time):
        '''
        This method creates the initial population. The population is 
        '''

        # This part of the code is to allow unit tests to be get a reproducible
        # result in the population initialization 
        if os.environ.get('PLANNER_TEST',"FALSE").lower() == 'true':
            self._logger.debug('Setting seed')
            seed(0)

        # This list tracks when a resource whould be available.
        if isinstance(start_time, list):
            resource_free = start_time
        elif isinstance(start_time, float) or isinstance(start_time, int):
            resource_free = [start_time] * len(resources)
        else:
            resource_free = [0] * len(resources)

        total_operations = 0
        for workflow in workflows:
            total_operations += workflow['num_oper']

        total_rate = 0
        for resource in resources:
            total_rate += resource['performance']

        self._abs_fitness_term = total_operations / total_rate + sum(resource_free)


        for _ in range(self._population_size):
            chromosome = [[] for j in range(len(resources))]
            for idx in range(int(len(workflows) * random_init)):
                wf_est_tx = self._est_txs[idx]
                resource = randint(0,len(resources) - 1)
                chromosome[resource].append(workflows[idx]['id'])
                tmp_str_time = resource_free[resource]
                tmp_end_time = tmp_str_time + wf_est_tx[resource]
                resource_free[resource] = tmp_end_time

            for idx in range(int(len(workflows) * random_init), len(workflows)):
                wf_est_tx = self._est_txs[idx]
                min_end_time = float('inf')
                for i in range(len(resources)):
                    tmp_str_time = resource_free[i]
                    tmp_end_time = tmp_str_time + wf_est_tx[i]
                    if tmp_end_time < min_end_time:
                        min_end_time = tmp_end_time
                        tmp_min_idx = i
                chromosome[tmp_min_idx].append(workflows[idx]['id'])
                resource_free[tmp_min_idx] = resource_free[tmp_min_idx] + \
                                             wf_est_tx[tmp_min_idx] 

            self._population.append(self._encode_schedule(chromosome))


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
        for i in range(int(self._population_size / 2)):
            selection = random()
            i = 0
            while selection > slots[i]:
                i += 1
            sel_idx.append(i - 1)

        selected = [self._population[i] for i in sel_idx]
        return selected


    def _crossover(self, parents):
        '''
        This method implements the cycle crossover method

        Reference: https://codereview.stackexchange.com/questions/226179/easiest-way-to-implement-cycle-crossover
        '''
        children = []
        for p_id in range(0, len(parents), 2):
            delimiters = [[], []]
            tmp_parent1 = []
            tmp_parent2 = []
            # Remove delimiters from parents and keep their position
            for j in range(len(parents[p_id])):
                if parents[p_id][j] == -1:
                    delimiters[0].append(j)
                else:
                    tmp_parent1.append(parents[p_id][j])

            for j in range(len(parents[p_id + 1])):
                if parents[p_id + 1][j] == -1:
                    delimiters[1].append(j)
                else:
                    tmp_parent2.append(parents[p_id + 1][j])

            swap = True
            count = 0
            pos = 0
            p1_copy = deepcopy(tmp_parent1)
            p2_copy = deepcopy(tmp_parent2)
            tmp_child1 = [-1] * len(tmp_parent1)
            tmp_child2 = [-1] * len(tmp_parent2)
            chrom_length = len(tmp_parent1)
            while True:
                if count > chrom_length:
                    break
                for i in range(chrom_length):
                    if tmp_child1[i] == -1:
                        pos = i
                        break

                if swap:
                    while True:
                        tmp_child1[pos] = tmp_parent1[pos]
                        count += 1
                        pos = tmp_parent2.index(tmp_parent1[pos])
                        if p1_copy[pos] == -1:
                            swap = False
                            break
                        p1_copy[pos] = -1
                else:
                    while True:
                        tmp_child1[pos] = tmp_parent2[pos]
                        count += 1
                        pos = tmp_parent1.index(tmp_parent2[pos])
                        if p2_copy[pos] == -1:
                            swap = True
                            break
                        p2_copy[pos] = -1

            for i in range(chrom_length):  # for the second child
                if tmp_child1[i] == tmp_parent1[i]:
                    tmp_child2[i] = tmp_parent2[i]
                else:
                    tmp_child2[i] = tmp_parent1[i]

            for i in range(chrom_length):  # Special mode
                if tmp_child1[i] == -1:
                    if p1_copy[i] == -1:  # it means that the ith gene from p1 has been already transfered
                        tmp_child1[i] = tmp_parent2[i]
                    else:
                        tmp_child1[i] = tmp_parent1[i]

            child1, child2 = [], []
            # Introduce the delimiters and produce the final children
            for i in range(len(parents[p_id])):
                if delimiters[0] and i == delimiters[0][0]:
                    child1.append(-1)
                    delimiters[0].pop(0)
                else:
                    child1.append(tmp_child1.pop(0))
            for i in range(len(parents[p_id + 1])):
                if delimiters[1] and i == delimiters[1][0]:
                    child2.append(-1)
                    delimiters[1].pop(0)
                else:
                    child2.append(tmp_child2.pop(0))
            children.append(child1)
            if child2 not in children:
                children.append(child2)

            self._logger.debug('Children %s %s', child1, child2)
        return children


    def _mutate(self, chromosomes):
        '''
        This method implements the swap mutation.
        '''

        for chromosome in chromosomes:
            self._logger.debug('Before mutation %s', chromosome)
            idx1 = randint(0, len(chromosome) - 1)
            while chromosome[idx1] == -1:
                idx1 = randint(0, len(chromosome) - 1)

            idx2 = randint(0, len(chromosome) - 1)
            while chromosome[idx2] == -1 or idx2 == idx1:
                idx2 = randint(0, len(chromosome) - 1)

            chromosome[idx1], chromosome[idx2] = chromosome[idx2], chromosome[idx1]
            self._logger.debug('After mutation %s', chromosome)
        return chromosomes


    # def _rebalancing(self):
    #     '''
    #     This method implements the rebalancing procedure
    #     '''
    #     pass

    def _calc_fitness(self):
        '''
        This methods calculates the fitness of the individuals in  the population.
        '''
        self._fitness = []
        for individual in self._population:
            total_dist = 0
            sched = self._decode_schedule(individual)
            self._logger.debug('Calculating fitness of %s with sched %s',
                                individual, sched)
            for r_id in range(len(self._resources)):
                workflows = sched[r_id]
                term = sum([self._est_txs[w_id - 1][r_id] for w_id in workflows])
                total_dist += math.pow(abs(self._abs_fitness_term - term), 2)
            error = math.sqrt(total_dist)

            if error:
                self._fitness.append(1 / error)
            else:
                self._fitness.append(1)


    def _get_makespan(self, individual):
        '''
        This method calculates the makespan based on a specific individual and
        returns it.
        '''

        sched = self._decode_schedule(individual)
        makespan = 0
        for r_id in range(len(self._resources)):
            workflows = sched[r_id]
            term = sum([self._est_txs[w_id - 1][r_id] for w_id in workflows]) 
            makespan = max(makespan, term)

        return makespan

    def _get_plan(self, individual):
        '''
        This method gets an individual and return the plan it corresponds
        '''

        sched = self._decode_schedule(individual)
        self._logger.debug('Plan sched: %s', sched)
        self._plan = list()

        resource_free = [0] * len(self._resources)
        try:
            for idx in range(len(self._campaign)):
                wf_est_tx = self._est_txs[idx]
                for r_id in range(len(sched)):
                    if self._campaign[idx]['id'] in sched[r_id]:
                        break
                tmp_str_time = resource_free[r_id]
                tmp_end_time = tmp_str_time + wf_est_tx[r_id]
                self._plan.append((self._campaign[idx], self._resources[r_id],
                                tmp_str_time, tmp_end_time))
                resource_free[r_id] = tmp_end_time
        except Exception as e:
            self._logger.error(idx, r_id, sched, individual, e)
            raise


    def plan(self, campaign=None, resources=None, num_oper=None, start_time=None,
             **kargs):
        '''
        This method implements the basic HEFT algorithm. It returns a list of tuples
        Each tuple contains: Workflow ID, Resource ID, Start Time, End Time.

        The plan method takes as input a campaign, resources and num_oper in case
        any of these has changed. They default to `None`

        *Returns:*
            list(tuples)
        '''

        self._deadline = kargs.get('deadline', self._deadline)
        self._max_gen = kargs.get('max_gen', self._max_gen)

        tmp_cmp = campaign if campaign else self._campaign
        tmp_res = resources if resources else self._resources
        # FIXME: allow replanning
        # tmp_nop = num_oper if num_oper else self._num_oper

        self._initialize_population(tmp_cmp, tmp_res, self._random_init,
                                    start_time=start_time)
        self._logger.debug('Initial  population: %s', self._population)
        self._calc_fitness()
        sorted_fitness = sorted(enumerate(self._fitness), key=lambda x: x[1])
        self._logger.debug('Sorted fitness: %s', sorted_fitness)
        gen_id = 0
        curr_makespan = 0
        while True:
            self._logger.debug('Generation: %d', gen_id)
            parents = self._selection()
            children = self._crossover(parents)
            self._logger.debug('Number of parents: %d, number of children %d',
                               len(parents), len(children))
            children = self._mutate(children)
            self._logger.debug('Number of parents: %d, number of children %d',
                                len(parents), len(children))
            # Replace half of the individuals with the worst fitness
            for i in range(len(children)):
                self._population[sorted_fitness[i][0]] = children[i]
            # Get the one with the best fitness and check it
            self._calc_fitness()
            sorted_fitness = sorted(enumerate(self._fitness), key=lambda x: x[1])
            self._logger.debug('Sorted fitness: %s', sorted_fitness)
            best_individual = self._population[sorted_fitness[-1][0]]
            tmp_makespan = self._get_makespan(best_individual)
            self._get_plan(best_individual)
            self._logger.debug('Best individual makespan: %f and plan %s',
                               tmp_makespan, self._plan)
            if self._deadline is not None and tmp_makespan < self._deadline:
                break
            elif sorted_fitness[-1][1] == 1:
                break
            elif gen_id == self._max_gen or tmp_makespan < curr_makespan:
                break
            curr_makespan = tmp_makespan
            gen_id += 1

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
