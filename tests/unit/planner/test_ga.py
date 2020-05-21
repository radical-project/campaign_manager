"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
Unit test for heft_planner scheduler
"""
# pylint: disable=protected-access, unused-argument

import os
from radical.cm.planner import GAPlanner
import radical.utils as ru
# from random import randint
import math

try:
    import mock
except ImportError:
    from unittest import mock

os.environ['PLANNER_TEST'] = 'TRUE'


# ------------------------------------------------------------------------------
#
@mock.patch.object(GAPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_encoding(mocked_init, mocked_raise_on):

    sched = [[1, 3, 5, 6],
             [2, 7],
             [4, 8, 9]
            ]

    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    encode = planner._encode_schedule(schedule=sched)

    assert encode == [1,3,5,6,-1,2,7,-1,4,8,9]


# ------------------------------------------------------------------------------
#
@mock.patch.object(GAPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_decoding(mocked_init, mocked_raise_on):

    encode = [1,3,5,6,-1,2,7,-1,4,8,9]

    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    sched = planner._decode_schedule(encoding=encode)

    assert sched == [[1, 3, 5, 6], [2, 7], [4, 8, 9]]


# ------------------------------------------------------------------------------
#
@mock.patch.object(GAPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_initialize_population(mocked_init, mocked_raise_on):

    population = [[3, -1, 1, 2, 4, 8, -1, 5, 6, 7, 9, 10],
                  [6, 8, -1, 1, 2, 3, 4, 5, 10, -1, 7, 9],
                  [2, 4, -1, 5, 9, -1, 1, 3, 6, 7, 8, 10],
                  [1, 2, 6, -1, 4, 7, 9, -1, 3, 5, 8, 10],
                  [3, 5, -1, 4, 7, 10, -1, 1, 2, 6, 8, 9]]
    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    planner._population = []
    planner._population_size = 5
    planner._est_txs = [[1, 0.5, 0.3],
                        [2,   1, 0.7],
                        [3, 1.5, 1],
                        [4,   2, 1.3],
                        [5, 2.5, 1.7],
                        [6,   3, 2],
                        [7, 3.5, 2.3],
                        [8,   4, 2.7],
                        [9, 4.5, 3],
                        [10,  5, 3.3]]
    workflows = [{'description': 'W1','id': 1,'num_oper': 1,'requirements': None},
                 {'description': 'W2','id': 2,'num_oper': 2,'requirements': None},
                 {'description': 'W3','id': 3,'num_oper': 3,'requirements': None},
                 {'description': 'W4','id': 4,'num_oper': 4,'requirements': None},
                 {'description': 'W5','id': 5,'num_oper': 5,'requirements': None},
                 {'description': 'W6','id': 6,'num_oper': 6,'requirements': None},
                 {'description': 'W7','id': 7,'num_oper': 7,'requirements': None},
                 {'description': 'W8','id': 8,'num_oper': 8,'requirements': None},
                 {'description': 'W9','id': 9,'num_oper': 9,'requirements': None},
                 {'description': 'W10','id': 10,'num_oper': 10,'requirements': None}]
    
    resources = [{'id': 1, 'performance': 1},
                 {'id': 2, 'performance': 2},
                 {'id': 3, 'performance': 3}]

    planner._initialize_population(workflows=workflows, resources=resources,
                                   random_init=0.5, start_time=None)

    assert planner._population == population


# ------------------------------------------------------------------------------
#
@mock.patch.object(GAPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_calc_fitness(mocked_init, mocked_raise_on):

    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    planner._campaign = []
    planner._population = [[1,3,5,6,-1,2,7,-1,4,8,9], [1,5,6,-1,3,2,7,-1,4,8,9]]
    planner._est_txs = [[10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10]]
    planner._abs_fitness_term = 30
    planner._population_size = 2
    planner._fitness = []
    for i in range(9):
        workflow = {'description': 'W%s' % str(i + 1),
                    'id': i + 1,
                    'num_oper': 10,
                    'requirements': None}
        planner._campaign.append(workflow)
    planner._resources = [{'id': 1, 'performance': 1},
                          {'id': 2, 'performance': 1},
                          {'id': 3, 'performance': 1}]
    planner._calc_fitness()
    assert planner._fitness[1] == 1
    assert planner._fitness[0] == (1 / math.sqrt(200))


# ------------------------------------------------------------------------------
#
@mock.patch.object(GAPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_selection(mocked_init, mocked_raise_on):

    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    planner._campaign = {'campaign': []}
    planner._population = [[1, 3, 5, 6, -1, 2, 7, -1, 4, 8, 9],
                           [1, 5, 6, -1, 3, 2, 7, -1, 4, 8, 9]]
    planner._population_size = 2
    planner._fitness = [1, 0.5]

    parents = planner._selection()
    assert (parents[0] == [1, 3, 5, 6, -1, 2, 7, -1, 4, 8, 9] or
            parents[0] == [1, 5, 6, -1, 3, 2, 7, -1, 4, 8, 9])


# ------------------------------------------------------------------------------
#
@mock.patch.object(GAPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_crossover(mocked_init, mocked_raise_on):

    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    parents = [[1, 2, 3, -1, 4, 5, -1, 6, 7, 8, 9],
               [9, 3, -1, 7, 8, 2, 6, -1, 5, 1, 4]]
    children = planner._crossover(parents)
    assert children[0] == [1, 3, 7, -1, 4, 2, -1, 6, 5, 8, 9]
    assert children[1] == [9, 2, -1, 3, 8, 5, 6, -1, 7, 1, 4]

    parents = [[1, 4, -1, 3, -1, -1, 2], [-1, 2, 4, -1, 1, -1, 3]]
    children = planner._crossover(parents=parents)
    assert children[0] == [1, 4, -1, 3, -1, -1, 2]
    assert children[1] == [-1, 2, 4, -1, 1, -1, 3]


# ------------------------------------------------------------------------------
#
@mock.patch.object(GAPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_get_plan(mocked_init, mocked_raise_on):

    actual_plan = [({'description': 'W1', 'id': 0, 'num_oper': 10}, {'id': 2, 'performance': 1}, 0, 10), 
                   ({'description': 'W2', 'id': 1, 'num_oper': 10}, {'id': 1, 'performance': 1}, 0, 10), 
                   ({'description': 'W3', 'id': 2, 'num_oper': 10}, {'id': 2, 'performance': 1}, 10, 20), 
                   ({'description': 'W4', 'id': 3, 'num_oper': 10}, {'id': 1, 'performance': 1}, 10, 20), 
                   ({'description': 'W5', 'id': 4, 'num_oper': 10}, {'id': 3, 'performance': 1}, 0, 10), 
                   ({'description': 'W6', 'id': 5, 'num_oper': 10}, {'id': 1, 'performance': 1}, 20, 30), 
                   ({'description': 'W7', 'id': 6, 'num_oper': 10}, {'id': 1, 'performance': 1}, 30, 40), 
                   ({'description': 'W8', 'id': 7, 'num_oper': 10}, {'id': 2, 'performance': 1}, 20, 30), 
                   ({'description': 'W9', 'id': 8, 'num_oper': 10}, {'id': 3, 'performance': 1}, 10, 20), 
                   ({'description': 'W10', 'id': 9, 'num_oper': 10}, {'id': 3, 'performance': 1}, 20, 30)]
    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    planner._campaign = [{'description': 'W1', 'id': 0, 'num_oper': 10},
                         {'description': 'W2', 'id': 1, 'num_oper': 10},
                         {'description': 'W3', 'id': 2, 'num_oper': 10},
                         {'description': 'W4', 'id': 3, 'num_oper': 10},
                         {'description': 'W5', 'id': 4, 'num_oper': 10},
                         {'description': 'W6', 'id': 5, 'num_oper': 10},
                         {'description': 'W7', 'id': 6, 'num_oper': 10},
                         {'description': 'W8', 'id': 7, 'num_oper': 10},
                         {'description': 'W9', 'id': 8, 'num_oper': 10},
                         {'description': 'W10', 'id': 9, 'num_oper': 10}]
    planner._resources = [{'id': 1, 'performance': 1},
                          {'id': 2, 'performance': 1},
                          {'id': 3, 'performance': 1}]
    planner._num_oper = [10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
    planner._est_txs = [[10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10]]
    planner._get_plan([1, 3, 5, 6, -1, 2, 7, 0, -1, 4, 8, 9])

    assert planner._plan == actual_plan
    actual_plan = [({'description': None, 'id': 1, 'num_oper': 75000}, {'id': 1, 'performance': 1}, 0, 75000.0), 
                   ({'description': None, 'id': 2, 'num_oper': 75000}, {'id': 3, 'performance': 1}, 0, 75000.0), 
                   ({'description': None, 'id': 3, 'num_oper': 75000}, {'id': 4, 'performance': 1}, 0, 75000.0), 
                   ({'description': None, 'id': 4, 'num_oper': 75000}, {'id': 2, 'performance': 1}, 0, 75000.0)]
    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    planner._campaign = [{'description': None, 'id': 1, 'num_oper': 75000},
                         {'description': None, 'id': 2, 'num_oper': 75000},
                         {'description': None, 'id': 3, 'num_oper': 75000},
                         {'description': None, 'id': 4, 'num_oper': 75000}]
    planner._resources = [{'id': 1, 'performance': 1},
                          {'id': 2, 'performance': 1},
                          {'id': 3, 'performance': 1},
                          {'id': 4, 'performance': 1}]
    planner._num_oper = [75000, 75000, 75000, 75000]
    planner._est_txs = [[75000, 75000, 75000, 75000],
                        [75000, 75000, 75000, 75000],
                        [75000, 75000, 75000, 75000],
                        [75000, 75000, 75000, 75000]]
    planner._get_plan([1,-1,4,-1,2,-1,3])

    assert planner._plan == actual_plan


# ------------------------------------------------------------------------------
#
@mock.patch.object(GAPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_get_makespan(mocked_init, mocked_raise_on):

    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner._resources = [{'id': 1, 'performance': 1},
                          {'id': 2, 'performance': 1},
                          {'id': 3, 'performance': 1}]
    planner._num_oper = [10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
    planner._est_txs = [[10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10],
                        [10, 10, 10]]
    makespan = planner._get_makespan([1, 3, 5, 6, -1, 2, 7, 0, -1, 4, 8, 9])

    assert makespan == 40
