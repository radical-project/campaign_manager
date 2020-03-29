"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
Unit test for heft_planner scheduler
"""
# pylint: disable=protected-access, unused-argument

from radical.cm.planner import GAPlanner
import radical.utils as ru
from random import randint
import math

try:
    import mock
except ImportError:
    from unittest import mock

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

    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    planner._population = []
    planner._population_size = 20
    workflows = []
    for i in range(8):
        oper = randint(1,100)
        workflow = {'description': 'W%s' % str(i + 1),
                    'id': i + 1,
                    'num_oper': oper,
                    'requirements': None}
        workflows.append(workflow)
    resources = [{'id': 1, 'performance': 523},
                 {'id': 2, 'performance': 487},
                 {'id': 3, 'performance': 96}]

    planner._initialize_population(workflows=workflows, resources=resources)
    assert isinstance(planner._population, list)
    for individual in planner._population:
        assert isinstance(individual, list)
        assert individual.count(-1) == len(resources) - 1
        assert len(individual) - individual.count(-1) == len(workflows)
        while -1 in individual:
            individual.remove(-1)
        assert len(individual) == len(set(individual))

# ------------------------------------------------------------------------------
#
@mock.patch.object(GAPlanner, '__init__', return_value=None)
@mock.patch.object(GAPlanner, '_calc_est_tx', return_value= [[10, 10, 10],
                                                             [10, 10, 10],
                                                             [10, 10, 10],
                                                             [10, 10, 10],
                                                             [10, 10, 10],
                                                             [10, 10, 10],
                                                             [10, 10, 10],
                                                             [10, 10, 10],
                                                             [10, 10, 10],
                                                             [10, 10, 10]])
@mock.patch('radical.utils.raise_on')
def test_calc_fitness(mocked_init, mocked_calc_est_tx, mocked_raise_on):

    planner = GAPlanner(None, None, None)
    planner._logger = ru.Logger('dummy')
    planner._campaign = {'campaign': []}
    planner._population = [[1,3,5,6,-1,2,7,-1,4,8,9], [1,5,6,-1,3,2,7,-1,4,8,9]]
    planner._population_size = 1
    planner._fitness = []
    for i in range(9):
        workflow = {'description': 'W%s' % str(i + 1),
                    'id': i + 1,
                    'num_oper': 10,
                    'requirements': None}
        planner._campaign['campaign'].append(workflow)
    planner._resources = [{'id': 1, 'performance': 1},
                          {'id': 2, 'performance': 1},
                          {'id': 3, 'performance': 1}]
    planner._calc_fitness()
    assert planner._fitness[1] == 1
    assert planner._fitness[0] == (1 / math.sqrt(200))
