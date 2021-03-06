"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
Unit test for heft_planner scheduler
"""
# pylint: disable=protected-access, unused-argument

from radical.cm.planner import HeftPlanner
import radical.utils as ru

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(HeftPlanner, '__init__', return_value=None)
@mock.patch.object(HeftPlanner, '_calc_est_tx', return_value= [[14, 16,  9],
                                                               [13, 19, 18],
                                                               [11, 13, 19],
                                                               [13,  8, 17],
                                                               [12, 13, 10],
                                                               [13, 16,  9],
                                                                [7, 15, 11],
                                                                [5, 11, 14],
                                                               [18, 12, 20],
                                                               [21,  7, 16]])
@mock.patch('radical.utils.raise_on')
def test_plan(mocked_init, mocked_calc_est_tx, mocked_raise_on):

    actual_plan = [('W2', {'id': 1, 'performance': 1}, 0, 13),
                   ('W9', {'id': 2, 'performance': 2}, 0, 12),
                   ('W10', {'id': 3, 'performance': 3}, 0, 16),
                   ('W3', {'id': 1, 'performance': 1}, 13, 24),
                   ('W1', {'id': 3, 'performance': 3}, 16, 25),
                   ('W4', {'id': 2, 'performance': 2}, 12, 20),
                   ('W6', {'id': 3, 'performance': 3}, 25, 34),
                   ('W5', {'id': 2, 'performance': 2}, 20, 33),
                   ('W7', {'id': 1, 'performance': 1}, 24, 31),
                   ('W8', {'id': 1, 'performance': 1}, 31, 36)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner._resources = [{'id': 1, 'performance': 1},
                          {'id': 2, 'performance': 2},
                          {'id': 3, 'performance': 3}]
    planner._num_oper = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    planner._logger = ru.Logger('dummy')
    est_plan = planner.plan()

    assert est_plan == actual_plan


# ------------------------------------------------------------------------------
#
@mock.patch.object(HeftPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan2(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 1, 'performance': 523}, 0, 102.5793499043977),
                   ('W9', {'id': 2, 'performance': 487}, 0, 82.13552361396304),
                   ('W3', {'id': 2, 'performance': 487}, 82.13552361396304, 147.22792607802876),
                   ('W5', {'id': 1, 'performance': 523}, 102.5793499043977, 140.82026768642447),
                   ('W10', {'id': 3, 'performance': 96}, 0, 166.66666666666666),
                   ('W4', {'id': 1, 'performance': 523}, 140.82026768642447, 166.0),
                   ('W2', {'id': 2, 'performance': 487}, 147.22792607802876, 170.22792607802876),
                   ('W7', {'id': 1, 'performance': 523}, 166.0, 185.11854684512429),
                   ('W8', {'id': 2, 'performance': 487}, 170.22792607802876, 180.54620123203287),
                   ('W6', {'id': 3, 'performance': 96}, 166.66666666666666, 179.16666666666666)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9',
                         'W10']
    planner._resources = [{'id': 1, 'performance': 523},
                          {'id': 2, 'performance': 487},
                          {'id': 3, 'performance': 96}]
    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]
    planner._logger = ru.Logger('dummy')
    est_plan = planner.plan()
    assert est_plan == actual_plan


# ------------------------------------------------------------------------------
#
@mock.patch.object(HeftPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan3(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 1, 'performance': 523}, 0, 102.5793499043977),
                   ('W9', {'id': 1, 'performance': 523}, 102.5793499043977, 179.06118546845124),
                   ('W3', {'id': 1, 'performance': 523}, 179.06118546845124, 239.67304015296367),
                   ('W5', {'id': 1, 'performance': 523}, 239.67304015296367, 277.91395793499044),
                   ('W10', {'id': 1, 'performance': 523}, 277.91395793499044, 308.50669216061186),
                   ('W4', {'id': 1, 'performance': 523}, 308.50669216061186, 333.6864244741874), 
                   ('W2', {'id': 1, 'performance': 523}, 333.6864244741874, 355.1032504780115), 
                   ('W7', {'id': 1, 'performance': 523}, 355.1032504780115, 374.2217973231358), 
                   ('W8', {'id': 1, 'performance': 523}, 374.2217973231358, 383.82982791587), 
                   ('W6', {'id': 1, 'performance': 523}, 383.82982791587, 386.1242829827916)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9',
                         'W10']
    planner._resources = [{'id': 1, 'performance': 523}]

    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]
    planner._logger = ru.Logger('dummy')
    est_plan = planner.plan()
    assert est_plan == actual_plan


# ------------------------------------------------------------------------------
#
@mock.patch.object(HeftPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan4(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 1, 'performance': 523}, 0, 102.5793499043977)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1']
    planner._resources = [{'id': 1, 'performance': 523},
                          {'id': 2, 'performance': 487},
                          {'id': 3, 'performance': 96}]
    planner._num_oper = [53649]
    planner._logger = ru.Logger('dummy')
    est_plan = planner.plan()
    assert est_plan == actual_plan


# ------------------------------------------------------------------------------
#
@mock.patch.object(HeftPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan_start_time(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 1, 'performance': 523}, 5, 107.5793499043977),
                   ('W9', {'id': 1, 'performance': 523}, 107.5793499043977, 184.06118546845124),
                   ('W3', {'id': 1, 'performance': 523}, 184.06118546845124, 244.67304015296367),
                   ('W5', {'id': 1, 'performance': 523}, 244.67304015296367, 282.91395793499044),
                   ('W10', {'id': 1, 'performance': 523}, 282.91395793499044, 313.50669216061186),
                   ('W4', {'id': 1, 'performance': 523}, 313.50669216061186, 338.6864244741874),
                   ('W2', {'id': 1, 'performance': 523}, 338.6864244741874, 360.1032504780115),
                   ('W7', {'id': 1, 'performance': 523}, 360.1032504780115, 379.2217973231358),
                   ('W8', {'id': 1, 'performance': 523}, 379.2217973231358, 388.82982791587),
                   ('W6', {'id': 1, 'performance': 523}, 388.82982791587, 391.1242829827916)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9',
                         'W10']
    planner._resources = [{'id': 1, 'performance': 523}]

    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]
    planner._logger = ru.Logger('dummy')    
    est_plan = planner.plan(start_time=5)
    assert est_plan == actual_plan


# ------------------------------------------------------------------------------
#
@mock.patch.object(HeftPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan_start_list(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 1, 'performance': 523}, 5, 107.5793499043977),
                   ('W9', {'id': 2, 'performance': 487}, 3, 85.13552361396304),
                   ('W3', {'id': 2, 'performance': 487}, 85.13552361396304, 150.22792607802876),
                   ('W5', {'id': 1, 'performance': 523}, 107.5793499043977, 145.82026768642447),
                   ('W10', {'id': 3, 'performance': 96}, 4, 170.66666666666666),
                   ('W4', {'id': 1, 'performance': 523}, 145.82026768642447, 171.0),
                   ('W2', {'id': 2, 'performance': 487}, 150.22792607802876, 173.22792607802876),
                   ('W7', {'id': 1, 'performance': 523}, 171.0, 190.11854684512429),
                   ('W8', {'id': 2, 'performance': 487}, 173.22792607802876, 183.54620123203287),
                   ('W6', {'id': 3, 'performance': 96}, 170.66666666666666, 183.16666666666666)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9',
                         'W10']
    planner._resources = [{'id': 1, 'performance': 523},
                          {'id': 2, 'performance': 487},
                          {'id': 3, 'performance': 96}]
    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]
    planner._logger = ru.Logger('dummy') 
    est_plan = planner.plan(start_time=[5,3,4])
    assert est_plan == actual_plan
