"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
Unit test for heft_planner scheduler
"""
# pylint: disable=protected-access, unused-argument

from radical.cm.planner import HeftPlanner

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

    actual_plan = [('W2', { 'id': 1, 'performance': 1}, 0, 13),
                   ('W9', { 'id': 2, 'performance': 2}, 0, 12),
                   ('W10', { 'id': 3, 'performance': 3}, 0, 16),
                   ('W3', { 'id': 1, 'performance': 1}, 13, 24),
                   ('W1', { 'id': 3, 'performance': 3}, 16, 25),
                   ('W4', { 'id': 2, 'performance': 2}, 12, 20),
                   ('W6', { 'id': 3, 'performance': 3}, 25, 34),
                   ('W5', { 'id': 2, 'performance': 2}, 20, 33),
                   ('W7', { 'id': 1, 'performance': 1}, 24, 31),
                   ('W8', { 'id': 1, 'performance': 1}, 31, 36)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner._resources = [{ 'id': 1, 'performance': 1},
                          { 'id': 2, 'performance': 2},
                          { 'id': 3, 'performance': 3}]
    planner._num_oper = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    est_plan = planner.plan()

    assert est_plan == actual_plan

# ------------------------------------------------------------------------------
#
@mock.patch.object(HeftPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan2(mocked_init, mocked_raise_on):

    actual_plan = [('W1', { 'id': 1, 'performance': 523}, 0, 102.5793499043977),
                   ('W9', { 'id': 2, 'performance': 487}, 0, 82.13552361396304),
                   ('W3', { 'id': 2, 'performance': 487}, 82.13552361396304, 147.22792607802876),
                   ('W5', { 'id': 1, 'performance': 523}, 102.5793499043977, 140.82026768642447),
                   ('W10', { 'id': 3, 'performance': 96}, 0, 166.66666666666666),
                   ('W4', { 'id': 1, 'performance': 523}, 140.82026768642447, 166.0),
                   ('W2', { 'id': 2, 'performance': 487}, 147.22792607802876, 170.22792607802876),
                   ('W7', { 'id': 1, 'performance': 523}, 166.0, 185.11854684512429),
                   ('W8', { 'id': 2, 'performance': 487}, 170.22792607802876, 180.54620123203287),
                   ('W6', { 'id': 3, 'performance': 96}, 166.66666666666666, 179.16666666666666)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9',
                         'W10']
    planner._resources = [{ 'id': 1, 'performance': 523},
                          { 'id': 2, 'performance': 487},
                          { 'id': 3, 'performance': 96}]
    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]

    est_plan = planner.plan()
    assert est_plan == actual_plan

# ------------------------------------------------------------------------------
#
@mock.patch.object(HeftPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan3(mocked_init, mocked_raise_on):

    actual_plan = [('W1', { 'id': 1, 'performance': 523}, 0, 102.5793499043977),
                   ('W9', { 'id': 1, 'performance': 523}, 102.5793499043977, 179.06118546845124),
                   ('W3', { 'id': 1, 'performance': 523}, 179.06118546845124, 239.67304015296367),
                   ('W5', { 'id': 1, 'performance': 523}, 239.67304015296367, 277.91395793499044),
                   ('W10', { 'id': 1, 'performance': 523}, 277.91395793499044, 308.50669216061186), 
                   ('W4', { 'id': 1, 'performance': 523}, 308.50669216061186, 333.6864244741874), 
                   ('W2', { 'id': 1, 'performance': 523}, 333.6864244741874, 355.1032504780115), 
                   ('W7', { 'id': 1, 'performance': 523}, 355.1032504780115, 374.2217973231358), 
                   ('W8', { 'id': 1, 'performance': 523}, 374.2217973231358, 383.82982791587), 
                   ('W6', { 'id': 1, 'performance': 523}, 383.82982791587, 386.1242829827916)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9',
                         'W10']
    planner._resources = [{ 'id': 1, 'performance': 523}]

    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]

    est_plan = planner.plan()
    assert est_plan == actual_plan

# ------------------------------------------------------------------------------
#
@mock.patch.object(HeftPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan4(mocked_init, mocked_raise_on):

    actual_plan = [('W1', { 'id': 1, 'performance': 523}, 0, 102.5793499043977)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1']
    planner._resources = [{ 'id': 1, 'performance': 523},
                          { 'id': 2, 'performance': 487},
                          { 'id': 3, 'performance': 96}]
    planner._num_oper = [53649]

    est_plan = planner.plan()
    assert est_plan == actual_plan
