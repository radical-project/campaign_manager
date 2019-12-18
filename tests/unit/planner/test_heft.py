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

    actual_plan = [('W2', 1, 0, 13),
                   ('W9', 2, 0, 12),
                   ('W10', 3, 0, 16),
                   ('W3', 1, 13, 24),
                   ('W1', 3, 16, 25),
                   ('W4', 2, 12, 20),
                   ('W6', 3, 25, 34),
                   ('W5', 2, 20, 33),
                   ('W7', 1, 24, 31),
                   ('W8', 1, 31, 36)]
    planner = HeftPlanner(None, None, None)
    planner._campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner._resources = [1,2,3]
    planner._num_oper = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    est_plan = planner.plan()

    assert est_plan == actual_plan
