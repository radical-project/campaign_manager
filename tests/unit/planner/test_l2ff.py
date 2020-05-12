"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
Unit test for heft_planner scheduler
"""
# pylint: disable=protected-access, unused-argument

from radical.cm.planner import L2FFPlanner
import radical.utils as ru

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(L2FFPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan1(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 1, 'performance': 523}, 0, 102.5793499043977),
                   ('W9', {'id': 2, 'performance': 487}, 0, 82.13552361396304),
                   ('W3', {'id': 3, 'performance': 96}, 0, 330.2083333333333),
                   ('W5', {'id': 1, 'performance': 523}, 102.5793499043977, 140.82026768642447),
                   ('W10', {'id': 2, 'performance': 487}, 82.13552361396304, 114.98973305954826),
                   ('W4', {'id': 3, 'performance': 96}, 330.2083333333333, 467.38541666666663),
                   ('W2', {'id': 1, 'performance': 523}, 140.82026768642447, 162.23709369024857),
                   ('W7', {'id': 2, 'performance': 487}, 114.98973305954826, 135.52156057494867),
                   ('W8', {'id': 3, 'performance': 96}, 467.38541666666663, 519.7291666666666),
                   ('W6', {'id': 1, 'performance': 523}, 162.23709369024857, 164.53154875717019)]
    campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner = L2FFPlanner(None, None, None)
    planner._campaign = campaign
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
@mock.patch.object(L2FFPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan2(mocked_init, mocked_raise_on):

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
    campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner = L2FFPlanner(None, None, None)
    planner._campaign = campaign
    planner._resources = [{'id': 1, 'performance': 523}]
    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]
    planner._logger = ru.Logger('dummy')

    est_plan = planner.plan()
    assert est_plan == actual_plan


# ------------------------------------------------------------------------------
#
@mock.patch.object(L2FFPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan3(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 1, 'performance': 523}, 0, 102.5793499043977)]
    planner = L2FFPlanner(None, None, None)
    planner._campaign = ['W1']
    planner._resources = [{'id': 1, 'performance': 523},
                          {'id': 2, 'performance': 487},
                          {'id': 3, 'performance': 96}]
    planner._num_oper = [53649]
    planner._logger = ru.Logger('dummy')

    est_plan = planner.plan()
    assert est_plan == actual_plan

