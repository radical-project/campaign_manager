"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
Unit test for heft_planner scheduler
"""
# pylint: disable=protected-access, unused-argument

from radical.cm.planner import RandomPlanner

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
@mock.patch.object(RandomPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan1(mocked_init, mocked_raise_on):

    campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner = RandomPlanner(None, None, None)
    planner._campaign = campaign
    planner._resources = [523, 487, 96]
    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]

    est_plan = planner.plan()
    print est_plan
    for elem in est_plan:
        assert elem[0] in campaign
        campaign.remove(elem[0])
        assert elem[1] in planner._resources
        assert (isinstance(elem[2], float) or elem[2] == 0)
        assert isinstance(elem[3], float)

# ------------------------------------------------------------------------------
#
@mock.patch.object(RandomPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan2(mocked_init, mocked_raise_on):

    campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner = RandomPlanner(None, None, None)
    planner._campaign = campaign
    planner._resources = [523]
    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]

    est_plan = planner.plan()
    for elem in est_plan:
        assert elem[0] in campaign
        campaign.remove(elem[0])
        assert elem[1] == 523
        assert (isinstance(elem[2], float) or elem[2] == 0)
        assert isinstance(elem[3], float)

# ------------------------------------------------------------------------------
#
@mock.patch.object(RandomPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan3(mocked_init, mocked_raise_on):

    actual_plan = [('W1', 523, 0, 102.5793499043977),
                   ('W1', 487, 0, 110.16221765913758),
                   ('W1', 96, 0, 558.84375)]
    planner = RandomPlanner(None, None, None)
    planner._campaign = ['W1']
    planner._resources = [523, 487, 96]
    planner._num_oper = [53649]

    est_plan = planner.plan()
    assert est_plan[0] in actual_plan
