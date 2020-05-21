"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
Unit test for heft_planner scheduler
"""
# pylint: disable=protected-access, unused-argument

from radical.cm.planner import RandomPlanner
import radical.utils as ru
import os

try:
    import mock
except ImportError:
    from unittest import mock

os.environ['PLANNER_TEST'] = 'TRUE'


# ------------------------------------------------------------------------------
#
@mock.patch.object(RandomPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan1(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 2, 'performance': 487}, 0, 110.16221765913758),
                   ('W2', {'id': 2, 'performance': 487}, 110.16221765913758, 133.16221765913758),
                   ('W3', {'id': 1, 'performance': 523}, 0, 60.61185468451243),
                   ('W4', {'id': 2, 'performance': 487}, 133.16221765913758, 160.20328542094455),
                   ('W5', {'id': 3, 'performance': 96}, 0, 208.33333333333334),
                   ('W6', {'id': 2, 'performance': 487}, 160.20328542094455, 162.66735112936345),
                   ('W7', {'id': 2, 'performance': 487}, 162.66735112936345, 183.19917864476386),
                   ('W8', {'id': 2, 'performance': 487}, 183.19917864476386, 193.51745379876797),
                   ('W9', {'id': 2, 'performance': 487}, 193.51745379876797, 275.65297741273105),
                   ('W10', {'id': 2, 'performance': 487}, 275.65297741273105, 308.50718685831623)]
    campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner = RandomPlanner(None, None, None)
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
@mock.patch.object(RandomPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan2(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 1, 'performance': 523}, 0, 102.5793499043977),
                   ('W2', {'id': 1, 'performance': 523}, 102.5793499043977, 123.9961759082218),
                   ('W3', {'id': 1, 'performance': 523}, 123.9961759082218, 184.60803059273422),
                   ('W4', {'id': 1, 'performance': 523}, 184.60803059273422, 209.78776290630975),
                   ('W5', {'id': 1, 'performance': 523}, 209.78776290630975, 248.02868068833652),
                   ('W6', {'id': 1, 'performance': 523}, 248.02868068833652, 250.32313575525814),
                   ('W7', {'id': 1, 'performance': 523}, 250.32313575525814, 269.4416826003824),
                   ('W8', {'id': 1, 'performance': 523}, 269.4416826003824, 279.04971319311665),
                   ('W9', {'id': 1, 'performance': 523}, 279.04971319311665, 355.5315487571702),
                   ('W10', {'id': 1, 'performance': 523}, 355.5315487571702, 386.1242829827916)]
    campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner = RandomPlanner(None, None, None)
    planner._campaign = campaign
    planner._resources = [{'id': 1, 'performance': 523}]
    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]
    planner._logger = ru.Logger('dummy')

    est_plan = planner.plan()
    assert est_plan == actual_plan


# ------------------------------------------------------------------------------
#
@mock.patch.object(RandomPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan3(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 2, 'performance': 487}, 0, 110.16221765913758)]
    planner = RandomPlanner(None, None, None)
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
@mock.patch.object(RandomPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan_start_time(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 2, 'performance': 487}, 5, 115.16221765913758),
                   ('W2', {'id': 2, 'performance': 487}, 115.16221765913758, 138.16221765913758),
                   ('W3', {'id': 1, 'performance': 523}, 5, 65.61185468451242),
                   ('W4', {'id': 2, 'performance': 487}, 138.16221765913758, 165.20328542094455),
                   ('W5', {'id': 3, 'performance': 96}, 5, 213.33333333333334),
                   ('W6', {'id': 2, 'performance': 487}, 165.20328542094455, 167.66735112936345),
                   ('W7', {'id': 2, 'performance': 487}, 167.66735112936345, 188.19917864476386),
                   ('W8', {'id': 2, 'performance': 487}, 188.19917864476386, 198.51745379876797),
                   ('W9', {'id': 2, 'performance': 487}, 198.51745379876797, 280.65297741273105),
                   ('W10', {'id': 2, 'performance': 487}, 280.65297741273105, 313.50718685831623)]
    campaign = ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']
    planner = RandomPlanner(None, None, None)
    planner._campaign = campaign
    planner._resources = [{'id': 1, 'performance': 523},
                          {'id': 2, 'performance': 487},
                          {'id': 3, 'performance': 96}]
    planner._num_oper = [53649, 11201, 31700, 13169, 20000, 1200, 9999, 5025,
                         40000, 16000]
    planner._logger = ru.Logger('dummy')

    est_plan = planner.plan(start_time=5)
    assert est_plan == actual_plan


# ------------------------------------------------------------------------------
#
@mock.patch.object(RandomPlanner, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_plan_start_list(mocked_init, mocked_raise_on):

    actual_plan = [('W1', {'id': 2, 'performance': 487}, 3, 113.16221765913758),
                   ('W2', {'id': 2, 'performance': 487}, 113.16221765913758, 136.16221765913758),
                   ('W3', {'id': 1, 'performance': 523}, 5, 65.61185468451242),
                   ('W4', {'id': 2, 'performance': 487}, 136.16221765913758, 163.20328542094455),
                   ('W5', {'id': 3, 'performance': 96}, 4, 212.33333333333334),
                   ('W6', {'id': 2, 'performance': 487}, 163.20328542094455, 165.66735112936345),
                   ('W7', {'id': 2, 'performance': 487}, 165.66735112936345, 186.19917864476386),
                   ('W8', {'id': 2, 'performance': 487}, 186.19917864476386, 196.51745379876797),
                   ('W9', {'id': 2, 'performance': 487}, 196.51745379876797, 278.65297741273105),
                   ('W10', {'id': 2, 'performance': 487}, 278.65297741273105, 311.50718685831623)]
    planner = RandomPlanner(None, None, None)
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
