"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
Unit test for heft_planner scheduler
"""
# pylint: disable=protected-access, unused-argument


from radical.cm.bookkeeper import Bookkeeper
from radical.cm.utils import states as st

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
@mock.patch.object(Bookkeeper, '__init__', return_value=None)
def test_update_checkpoints(mocked_init):

    bookkeeper = Bookkeeper(campaign=None, resources=None)
    bookkeeper._checkpoints = None
    bookkeeper._plan = [('W1', {'id': 1, 'performance': 523}, 0, 102.5793499043977),
                        ('W9', {'id': 2, 'performance': 487}, 0, 82.13552361396304),
                        ('W3', {'id': 2, 'performance': 487}, 82.13552361396304, 147.22792607802876),
                        ('W5', {'id': 1, 'performance': 523}, 102.5793499043977, 140.82026768642447),
                        ('W10', {'id': 3, 'performance': 96}, 0, 166.66666666666666),
                        ('W4', {'id': 1, 'performance': 523}, 140.82026768642447, 166.0),
                        ('W2', {'id': 2, 'performance': 487}, 147.22792607802876, 170.22792607802876),
                        ('W7', {'id': 1, 'performance': 523}, 166.0, 185.11854684512429),
                        ('W8', {'id': 2, 'performance': 487}, 170.22792607802876, 180.54620123203287),
                        ('W6', {'id': 3, 'performance': 96}, 166.66666666666666, 179.16666666666666)]

    checkpoints = [0,
                   82.13552361396304,
                   102.5793499043977,
                   140.82026768642447,
                   147.22792607802876,
                   166.0,
                   166.66666666666666,
                   170.22792607802876,
                   179.16666666666666,
                   180.54620123203287,
                   185.11854684512429]

    bookkeeper._update_checkpoints()

    assert checkpoints == bookkeeper._checkpoints

# ------------------------------------------------------------------------------
#
@mock.patch.object(Bookkeeper, '__init__', return_value=None)
@mock.patch.object(Bookkeeper, '_update_checkpoints', return_value=None)
def test_verify_objective(mocked_init, mocked_update_checkpoints):

    bookkeeper = Bookkeeper(campaign=None, resources=None)
    bookkeeper._checkpoints = [0,
                               82.13552361396304,
                               102.5793499043977,
                               140.82026768642447,
                               147.22792607802876,
                               166.0,
                               166.66666666666666,
                               170.22792607802876,
                               179.16666666666666,
                               180.54620123203287,
                               185.11854684512429]
    bookkeeper._objective = 500
    assert bookkeeper._verify_objective()

    bookkeeper._objective = 150

    assert not bookkeeper._verify_objective()

# ------------------------------------------------------------------------------
#
@mock.patch.object(Bookkeeper, '__init__', return_value=None)
@mock.patch.object(Bookkeeper, '_update_checkpoints', return_value=None)
def test_get_makespan(mocked_init, mocked_update_checkpoints):

    bookkeeper = Bookkeeper(campaign=None, resources=None)
    bookkeeper._checkpoints = [0,
                               82.13552361396304,
                               102.5793499043977,
                               140.82026768642447,
                               147.22792607802876,
                               166.0,
                               166.66666666666666,
                               170.22792607802876,
                               179.16666666666666,
                               180.54620123203287,
                               185.11854684512429]

    makespan = 185.11854684512429
    assert bookkeeper.get_makespan() == makespan

# ------------------------------------------------------------------------------
#
@mock.patch.object(Bookkeeper, '__init__', return_value=None)
def test_get_campaign_state(mocked_init):

    bookkeeper = Bookkeeper(campaign=None, resources=None)
    bookkeeper._campaign = {'campaign': None,
                            'state': st.NEW}

    assert bookkeeper.get_campaign_state() == st.NEW

# ------------------------------------------------------------------------------
#
@mock.patch.object(Bookkeeper, '__init__', return_value=None)
def test_get_workflows_state(mocked_init):

    bookkeeper = Bookkeeper(campaign=None, resources=None)
    bookkeeper._workflows_state = {1: st.NEW,
                                   2: st.EXECUTING}

    bookkeeper._campaign = {'campaign': [{'id': 1},
                                         {'id': 2}]}
    assert bookkeeper.get_workflows_state() == {1: st.NEW, 2: st.EXECUTING}
