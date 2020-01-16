"""
Author: Ioannis Paraskevakos
License: MIT
Copyright: 2018-2019
Unit test for heft_planner scheduler
"""
# pylint: disable=protected-access, unused-argument

from radical.cm.utils.calculator.api.resource import Resource
from radical.cm.utils.calculator.entities.task import Task
from radical.cm.enactor import EmulatedEnactor
import radical.utils as ru

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
@mock.patch.object(EmulatedEnactor, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch('time.time', return_value=0)
def test_enact(mocked_init, mocked_raise_on, mocked_time):

    enactor = EmulatedEnactor()
    enactor._execution_status = dict()
    enactor._logger = ru.Logger('dummy')
    enactor._monitoring_lock = ru.RLock('cm.monitor_lock')
    enactor._monitoring_thread = 1
    enactor._to_monitor = list()
    resource = Resource()
    resource.create_core_list()
    core = resource.core_list[0]
    workflows = [{'description': 'blabla',
                  'id': 1,
                  'num_oper': 20,
                  'requirements': None}]
    enactor.enact(workflows=workflows, resources=[core])

    assert enactor._execution_status[1]['state'] == 2
    assert isinstance(enactor._execution_status[1]['endpoint'], Task)
    assert enactor._execution_status[1]['start_time'] == 0
    assert enactor._execution_status[1]['end_time'] is None
   