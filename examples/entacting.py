from simpy import Environment
from radical.cm.utils.calculator.api.resource import Resource
from radical.cm.enactor import SimulatedEnactor
import time

#pylint: disable=protected-access
environ = Environment()

enactor = SimulatedEnactor(env=environ)
resources = Resource(num_cores=4)
resources.create_core_list()
res = []
for resource in resources.core_list:
    res.append({'label': resource})

workflows = [{'description': 'blabla','id': 1,'num_oper': 20,'requirements': None},
             {'description': 'blabla','id': 2,'num_oper': 10,'requirements': None},
             {'description': 'blabla','id': 3,'num_oper': 30,'requirements': None},
             {'description': 'blabla','id': 4,'num_oper': 40,'requirements': None}]
enactor.enact(workflows=workflows,resources=res)
print(environ.now)
environ.run(until=100)
print(environ.now)
time.sleep(5)
enactor.terminate()
print(enactor._execution_status)
