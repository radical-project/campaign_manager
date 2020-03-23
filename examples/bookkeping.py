from radical.cm.utils.calculator.api.resource import Resource
from radical.cm.bookkeeper import Bookkeeper
import time
from random import randint

# ------------------------------------------------------------------------------
# Create campaign
campaign = []
for i in range(8):
    oper = randint(1,100)
    workflow = {'description': 'blabla%s' % str(i + 1),
                'id': i + 1,
                'num_oper': oper,
                'requirements': None}
    campaign.append(workflow)

# ------------------------------------------------------------------------------
# Create a resource list
tmp_resources = Resource(num_cores=4, dist_mean=1)#, temporal_var=0.1)
tmp_resources.create_core_list()
res = []
for i in range(len(tmp_resources.core_list)):
    res.append({'id': i + 1,
                'performance': 1,
                'label': tmp_resources.core_list[i]})


bookkeep = Bookkeeper(campaign=campaign, resources=res, planner='heft')
bookkeep.run()

