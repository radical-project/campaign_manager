from radical.cm.utils.calculator.api.resource import Resource
from radical.cm.bookkeeper import Bookkeeper
import time
from random import randint


#-------------------------------------------------------------------------------
#
def get_makespan(curr_plan):

    checkpoints = [0]

    for work in curr_plan:
        if work[2] not in checkpoints:
            checkpoints.append(work[2])
        if work[3] not in checkpoints:
            checkpoints.append(work[3])
    
    checkpoints.sort()
    return checkpoints[-1]

# ------------------------------------------------------------------------------
# Create campaign
campaign = []
for i in range(16):
    oper = randint(1,100)
    workflow = {'description': 'blabla%s' % str(i + 1),
                'id': i + 1,
                'num_oper': oper,
                'requirements': None}
    campaign.append(workflow)

# ------------------------------------------------------------------------------
# Create a resource list
tmp_resources = Resource(num_cores=8, dist_mean=1)#, temporal_var=0.1)
tmp_resources.create_core_list()
res = []
for i in range(len(tmp_resources.core_list)):
    res.append({'id': i + 1,
                'performance': 1,
                'label': tmp_resources.core_list[i]})


bookkeep = Bookkeeper(campaign=campaign, resources=res, planner='heft')
bookkeep.run()

print('After finishing time is %f' % bookkeep._time)
print('Expected makespan was %f' % get_makespan(bookkeep._plan))

