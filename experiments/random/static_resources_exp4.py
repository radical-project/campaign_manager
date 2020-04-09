from radical.cm.planner import RandomPlanner
import pandas as pd
import sys
from time import time


def df_to_lists(cmp, size):

    tmp_workflows = list()
    tmp_numoper = list()
    for i in range(size):
        point = cmp.loc[i] 
        workflow = {'description': None}
        workflow['id'] = int(point['id'])
        workflow['num_oper'] = point['num_oper']
        tmp_workflows.append(workflow)
        tmp_numoper.append(workflow['num_oper'])

    return tmp_workflows, tmp_numoper


def get_makespan(curr_plan):

    checkpoints = [0]

    for work in curr_plan:
        if work[2] not in checkpoints:
            checkpoints.append(work[2])
        if work[3] not in checkpoints:
            checkpoints.append(work[3])

    checkpoints.sort()
    return checkpoints[-1]


if __name__ == "__main__":

    repetitions = int(sys.argv[1])
    resources = [{'id': 1, 'performance': 1},
                 {'id': 2, 'performance': 1},
                 {'id': 3, 'performance': 2},
                 {'id': 4, 'performance': 2}]
    campaign_sizes = [4, 8, 16, 32, 64, 128, 256, 512, 1024]
    results = pd.DataFrame(columns=['size','planner','plan','makespan','time'])
    total_cmp = pd.read_csv('heterogeneous_campaign.csv')
    for cm_size in campaign_sizes:
        print('Current campaign size: %d' % cm_size)
        campaign, num_oper = df_to_lists(cmp=total_cmp, size=cm_size)
        for _ in range(repetitions):
            planner = RandomPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
            tic = time()
            plan = planner.plan()
            toc = time()
            makespan = get_makespan(plan)
            results.loc[len(results)]= [cm_size, 'RANDOM', plan, makespan, toc - tic]
            del planner

    results.to_csv('StHeteroCampaigns_4St050HeteroResourcesRAND.csv', index=False)
