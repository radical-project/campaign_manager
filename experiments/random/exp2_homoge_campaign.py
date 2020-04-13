from radical.cm.planner import RandomPlanner
import pandas as pd
import sys
from time import time


def campaign_creator(num_workflows):

    tmp_campaign = list()
    tmp_num_oper = list()
    for i in range(num_workflows):
        workflow = {'description':None}
        workflow['id'] = i + 1
        workflow['num_oper'] = 75000
        
        tmp_campaign.append(workflow)
        tmp_num_oper.append(workflow['num_oper'])

    return tmp_campaign, tmp_num_oper


def resdf_to_dict(res_df, size):

    tmp_resources = list()
    for i in range(size):
        point = res_df.loc[i]
        tmp_res = {'id': int(point['id']),
                   'performance': 1.0}
        tmp_resources.append(tmp_res)

    return tmp_resources

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
    num_resources = [4, 8, 16, 32, 64, 128]
    total_resources = pd.read_csv('heterogeneous_resources.csv')
    campaign, num_oper = campaign_creator(num_workflows=1024)
    results = pd.DataFrame(columns=['size','planner','plan','makespan', 'time'])
    for res_num in num_resources:
        print('Number of resources: %d' % res_num)
        resources = resdf_to_dict(res_df=total_resources, size=res_num)
        for _ in range(repetitions):
            planner = RandomPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
            tic = time()
            plan = planner.plan()
            toc = time()
            makespan = get_makespan(plan)
            results.loc[len(results)]= [res_num, 'RANDOM', plan, makespan, toc - tic]
            del planner

    results.to_csv('HomogeResources_StHomogeCampaignsRAND.csv', index=False)
