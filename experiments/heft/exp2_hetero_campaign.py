from radical.cm.planner import HeftPlanner
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

def resdf_to_dict(res_df, size):

    tmp_resources = list()
    for i in range(size):
        point = res_df.loc[i]
        tmp_res = {'id': int(point['id']),
                   'performance': point['performance']}
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
    total_cmp = pd.read_csv('heterogeneous_campaign.csv')
    campaign, num_oper = df_to_lists(cmp=total_cmp, size=1024)
    results = pd.DataFrame(columns=['size','planner','plan','makespan', 'time'])
    for res_num in num_resources:
        print('Number of resources: %d' % res_num)
        resources = resdf_to_dict(res_df=total_resources, size=res_num)
        for _ in range(repetitions):
            planner = HeftPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
            tic = time()
            plan = planner.plan()
            toc = time()
            makespan = get_makespan(plan)
            results.loc[len(results)]= [res_num, 'HEFT', plan, makespan, toc - tic]
            del planner

    results.to_csv('HeteroResources_StHeteroCampaignsHEFT.csv', index=False)
