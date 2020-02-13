from radical.cm.planner import HeftPlanner, RandomPlanner
from random import gauss
import pandas as pd

def campaign_creator(num_workflows, heterogeneity=False):

    campaign = list()
    num_oper = list()
    for i in range(num_workflows):
        workflow = {'description':None}
        workflow['id'] = 'W%03d' % i
        if not heterogeneity:
            workflow['num_oper'] = 75000
        else:
            workflow['num_oper'] = gauss(75000, 6000)
        
        campaign.append(workflow)
        num_oper.append(workflow['num_oper'])

    return campaign, num_oper

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
    
    campaign_sizes = [4, 8, 16, 32, 64, 128, 256, 512, 1024]
    resources = [{'id': 1, 'performance': 1},
                 {'id': 2, 'performance': 2},
                 {'id': 3, 'performance': 3},
                 {'id': 4, 'performance': 4}]
    results = pd.DataFrame(columns=['size','planner','plan','makespan'])
    for cm_size in campaign_sizes:
        print('Current campaign size: %d' % cm_size)
        campaign, num_oper = campaign_creator(num_workflows=cm_size, heterogeneity=True)
        heft_planner = HeftPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
        random_planner = RandomPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
        for _ in range(10000):
            plan = heft_planner.plan(campaign=campaign, resources=resources, num_oper=num_oper)
            makespan = get_makespan(plan)
            results.loc[len(results)]= [cm_size, 'HEFT', plan, makespan]
        for _ in range(10000):
            plan = random_planner.plan(campaign=campaign, resources=resources, num_oper=num_oper)
            makespan = get_makespan(plan)
            results.loc[len(results)]= [cm_size, 'RANDOM', plan, makespan]
    results.to_csv('StHeteroCampaigns_4St100HeteroResources.csv', index=False)