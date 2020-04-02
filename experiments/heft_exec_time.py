from radical.cm.planner import HeftPlanner
from random import gauss
from time import time
import pandas as pd


def campaign_creator(num_workflows, heterogeneity=False):

    tmp_campaign = list()
    tmp_num_oper = list()
    for i in range(num_workflows):
        workflow = {'description':None}
        workflow['id'] = 'W%03d' % i
        if not heterogeneity:
            workflow['num_oper'] = 75000
        else:
            workflow['num_oper'] = gauss(75000, 6000)

        tmp_campaign.append(workflow)
        tmp_num_oper.append(workflow['num_oper'])

    return tmp_campaign, tmp_num_oper


def create_resources(num):
    tmp_resources = list()
    for i in range(num):
        tmp_resources.append({'id': i + 1,
                              'performance': i + 1,
                              'label': None})
    return tmp_resources


if __name__ == "__main__":

    campaign_sizes = [4, 8, 16, 32, 64, 128, 256, 512, 1024]
    resources = create_resources(4)
    results = pd.DataFrame(columns=['size','planner','Execution time'])
    for cm_size in campaign_sizes:
        print('Current campaign size: %d' % cm_size)
        campaign, num_oper = campaign_creator(num_workflows=cm_size, heterogeneity=True)
        heft_planner = HeftPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
        for _ in range(10000):
            tic = time()
            plan = heft_planner.plan(campaign=campaign, resources=resources, num_oper=num_oper)
            toc = time()
            results.loc[len(results)] = [cm_size, 'HEFT', toc - tic]
        results.to_csv('HEFT_TX_const_res.csv', index=False)

    rs_sizes = [1,2,4,8,16,32,64,128,256,512,1024]
    results = pd.DataFrame(columns=['size','planner','Execution time'])
    for rs_size in rs_sizes:
        print('Current number of resources: %d' % rs_size)
        campaign, num_oper = campaign_creator(num_workflows=1024, heterogeneity=True)
        heft_planner = HeftPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
        for _ in range(10000):
            tic = time()
            plan = heft_planner.plan(campaign=campaign, resources=resources, num_oper=num_oper)
            toc = time()
            results.loc[len(results)] = [rs_size, 'HEFT', toc - tic]

    results.to_csv('HEFT_TX_const_camp.csv', index=False)
