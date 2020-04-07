from radical.cm.planner import HeftPlanner, RandomPlanner, GAPlanner
import pandas as pd


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

    resources = [{'id': 1, 'performance': 1},
                 {'id': 2, 'performance': 1},
                 {'id': 3, 'performance': 1},
                 {'id': 4, 'performance': 1}]
    campaign_sizes = [4, 8, 16, 32, 64, 128, 256, 512, 1024]
    results = pd.DataFrame(columns=['size','planner','plan','makespan'])
    total_cmp = pd.read_csv('heterogeneous_campaign.csv')
    for cm_size in campaign_sizes:
        print('Current campaign size: %d' % cm_size)
        campaign, num_oper = df_to_lists(cmp=total_cmp, size=cm_size)
        heft_planner = HeftPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
        random_planner = RandomPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
        ga_planner = GAPlanner(campaign=campaign, resources=resources, num_oper=num_oper)
        for _ in range(10):
            plan = heft_planner.plan(campaign=campaign, resources=resources, num_oper=num_oper)
            makespan = get_makespan(plan)
            try:
                os.remove('rcm.planner.log')
            except:
                pass
            results.loc[len(results)] = [cm_size, 'HEFT', plan, makespan]

        for _ in range(10):
            plan = random_planner.plan(campaign=campaign, resources=resources, num_oper=num_oper)
            makespan = get_makespan(plan)
            try:
                os.remove('rcm.planner.log')
            except:
                pass
            results.loc[len(results)] = [cm_size, 'RANDOM', plan, makespan]

        for _ in range(10):
            plan = ga_planner.plan()
            makespan = get_makespan(plan)
            try:
                os.remove('rcm.planner.log')
            except:
                pass
            results.loc[len(results)]= [cm_size, 'GA', plan, makespan]
    results.to_csv('StHeteroCampaigns_4St075HeteroResources.csv', index=False)
