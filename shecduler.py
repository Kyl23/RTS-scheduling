# %% dependencies
import json
import numpy as np

# %% read configuration
config_file = "./config1.json"
config = None
with open(config_file, "rb") as f:
    config = json.load(f)

    print(len(config))
# %% scheduler config
until_time = 100

# %% init params
time_store = np.zeros((until_time + 1), 'int64')
reject_rate_store = {
    "P" : 0,
    "A" : 0,
    "S" : 0
}
global_job = []

def check_available_time(start, end, cost):
    if end > until_time:
        end = until_time
    
    count = 0

    for i in range(start + 1, end + 1):
        if time_store[i] == 0:
            count += 1
        if count == cost:
            return True

    return False


def insert_job2store(start, cost):
    count = 0
    start_time = None
    for i in range(start + 1, until_time + 1):
        if time_store[i] == 0:
            if start_time == None:
                start_time = i - 1
            time_store[i] = 1
            count += 1
            if count == cost:
                return [start_time, i]

# %% setup parse function
def check_cost_large_than_period(obj, period_key):
    return True if obj[period_key] < obj["C"] else False

def make_reject_msg(task_name, id):
    return f'{task_name} {id} -1 -1 Reject'

def make_complete_msg(task_name, id, start, end):
    return f'{task_name} {id} {start} {end} Complete'

def parse_periodic(obj, global_job):
    origin_obj = obj.copy()
    tmp_obj = []

    for i in range(len(obj)):
        task = obj[i]

        if check_cost_large_than_period(task, "P"):
            global_job.append(task)
            task['A'] = 0
            task['msg'] = make_reject_msg("P", i)
            del task['P'] 
            
            reject_rate_store["P"] += 1
            continue
        
        period = task['P']
        cost = task['C']
        for start in range(0, until_time, period):
            tmp_obj.append({
                'A' : start,
                'C' : cost,
                'id': i
            })

    obj = tmp_obj
    obj = sorted(obj, key=lambda x: x['A'] + x['C'])

    rejected_id = set()

    for i in range(len(obj)):
        task = obj[i]
        global_job.append(task)
        start = task['A']
        cost = task['C']
        end = start + cost

        id = task['id'] if 'id' in task else i
        if start <= until_time and check_available_time(start, end, cost):
            [start, end] = insert_job2store(start, cost)
            task['msg'] = make_complete_msg("P", id, start, end)
        else:
            task['msg'] = make_reject_msg("P", id)
            reject_rate_store["P"] += 1
            rejected_id.add(id)

    reject_rate_store["P"] /= len(origin_obj)
    return

def parse_aperiodic(obj, global_job):
    for i in range(len(obj)):
        obj[i]['id'] = i

    obj = sorted(obj, key=lambda x: x['A'] + x['C'])

    for i in range(len(obj)):
        task = obj[i]
        global_job.append(task)
        start = task['A']
        cost = task['C']
        end = until_time

        id = task['id'] if 'id' in task else i
        if start <= until_time and check_available_time(start, end, cost):
            [start, end] = insert_job2store(start, cost)
            task['msg'] = make_complete_msg("A", id, start, end)
        else:
            task['msg'] = make_reject_msg("A", id)
            reject_rate_store["A"] += 1

    reject_rate_store["A"] /= len(obj)
    return


def parse_sporadic(obj, global_job):
    for i in range(len(obj)):
        obj[i]['id'] = i
        
    obj = sorted(obj, key=lambda x: x['A'] + x['C'])

    for i in range(len(obj)):
        task = obj[i]
        global_job.append(task)
        start = task['A']
        cost = task['C']
        end = start + cost

        id = task['id'] if 'id' in task else i
        if start <= until_time and check_available_time(start, end, cost):
            [start, end] = insert_job2store(start, cost)
            task['msg'] = make_complete_msg("S", id, start, end)
        else:
            task['msg'] = make_reject_msg("S", id)
            reject_rate_store["S"] += 1

    reject_rate_store["S"] /= len(obj)
    return


config_parsing = {
    "Periodic": parse_periodic,
    "Aperiodic": parse_aperiodic,
    "Sporadic": parse_sporadic
}

def shedule(schedule_order, json, global_job):
    for key in schedule_order:
        value = json[key]
        config_parsing[key](value, global_job)
    return


# %%
scheduling_order = ["Periodic", "Sporadic", "Aperiodic"]
for i in range(len(config)):
    print(i)
    shedule(scheduling_order, config[i], global_job)

    global_job = sorted(global_job, key=lambda x: x['A'])
    for job in global_job:
        print(job['msg'])
    
    # drop data & ending alpha
    for index, value in enumerate(reject_rate_store.values()):
        print(value, end=" " if index != 2 else "\n")

    

# %%
print(-1)