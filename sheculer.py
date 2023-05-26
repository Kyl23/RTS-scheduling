# %% dependencies
import json
import numpy as np

# %% read configuration
config_file = "./config.json"
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

def print_reject(task_name, id):
    print(f'{task_name} {id} -1 -1 Reject')

def print_complete(task_name, id, start, end):
    print(f'{task_name} {id} {start} {end} Complete')

def parse_periodic(obj):
    for i in range(len(obj)):
        task = obj[i]
        
        if check_cost_large_than_period(task, "P"):
            print_reject("P", i)
            reject_rate_store["P"] += 1

            continue

        period = task['P']
        cost = task['C']
        for start in range(0, until_time, period):
            end = start + period

            if start <= until_time and check_available_time(start, end, cost):
                [start, end] = insert_job2store(start, cost)
                print_complete("P", i, start, end)
            else:
                print_reject("P", i)
                reject_rate_store["P"] += 1

    reject_rate_store["P"] /= len(obj)
    return


def parse_aperiodic(obj):
    for i in range(len(obj)):
        task = obj[i]
        start = task['A']
        cost = task['C']

        end = until_time
        if start <= until_time and check_available_time(start, end, cost):
            [start, end] = insert_job2store(start, cost)
            print_complete("A", i, start, end)
        else:
            print_reject("A", i)
            reject_rate_store["A"] += 1

    reject_rate_store["A"] /= len(obj)
    return


def parse_sporadic(obj):
    for i in range(len(obj)):
        task = obj[i]
        start = task['A']
        cost = task['C']

        end = start + cost
        if start <= until_time and check_available_time(start, end, cost):
            [start, end] = insert_job2store(start, cost)
            print_complete("S", i, start, end)
        else:
            print_reject("S", i)
            reject_rate_store["S"] += 1

    reject_rate_store["S"] /= len(obj)
    return


config_parsing = {
    "Periodic": parse_periodic,
    "Aperiodic": parse_aperiodic,
    "Sporadic": parse_sporadic
}


def shedule(schedule_order, json):
    for key in schedule_order:
        value = json[key]
        config_parsing[key](value)
    return


# %%
scheduling_order = ["Periodic", "Sporadic", "Aperiodic"]
shedule(scheduling_order, config[0])

# %% drop data & ending alpha
for value in reject_rate_store.values():
    print(value, end=" ")
print("")
print(-1)

# %%
