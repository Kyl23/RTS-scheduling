# %% dependencies
import json
import numpy as np
import sys
import pandas as pd
import plotly.express as px

# %% read configuration
config_file = sys.argv[1]
config = None
with open(config_file, "rb") as f:
    config = json.load(f)

    print(len(config))
# %% scheduler config
until_time = 100

# %% init params
class Scheduler:
    scheduling_order = ["Periodic", "Sporadic", "Aperiodic"]
    time_store = np.zeros((until_time + 1), 'int64')
    reject_rate_store = {
        "P" : 0,
        "A" : 0,
        "S" : 0,
    }
    global_job = []

    def check_available_time(self, start, end, cost):
        if end > until_time:
            end = until_time
        
        count = 0

        for i in range(start + 1, end + 1):
            if self.time_store[i] == 0:
                count += 1
            if count == cost:
                return True

        return False


    def insert_job2store(self, start, cost):
        count = 0
        start_time = None
        mask = np.ones((until_time + 1))
        for i in range(start + 1, until_time + 1):
            if self.time_store[i] == 0:
                if start_time == None:
                    start_time = i - 1
                self.time_store[i] = 1
                mask[i] = 0
                count += 1
                if count == cost:
                    return [start_time, i, mask]

    def check_cost_large_than_period(self, obj, period_key):
        return True if obj[period_key] < obj["C"] else False

    def make_reject_msg(self, task_name, id):
        return f'{task_name} {id} -1 -1 Reject'

    def make_complete_msg(self, task_name, id, start, end):
        return f'{task_name} {id} {start} {end} Complete'

    def parse_periodic(self, obj):
        origin_obj = obj.copy()
        tmp_obj = []
        id_mask = {}

        obj = sorted(obj, key=lambda x: x['P'])

        for i in range(len(obj)):
            task = obj[i]

            if self.check_cost_large_than_period(task, "P"):
                self.global_job.append(task)
                task['A'] = 0
                task['msg'] = self.make_reject_msg("P", i)
                task['name'] = f'P {i}'
                del task['P'] 

                self.reject_rate_store["P"] += 1
                continue
            
            period = task['P']
            cost = task['C']
            for start in range(0, until_time, period):
                tmp_obj.append({
                    'A' : start,
                    'C' : cost,
                    'P' : period,
                    'id': i,
                    'msg': ""
                })

        obj = tmp_obj

        rejected_id = set()

        for i in range(len(obj)):
            task = obj[i]
            self.global_job.append(task)
            start = task['A']
            cost = task['C']
            end = start + task['P']

            id = task['id'] if 'id' in task else i
            task['name'] = f'P {id}'
            if id not in id_mask:
                id_mask[id] = np.ones((until_time + 1))

            if (end > until_time or id in rejected_id) and not self.check_available_time(start, end, cost):
                continue
            if start <= until_time and self.check_available_time(start, end, cost):
                [start, end, mask] = self.insert_job2store(start, cost)
                task['msg'] = self.make_complete_msg("P", id, start, end)
                task['start'] = start
                task['end'] = end
                id_mask[id] *= mask
            else:
                task['msg'] = self.make_reject_msg("P", id)
                self.reject_rate_store["P"] += 1
                rejected_id.add(id)
                self.time_store *= id_mask[id].astype('int32')

            tmp_global_job = []
            for task in self.global_job:
                if 'id' in task and task['id'] in rejected_id:
                    continue
                tmp_global_job.append(task)
            
            self.global_job = tmp_global_job
        self.reject_rate_store["P"] /= (len(origin_obj))
        return

    def parse_aperiodic(self, obj):
        for i in range(len(obj)):
            obj[i]['id'] = i

        obj = sorted(obj, key=lambda x: x['A'] + x['C'])

        for i in range(len(obj)):
            task = obj[i]
            self.global_job.append(task)
            start = task['A']
            cost = task['C']
            end = until_time

            id = task['id'] if 'id' in task else i
            task['name'] = f'A {id}'
            if start <= until_time and self.check_available_time(start, end, cost):
                [start, end, mask] = self.insert_job2store(start, cost)
                task['msg'] = self.make_complete_msg("A", id, start, end)
                task['start'] = start
                task['end'] = end
            else:
                task['msg'] = self.make_reject_msg("A", id)
                self.reject_rate_store["A"] += 1

        self.reject_rate_store["A"] /= len(obj)
        return


    def parse_sporadic(self, obj):
        for i in range(len(obj)):
            obj[i]['id'] = i
            
        obj = sorted(obj, key=lambda x: x['A'] + x['C'])

        for i in range(len(obj)):
            task = obj[i]
            self.global_job.append(task)
            start = task['A']
            cost = task['C']
            end = start + cost

            id = task['id'] if 'id' in task else i
            task['name'] = f'S {id}'
            if start <= until_time and self.check_available_time(start, end, cost):
                [start, end, mask] = self.insert_job2store(start, cost)
                task['msg'] = self.make_complete_msg("S", id, start, end)
                task['start'] = start
                task['end'] = end
            else:
                task['msg'] = self.make_reject_msg("S", id)
                self.reject_rate_store["S"] += 1

        self.reject_rate_store["S"] /= len(obj)
        return


    config_parsing = {
        "Periodic": parse_periodic,
        "Aperiodic": parse_aperiodic,
        "Sporadic": parse_sporadic
    }

    def shedule(self, json):
        for key in self.scheduling_order:
            value = json[key]
            self.config_parsing[key](self, value)
        return

    def init(self):
        self.time_store = np.zeros((until_time + 1), 'int64')
        self.reject_rate_store = {
            "P" : 0,
            "A" : 0,
            "S" : 0,
        }
        self.global_job = []
    
    def plot(self):
        tmp = []
        for task in self.global_job:
            if 'start' not in task or task['start'] == -1 or task['end'] == -1:
                continue
            tmp.append({
                "Task":  task['name'],
                "Start": np.datetime64(f"{task['start']+2000}-01-01"),
                "End": np.datetime64(f"{task['end']+2000}-01-01")
            })

        df = pd.DataFrame(tmp)

        fig = px.timeline(df, x_start="Start", x_end="End", y="Task")
        fig.update_yaxes(autorange="reversed") # otherwise tasks are listed from the bottom up

        fig.show()
# %%
scheduler = Scheduler()
for i in range(len(config)):
    print(i)
    scheduler.init()
    scheduler.shedule(config[i])
    
    scheduler.global_job = sorted(scheduler.global_job, key=lambda x: x['A'])
    scheduler.global_job = sorted(scheduler.global_job, key=lambda x: not x['msg'].endswith("Reject"))
    for job in scheduler.global_job:
        if job['msg'] != "":
            print(job['msg'])
    
    # drop data & ending alpha
    
    for index, value in enumerate(scheduler.reject_rate_store.values()):
        print(value, end=" " if index != 2 else "\n")

    scheduler.plot()
# %%
print(-1)

# %%
