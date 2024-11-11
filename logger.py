import numpy as np
import csv
import os
import json


class Logger():
    
    def __init__(self, name, params, dir):
        self.name = name
        self.params = params
        self.dir = "./logs/" + dir
        self.logs = []
        # for hit rate
        self.pkg_num = 0
        self.cached_pkg_num = 0
        # for coefficient of variation
        self.co_var_t = []
        self.co_var_sigma = []
        self.co_var_mu = []
        # for task arriving and finishing
        self.tasks = {}


    def add_pkg_num(self, num):
        self.pkg_num += num

    
    def add_cached_num(self, num):
        self.cached_pkg_num += num


    def cal_co_var(self, workers, t):
        tasks = [w.get_load() for w in workers]
        np_arr = np.array(tasks)
        if np.all(np_arr == 0):
            return
        mu = np.mean(np_arr)
        sigma = np.std(np_arr, ddof=1)
        self.co_var_t.append(t)
        self.co_var_sigma.append(sigma)
        self.co_var_mu.append(mu)


    def task_arrive(self, task_id, t):
        entry = {"arrive": t, "alloc": float("inf"), "load": float("inf"), "finish": float("inf")}
        self.tasks[str(task_id)] = entry

    
    def task_load(self, task_id, t):
        self.tasks[str(task_id)]["load"] = t

    
    def task_alloc(self, task_id, t):
        self.tasks[str(task_id)]["alloc"] = t


    def task_finish(self, task_id, t):
        self.tasks[str(task_id)]["finish"] = t
        

    def print_log(self):
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        log_strs = []
        log_strs.append("========== log ==========")
        
        # calculate hit rate
        log_strs.append("--- hit rate ---")
        log_strs.append(f"pkgs: {self.pkg_num}, cached: {self.cached_pkg_num}, \
            rate: {self.cached_pkg_num / self.pkg_num}")
        
        # calculate coefficient of variation
        log_strs.append("--- coefficient of variation ---")
        vals = [self.co_var_sigma[i] / self.co_var_mu[i] for i in range(len(self.co_var_t))]
        log_strs.append(f"co_val mean: {float(np.mean(vals))}")

        with open(f"{self.dir}/{self.name}_coval.csv", mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["abs_time", "mu", "sigma", "value"])
            for i in range(len(self.co_var_t)):
                sigma = self.co_var_sigma[i]
                mu = self.co_var_mu[i]
                t = self.co_var_t[i]
                writer.writerow([t, mu, sigma, sigma / mu])

        # calculate finish time
        log_strs.append("--- finish time ---")
        vals = [v["finish"] - v["arrive"] for v in self.tasks.values()]
        log_strs.append(f"finish mean time: {float(np.mean(vals))} s")

        with open(f"{self.dir}/{self.name}_finishtime.csv", mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "arrive", "alloc", "load", "finish", "value"])
            for k in self.tasks.keys():
                arrive_t = self.tasks[k]["arrive"]
                alloc_t = self.tasks[k]["alloc"]
                load_t = self.tasks[k]["load"]
                finish_t = self.tasks[k]["finish"]
                writer.writerow([k, arrive_t, alloc_t, load_t, finish_t, finish_t - arrive_t])
    
        # print simple log
        for s in log_strs:
            print(s)

        with open(f"{self.dir}/{self.name}_log.txt", "w") as f:
            for s in log_strs:
                f.write(s + "\n")
                
        with open(f"{self.dir}/{self.name}_params.json", "w") as f:
            json.dump(self.params, f, indent=4)


    def log_info(self,
        subject,
        abs_time,
        type="info",
        info="",
        value=0
    ):
        log = {
            "subject": subject,
            "abs_time": abs_time,
            "type": type,
            "info": info,
            "value": value
        }
        self.logs.append(log)
        