import numpy as np
import csv

class Logger():
    
    def __init__(self, name):
        self.name = name
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
        mu = np.mean(np_arr)
        sigma = np.std(np_arr, ddof=1)
        self.co_var_t.append(t)
        self.co_var_sigma.append(sigma)
        self.co_var_mu.append(mu)


    def task_arrive(self, task_id, t):
        entry = {"arrive": t, "alloc": float("inf"), "finish": float("inf")}
        self.tasks[str(task_id)] = entry

    
    def task_alloc(self, task_id, t):
        self.tasks[str(task_id)]["alloc"] = t


    def task_finish(self, task_id, t):
        self.tasks[str(task_id)]["finish"] = t
        

    def print_log(self):
        print("========== log ==========")
        
        print("--- hit rate ---")
        print(f"pkgs: {self.pkg_num}, cached: {self.cached_pkg_num}, \
              rate: {self.cached_pkg_num / self.pkg_num}")
        
        print("--- coefficient of variation ---")
        vals = [self.co_var_sigma[i] / self.co_var_mu[i] for i in range(len(self.co_var_t))]
        print(f"co_val mean: {float(np.mean(vals))}")
        with open(f"{self.name}_coval.csv", mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["abs_time", "mu", "sigma", "value"])
            for i in range(len(self.co_var_t)):
                sigma = self.co_var_sigma[i]
                mu = self.co_var_mu[i]
                t = self.co_var_t[i]
                writer.writerow([t, mu, sigma, sigma / mu])

        print("--- finish time ---")
        vals = [v["finish"] - v["arrive"] for v in self.tasks.values()]
        print(f"finish mean time: {float(np.mean(vals))} s")
        with open(f"{self.name}_finishtime.csv", mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "arrive", "finish", "value"])
            for k in self.tasks.keys():
                arrive_t = self.tasks[k]["arrive"]
                finish_t = self.tasks[k]["finish"]
                writer.writerow([k, arrive_t, finish_t, finish_t - arrive_t])
    

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
        