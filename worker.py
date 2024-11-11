import math
import sys

from base_timer import BaseTimer

class WorkerThread(BaseTimer):

    def __init__(self, worker, t_id, logger):
        """
            state: FREE, PREPARE_LOADING, LOADING, WORKING
            remain_time: useful except FREE
            loading_idx: useful only in PREPARE_LOADING and LOADING
        """
        self.worker = worker
        self.t_id = t_id  # thread id
        self.logger = logger
        
        self.state = "FREE"
        self.abs_time = 0
        self.remain_time = float("inf")
        self.required_pkgs = []
        self.loading_idx = 0  # next pkg to be loaded
        self.tid = 0  # task id
        self.running_time = 0

        
    def _prepare_loading_ch_state(self):
        while self.loading_idx < len(self.required_pkgs):
            pkg_name = self.required_pkgs[self.loading_idx]["name"]
            if not self.worker.has_cached_pkg(pkg_name):
                break
            self.loading_idx += 1
        # has pkg to load
        if self.loading_idx < len(self.required_pkgs):
            self.remain_time = self.required_pkgs[self.loading_idx]["import_time"]
            self.state = "LOADING"
        # all pkgs are loaded (and cached), ready to execute
        else:
            self.logger.task_load(self.tid, self.abs_time)
            self.state = "WORKING"
            if len(self.required_pkgs) == 0:
                self.remain_time = 1
            else:
                self.remain_time = self.running_time


    def get_next_time(self):
        return self.remain_time

    
    def step(self, time_step):
        self.abs_time += time_step

        if self.state == "FREE":
            pass
        
        elif self.state == "LOADING":
            if time_step < self.remain_time:
                self.remain_time -= time_step
            elif math.fabs(time_step - self.remain_time) < 1e-6:
                loaded_pkg = self.required_pkgs[self.loading_idx]
                self.worker.add_cache_pkg(loaded_pkg["name"], loaded_pkg["size"])
                self.loading_idx += 1
                self._prepare_loading_ch_state()
            else:
                print("[WorkerThread] step error: time_step greater than exe_time")
                sys.exit(1)

        elif self.state == "WORKING":
            if time_step < self.remain_time:
                self.remain_time -= time_step
            elif math.fabs(time_step - self.remain_time) < 1e-6:
                finish_time = self.abs_time
                self.logger.task_finish(self.tid, finish_time)
                print(f"{self.worker.name}_t_{self.t_id} finished work {self.tid} at {finish_time}")
                self.state = "FREE"
                self.remain_time = float("inf")
            else:
                print("[WorkerThread] step error: time_step greater than exe_time")
                sys.exit(1)
        
        
    
    
    def set_task(self, pkgs, fid, running_time):
        """
            pkgs = [
                {
                    'name': xxx,
                    'size': xxx (KB),
                    'import_time',
                    'popularity': np.int64
                }, {xxx}, ...]
        """
        
        if self.state != "FREE":
            print("[WorkerThread] set task failed: assign a task to an occupied thread")
            return
        
        self.required_pkgs = pkgs
        self.loading_idx = 0
        self.tid = fid
        self.running_time = running_time
        self._prepare_loading_ch_state()


class Worker(BaseTimer):

    def __init__(self,
        name,
        threshold,        
        cache_size=1 * 1024, # KB
        logger=None
    ):
        self.name = name
        self.threshold = threshold
        self.cache_size = cache_size
        self.logger = logger
        
        self.threads = [WorkerThread(self, i, logger) for i in range(threshold)]
        self.abs_time = 0
        self.cached_pkgs = []
        
    
    def is_overload(self):
        return self.get_load() >= self.threshold


    def get_load(self):
        load_num = 0
        for t in self.threads:
            if t.state != "FREE":
                load_num += 1
        return load_num
    

    def get_name(self):
        return self.name
    
    
    def get_next_time(self):
        next_time = float("inf")
        for t in self.threads:
            next_time = min(next_time, t.get_next_time())
        return next_time
    
    
    def has_cached_pkg(self, pkg_name):
        i = 0
        while i < len(self.cached_pkgs):
            if self.cached_pkgs[i]["name"] == pkg_name:
                break
            i += 1
        
        if i < len(self.cached_pkgs):
            hit_pkg = self.cached_pkgs.pop(i)
            self.cached_pkgs.append(hit_pkg)

        return i < len(self.cached_pkgs)

    
    def add_cache_pkg(self, pkg_name, pkg_size):
        if self.has_cached_pkg(pkg_name):
            return
        cached_size = 0
        for pkg in self.cached_pkgs:
            cached_size += pkg["size"]
        if pkg_size > self.cache_size:
            self.cached_pkgs = []
            return
        while cached_size + pkg_size > self.cache_size:
            cached_size -= self.cached_pkgs[0]["size"]
            self.cached_pkgs = self.cached_pkgs[1:]
    
        self.cached_pkgs.append({"name": pkg_name, "size": pkg_size})
        

    def step(self, time_step):
        for t in self.threads:
            t.step(time_step)
        
        self.abs_time += time_step
    

    def set_task(self, pkgs, fid, running_time):
        assigned = False
        for t in self.threads:
            if t.state == "FREE":
                t.set_task(pkgs, fid, running_time)
                assigned = True
                break

        # logging for hit rate
        if assigned:
            cached_names = set([p["name"] for p in self.cached_pkgs])
            all_names = set([p["name"] for p in pkgs])
            self.logger.add_pkg_num(len(all_names))
            self.logger.add_cached_num(len(cached_names & all_names))

        else:
            print("[Worker] set task failed: set task to an inavailable worker")
            exit(1)

        
