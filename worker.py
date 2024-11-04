import sys

from base_timer import BaseTimer

class WorkerThread(BaseTimer):


    def __init__(self, ):
        self.is_working = False
        self.abs_time = 0
        self.remain_time = 0
        self.required_pkgs = {}

        self.last_finish_time = 0


    def get_next_time(self):
        if self.is_working:
            return self.remain_time
        return sys.maxsize

    
    def step(self, time_step):
        if self.is_working and time_step >= self.remain_time:
            self.last_finish_time = self.abs_time + self.remain_time
            self.is_working = False
        
        ret = {
            "is_working": self.is_working,
            "finish_time": self.last_finish_time
        }

        self.abs_time += time_step
        return ret


class Worker(BaseTimer):

    def __init__(self,
        threshold,
        cache_size,
        logger=None
    ):
        self.threshold = threshold
        self.cache_size = cache_size
        
        self.threads = [WorkerThread() for _ in range(threshold)]
        self.cached_pkgs = {}


    def get_next_time(self):
        next_time = sys.maxsize
        for t in self.threads:
            next_time = min(next_time, t.get_next_time())
        return next_time


    def step(self, time_step):
        ret = []
        for t in self.threads:
            t_ret = t.step(time_step)
            ret.append(t_ret)
        return ret
    

    def set_task(pkgs):
        pass
