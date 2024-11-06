from base_timer import BaseTimer
import sys
class PASch_Scheduler(BaseTimer):
    def __init__(self,):
        self.worker_num = 100
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