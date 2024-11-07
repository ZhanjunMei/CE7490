from base_timer import BaseTimer
from worker import Worker
import sys
class PASch_Scheduler(BaseTimer):
    def __init__(self,tasks,workers,mapper):
        self.abs_time = 0
        self.tasks = tasks
        self.workers = workers
        self.mapper = mapper
        self.task_finished = 0

    def get_affinity_nodes(self, package_name):
        return self.mapper._find_two_closest_workers(package_name)

    def find_least_loaded_worker(self):
        min = sys.maxsize
        least_loaded_worker = None
        for worker in self.workers:
            if worker.get_load()< min:
                min = worker.get_load()
                least_loaded_worker = worker
        return least_loaded_worker
    
    def allocate(self, Function_ID):
        package_name = self.tasks.get_largest_package_info(Function_ID)["name"]
        worker1_name, worker2_name = self.get_affinity_workers(package_name)
        worker1_load = -1
        worker2_load = -1
        worker1 = None
        worker2 = None
        final_worker = None
        for worker in self.workers:
            if worker.get_name() == worker1_name:
                worker1_load = worker.get_load()
                worker1 = worker
            elif worker.get_name() == worker2_name:
                worker2_load = worker.get_load()
                worker2 = worker
        if worker1_load<=worker2_load:
            if worker1._is_overload():
                final_worker = self.find_least_loaded_worker()
            else:
                final_woker = worker1
        else:
            if worker2._is_overload():
                final_worker = self.find_least_loaded_worker()
            else:
                final_woker = worker2
        
        

        
        
            

    def get_next_time(self):
        ans = self.tasks.get_arrival_time(self.task_finished+1)
        if ans != -1:
            return ans
        else:
            print("PASch has no more tasks!!!")
            return -1


    def step(self, time_step):
        self.abs_time += time_step
        if self.get_next_time() == self.abs_time:
            self.allocate(self.task_finished+1)
            self.task_finished+=1
            print(f"Task {self.task_finished} arrived at PASch_Scheduler at {self.abs_time}.")



