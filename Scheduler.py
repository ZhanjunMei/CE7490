from base_timer import BaseTimer


class PASch_Scheduler(BaseTimer):
    def __init__(self,tasks,workers,mapper,logger):
        self.abs_time = 0
        self.tasks = tasks
        self.workers = workers
        self.mapper = mapper
        self.task_finished = 0
        self.logger = logger

    def get_affinity_workers(self, package_name):
        return self.mapper._find_two_closest_workers(package_name)

    def find_least_loaded_worker(self):
        min = float("inf")
        least_loaded_worker = None
        for worker in self.workers:
            if worker.get_load()< min:
                if worker.is_overload():
                    continue
                else:
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
            if worker1.is_overload():
                final_worker = self.find_least_loaded_worker()
            else:
                final_worker = worker1
        else:
            if worker2.is_overload():
                final_worker = self.find_least_loaded_worker()
            else:
                final_worker = worker2
                
        task = self.tasks.get_package_info(Function_ID)
        arrive_time = self.tasks.get_arrival_time(Function_ID)
        running_time = self.tasks.get_launch_and_running_time(Function_ID)
        self.logger.task_arrive(Function_ID, arrive_time)
        
        

        if final_worker == None:
            return False
        else:
            self.logger.task_alloc(Function_ID, self.abs_time)
            final_worker.set_task(task, Function_ID, running_time)
            return True

    def get_next_time(self):
        ans = self.tasks.get_arrival_time(self.task_finished+1)
        return ans

    def get_abs_next_time(self):
        ans = -1
        for arrival in self.tasks.get_arrivals():
            if arrival > self.abs_time:
                ans = arrival
                break
        if ans == -1:
            return float("inf")
        else:
            return ans


    def has_next(self):
        return self.task_finished + 1 <= len(self.tasks.get_packages())


    def step(self, time_step):
        self.abs_time += time_step
        while self.get_next_time() <= self.abs_time:
            bool = self.allocate(self.task_finished+1)
            if bool:
                self.task_finished+=1
                print(f"Task {self.task_finished} arrived at PASch_Scheduler at {self.abs_time}.")
            else:
                break

class Leastloaded_Scheduler(BaseTimer):
    def __init__(self,tasks,workers,mapper,logger):
        self.abs_time = 0
        self.tasks = tasks
        self.workers = workers
        self.mapper = mapper
        self.task_finished = 0
        self.logger = logger


    def find_least_loaded_worker(self):
        min = float("inf")
        least_loaded_worker = None
        for worker in self.workers:
            if worker.get_load()< min:
                if worker.is_overload():
                    continue
                elif worker.get_load() == 0:
                    least_loaded_worker = worker
                    break
                else:
                    min = worker.get_load()
                    least_loaded_worker = worker
        return least_loaded_worker
    
    def allocate(self, Function_ID):
        final_worker = self.find_least_loaded_worker()

        task = self.tasks.get_package_info(Function_ID)
        arrive_time = self.tasks.get_arrival_time(Function_ID)
        running_time = self.tasks.get_launch_and_running_time(Function_ID)
        self.logger.task_arrive(Function_ID, arrive_time)

        if final_worker == None:
            return False
        else:
            self.logger.task_alloc(Function_ID, self.abs_time)
            final_worker.set_task(task, Function_ID, running_time)
            return True

    def get_next_time(self):
        ans = self.tasks.get_arrival_time(self.task_finished+1)
        return ans

    def get_abs_next_time(self):
        ans = -1
        for arrival in self.tasks.get_arrivals():
            if arrival > self.abs_time:
                ans = arrival
                break
        if ans == -1:
            return float("inf")
        else:
            return ans


    def has_next(self):
        return self.task_finished + 1 <= len(self.tasks.get_packages())


    def step(self, time_step):
        self.abs_time += time_step
        while self.get_next_time() <= self.abs_time:
            bool = self.allocate(self.task_finished+1)
            if bool:
                self.task_finished+=1
                print(f"Task {self.task_finished} arrived at Leastloaded_Scheduler at {self.abs_time}.")
            else:
                break





class Hashaffinity_Scheduler(BaseTimer):
    def __init__(self,tasks,workers,mapper,logger):
        self.abs_time = 0
        self.tasks = tasks
        self.workers = workers
        self.mapper = mapper
        self.task_finished = 0
        self.logger = logger

    def get_affinity_worker(self, package_name):
        return self.mapper._find_closest_worker_byname(package_name)

    def find_least_loaded_worker(self):
        min = float("inf")
        least_loaded_worker = None
        for worker in self.workers:
            if worker.get_load()< min:
                if worker.is_overload():
                    continue
                else:
                    min = worker.get_load()
                    least_loaded_worker = worker
        return least_loaded_worker
    
    def allocate(self, Function_ID):

        package_name = self.tasks.get_largest_package_info(Function_ID)["name"]
        final_worker_name = self.get_affinity_worker(package_name)
        final_worker = None
        for worker in self.workers:
            if worker.get_name() == final_worker_name:
                if worker.is_overload():
                    break
                else:
                    final_worker = worker
                    break
            
        task = self.tasks.get_package_info(Function_ID)
        arrive_time = self.tasks.get_arrival_time(Function_ID)
        running_time = self.tasks.get_launch_and_running_time(Function_ID)
        self.logger.task_arrive(Function_ID, arrive_time)
        
        

        if final_worker == None:
            return False
        else:
            self.logger.task_alloc(Function_ID, self.abs_time)
            final_worker.set_task(task, Function_ID, running_time)
            return True

    def get_next_time(self):
        ans = self.tasks.get_arrival_time(self.task_finished+1)
        return ans

    def get_abs_next_time(self):
        ans = -1
        for arrival in self.tasks.get_arrivals():
            if arrival > self.abs_time:
                ans = arrival
                break
        if ans == -1:
            return float("inf")
        else:
            return ans


    def has_next(self):
        return self.task_finished + 1 <= len(self.tasks.get_packages())


    def step(self, time_step):
        self.abs_time += time_step
        while self.get_next_time() <= self.abs_time:
            bool = self.allocate(self.task_finished+1)
            if bool:
                self.task_finished+=1
                print(f"Task {self.task_finished} arrived at Hashaffinity_Scheduler at {self.abs_time}.")
            else:
                break