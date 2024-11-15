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


class PASch_Scheduler_1(PASch_Scheduler):
    def __init__(self, tasks, workers, mapper, logger, n):
        super().__init__(tasks, workers, mapper, logger)
        self.n = n
    
    def get_affinity_workers(self, package_name):
        return self.mapper._find_closest_workers(package_name, self.n)
    
    def allocate(self, Function_ID):
        package_name = self.tasks.get_largest_package_info(Function_ID)["name"]
        worker_names = self.get_affinity_workers(package_name)
        worker_names = [w for w in worker_names if w is not None]
        workers = []
        for i in range(len(worker_names)):
            for j in range(len(self.workers)):
                if self.workers[j].get_name() == worker_names[i]:
                    workers.append(self.workers[j])
                    break
        loads = [w.get_load() for w in workers]

        worker = None
        while len(loads) > 0:
            least_idx = loads.index(min(loads))
            if workers[least_idx].is_overload():
                workers.pop(least_idx)
                loads.pop(least_idx)
                continue
            worker = workers[least_idx]
            break

        if worker is None:
            worker = self.find_least_loaded_worker()
            if worker.is_overload():
                worker = None
        
        task = self.tasks.get_package_info(Function_ID)
        arrive_time = self.tasks.get_arrival_time(Function_ID)
        running_time = self.tasks.get_launch_and_running_time(Function_ID)
        self.logger.task_arrive(Function_ID, arrive_time)

        if worker is None:
            return False
        else:
            self.logger.task_alloc(Function_ID, self.abs_time)
            worker.set_task(task, Function_ID, running_time)
            return True


class PASch_Scheduler_2(PASch_Scheduler):
    def __init__(self, tasks, workers, mapper, logger, n):
        super().__init__(tasks, workers, mapper, logger)
        self.n = n

    def allocate(self, Function_ID):
        pkg_info = self.tasks.get_package_info(Function_ID)
        pkg_info = sorted(pkg_info, key=lambda x: x["size"], reverse=True)
        package_name = ""
        for i in range(self.n):
            if i < len(pkg_info):
                package_name += pkg_info[i]["name"]
            else:
                package_name += "none"

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