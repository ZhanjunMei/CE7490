import numpy
import math
import sys
from Data_Generator import Functions
from Scheduler import PASch_Scheduler,Hashaffinity_Scheduler,Leastloaded_Scheduler
from Consistent_hash_mapping import ConsistentHashingWithPowerOfTwoChoices
from worker import Worker
from logger import Logger

numpy.random.seed(0)


def run_simulate(
    logger,
    workers,
    tasks,
    scheduler,
    max_time=1000 * 60,  # seconds
):
    
    now_time = 0
    while now_time < max_time:

        # get_next_time() from all generators to fine minimum next_time
        min_times = [w.get_next_time() for w in workers]
        min_times.append(float(scheduler.get_abs_next_time())-now_time)
        min_time = min(min_times)
       
        #if before the max_time, all the workers have finished all the tasks
        if min_time == float("inf"):
            break
        # step(min_time) for all Timers
        for w in workers:
            w.step(min_time)
        scheduler.step(min_time)
        
        now_time += min_time

        logger.cal_co_var(workers, now_time)

    
    #if after the max_time, some tasks have not come yet
    if tasks.get_last_arrival_time() >= max_time:
        while scheduler.has_next():
             # get_next_time() from all generators to fine minimum next_time
            min_times = [w.get_next_time() for w in workers]
            min_times.append(float(scheduler.get_abs_next_time())-now_time)
            min_time = min(min_times)
            

            #if before the max_time, all the workers have finished all the tasks
            if min_time == float("inf"):
                break
            # step(min_time) for all Timers
            for w in workers:
                w.step(min_time)
            scheduler.step(min_time)
            
            now_time += min_time

            logger.cal_co_var(workers, now_time)
    #if after the max_time, no more new tasks, some taks are waiting in the queue.
    #Or no more new tasks, waiting for existing tasks to be finished
    else:
        while True:
            min_times = [w.get_next_time() for w in workers]
            min_time = min(min_times)

            if math.isinf(min_time):
                break
            for w in workers:
                w.step(min_time)
            now_time += min_time

            logger.cal_co_var(workers, now_time)

    print("finish tasks")
    logger.print_log()


def run_simulates():
    task_num = 20000
    worker_num = 1000
    worker_th = 100  # threads in each worker
    max_time = 30 * 60  # seconds
    cache_size = 500 * 1024 # KB
    package_path = './pypi_package_data_1.csv'
    tasks_file = "./tasks_20000_1.json"

    tasks = Functions(file_name=tasks_file)

    # for pasch
    logger = Logger("pasch")
    workers = [Worker(f"worker_{i}", worker_th, logger=logger, cache_size=cache_size) for i in range(worker_num)]
    
    mapper = ConsistentHashingWithPowerOfTwoChoices(workers, package_path)
    scheduler = PASch_Scheduler(tasks, workers, mapper, logger)
    run_simulate(logger, workers, tasks, scheduler, max_time)

    # for hash
    logger = Logger("hash")
    workers = [Worker(f"worker_{i}", worker_th, logger=logger, cache_size=cache_size) for i in range(worker_num)]
    mapper = ConsistentHashingWithPowerOfTwoChoices(workers, package_path)
    scheduler = Hashaffinity_Scheduler(tasks, workers, mapper, logger)
    run_simulate(logger, workers, tasks, scheduler, max_time)

    # for least loaded
    logger = Logger("leastloaded")
    workers = [Worker(f"worker_{i}", worker_th, logger=logger, cache_size=cache_size) for i in range(worker_num)]
    mapper = ConsistentHashingWithPowerOfTwoChoices(workers, package_path)
    scheduler = Leastloaded_Scheduler(tasks, workers, mapper, logger)
    run_simulate(logger, workers, tasks, scheduler, max_time)


if __name__ == '__main__':
    run_simulates()