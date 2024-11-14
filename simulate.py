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
    scheduler,
):
    
    now_time = 0
    while True:
        # get_next_time() from all generators to fine minimum next_time
        min_times = [w.get_next_time() for w in workers]
        if scheduler.has_next():
            min_times.append(float(scheduler.get_abs_next_time())-now_time)
        min_time = min(min_times)
        

        #if before the max_time, all the workers have finished all the tasks
        if min_time == float("inf"):
            break
        # step(min_time) for all Timers
        for w in workers:
            w.step(min_time)
        if scheduler.has_next():
            scheduler.step(min_time)
        
        now_time += min_time

        logger.cal_co_var(workers, now_time)


    print("finish tasks")
    logger.print_log()


def run_simulates(
    log_dir,
    task_num = 2000,
    worker_num = 10,
    worker_th = 10,  # threads in each worker
    cache_size = 50 * 1024, # KB
):
    package_path = './pypi_package_data_1.csv'
    tasks_file = "./tasks_1_pop1.1.json"
    params = {
        "task_num": task_num,
        "worker_num": worker_num,
        "worker_th": worker_th,
        "cache_size": cache_size,
    }

    tasks = Functions(num=task_num, file_name=tasks_file)

    # for pasch
    logger = Logger(name="pasch", params=params, dir=log_dir)
    workers = [Worker(f"worker_{i}", worker_th, logger=logger, cache_size=cache_size) for i in range(worker_num)]
    mapper = ConsistentHashingWithPowerOfTwoChoices(workers, package_path)
    scheduler = PASch_Scheduler(tasks, workers, mapper, logger)
    run_simulate(logger, workers, scheduler)

    # for hash
    logger = Logger("hash", params=params, dir=log_dir)
    workers = [Worker(f"worker_{i}", worker_th, logger=logger, cache_size=cache_size) for i in range(worker_num)]
    mapper = ConsistentHashingWithPowerOfTwoChoices(workers, package_path)
    scheduler = Hashaffinity_Scheduler(tasks, workers, mapper, logger)
    run_simulate(logger, workers, scheduler)

    # for least loaded
    logger = Logger("leastloaded", params=params, dir=log_dir)
    workers = [Worker(f"worker_{i}", worker_th, logger=logger, cache_size=cache_size) for i in range(worker_num)]
    mapper = ConsistentHashingWithPowerOfTwoChoices(workers, package_path)
    scheduler = Leastloaded_Scheduler(tasks, workers, mapper, logger)
    run_simulate(logger, workers, scheduler)


def run_base():
    task_num = 3000
    worker_num = 100
    worker_th = 10
    cache_size = 50 * 1024
    run_simulates(f"log_main", task_num, worker_num, worker_th, cache_size)


if __name__ == '__main__':
    task_num = 3000
    worker_num = 100
    worker_th = 10
    cache_size = 0
    run_simulates(f"log_main_0cache", task_num, worker_num, worker_th, cache_size)

            
