import numpy
import math

from Data_Generator import Functions
from Scheduler import PASch_Scheduler
from Consistent_hash_mapping import ConsistentHashingWithPowerOfTwoChoices
from worker import Worker
from logger import Logger

numpy.random.seed(0)


def main():
    task_num = 500
    worker_num = 200
    worker_th = 50  # threads in each worker
    max_time = 1000 * 60  # seconds
    cache_size = 10 * 1024 # KB
    package_path = './pypi_package_data.csv'
    
    logger = Logger("pasch")
    workers = [Worker(f"worker_{i}", worker_th, logger=logger, cache_size=cache_size) for i in range(worker_num)]
    Tasks = Functions(num = task_num)
    mapper = ConsistentHashingWithPowerOfTwoChoices(workers, package_path)
    scheduler = PASch_Scheduler(Tasks, workers, mapper, logger)
    
    now_time = 0
    while scheduler.has_next() and now_time < max_time:

        # get_next_time() from all generators to fine minimum next_time
        min_times = [w.get_next_time() for w in workers]
        min_times.append(float(scheduler.get_next_time())-now_time)
        min_time = min(min_times)
        
        # step(min_time) for all Timers
        for w in workers:
            w.step(min_time)
        scheduler.step(min_time)
        
        now_time += min_time

        logger.cal_co_var(workers, now_time)
    
    # no more new tasks, waiting for existing tasks to be finished
    while True:
        min_times = [w.get_next_time() for w in workers]
        min_time = min(min_times)
        if math.isinf(min_time):
            break
        for w in workers:
            w.step(min_time)
        now_time += min_time

    print("finish tasks")
    logger.print_log()


if __name__ == '__main__':
    main()