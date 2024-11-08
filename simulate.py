from Data_Generator import Functions
from Scheduler import PASch_Scheduler
from Consistent_hash_mapping import ConsistentHashingWithPowerOfTwoChoices
from worker import Worker
from logger import Logger

def main():
    task_num = 100
    worker_num = 10
    worker_th = 10  # threads in each worker
    max_time = 100 * 60  # seconds
    package_path = './pypi_package_data.csv'
    
    logger = Logger()
    workers = [Worker(f"worker_{i}", worker_th, logger=logger) for i in range(worker_num)]
    Tasks = Functions(num = task_num)
    mapper = ConsistentHashingWithPowerOfTwoChoices(workers, package_path)
    scheduler = PASch_Scheduler(Tasks, workers, mapper)
    
    now_time = 0
    while now_time < max_time:

        # get_next_time() from all generators to fine minimum next_time
        min_times = [w.get_next_time() for w in workers]
        min_times.append(float(scheduler.get_next_time())-now_time)
        min_time = min(min_times)
        
        # step(min_time) for all Timers
        for w in workers:
            w.step(min_time)
        scheduler.step(min_time)
        
        now_time += min_time
    
    print("finish tasks")


if __name__ == '__main__':
    main()