from Data_Generator import Functions
from Scheduler import PASch_Scheduler
from Consistent_hash_mapping import ConsistentHashingWithPowerOfTwoChoices
def main():
    now_time = 0
    package_path = '/Users/meizhanjun/codes/CE7490_GroupProject/CE7490-Severless_Computing/CE7490/pypi_package_data.csv'
    workers = []
    Tasks = Functions(num = 100)
    mapper = ConsistentHashingWithPowerOfTwoChoices(workers,package_path)
    scheduler = PASch_Scheduler(Tasks,workers,mapper)
    while True:
        """
            get_next_time() from work_generator, worker, scheduler,
            to find minimum next time
        """
        # xxx
        """
            step(min_time) for all Timers
        """
        # now_time += min_time
