import numpy as np
import random
import csv
import json

def arrival_generator(mean_interarrival_time=0.1, num_events=10):
    interarrival_times = np.random.exponential(scale=mean_interarrival_time, size=num_events)
    arrival_times = np.cumsum(interarrival_times)
    arrival_times = [float(a) for a in arrival_times]
    return arrival_times

def running_time_generator(mean=0.1, num_tasks=10):
    running_times = np.random.exponential(scale=mean, size=num_tasks)
    #plus 1s launching time
    return [float(a+1) for a in running_times]

# 读取 CSV 文件并访问数据
def read_package_data_from_csv(filename="pypi_package_data.csv"):
    package_data = []
    with open(filename, mode='r') as file:
        reader = csv.reader(file)
        header = next(reader)  # 跳过表头
        for row in reader:
            package_name = row[0]
            package_size = float(row[1])
            package_data.append((package_name, package_size))
    return package_data


def package_generator(num_tasks, package_path = "./pypi_package_data.csv"):

    pypi_packages = read_package_data_from_csv(package_path)
    popularity = np.random.zipf(1.1, len(pypi_packages))
    
    package_popularity = list(zip(pypi_packages, popularity))
    package_popularity.sort(key=lambda x: x[1], reverse=True)  # 按受欢迎程度降序排序

    # 2. 为每个任务生成包信息
    tasks = []
    for _ in range(num_tasks):
        # 为每个任务生成所需的包数量，遵循指数分布，平均数量为3
        num_packages = int(np.random.exponential(scale=3)) + 1  # 至少一个包
        
        # 根据受欢迎程度，选择包并添加到任务
        task_packages = []
        selected_packages = set() 
        for _ in range(num_packages):
            # 选择一个包，考虑受欢迎程度（更高的受欢迎程度包更有可能被选择）
            chosen_package, _ = random.choices(package_popularity, weights = np.array([p[1] for p in package_popularity], dtype=np.float64), k=1)[0]
            while chosen_package in selected_packages:
                chosen_package, _ = random.choices(package_popularity, weights = np.array([p[1] for p in package_popularity], dtype=np.float64), k=1)[0]
            selected_packages.add(chosen_package)
            package_name, package_size = chosen_package
            
            # 为该包生成启动时间，遵循指数分布，平均时间为4.007秒
            import_time = np.random.exponential(scale=4.007)
            
            # 添加包信息到任务
            task_packages.append({
                "name": package_name,
                "size": float(package_size),
                "import_time": float(import_time),
                "popularity": int(_)
            })
        
        # 将任务包信息添加到任务列表中
        tasks.append(task_packages)
    
    return tasks




class Functions:
    def __init__(self, num=None, file_name=None):
        if file_name is None:
            self.arrival_times = arrival_generator(mean_interarrival_time=0.1, num_events=num)
            self.packages = package_generator(num_tasks=num)
            self.launch_and_running_time = running_time_generator(mean = 0.1,num_tasks=num)
            self.task_num = num
            
            list_tasks = []
            for i in range(self.task_num):
                t = {
                    "id": i + 1,
                    "arrive_time": self.arrival_times[i],
                    "run_time": self.launch_and_running_time[i],
                    "pkgs": self.packages[i]
                }
                list_tasks.append(t)
            with open(f"tasks_{num}.json", "w", encoding="utf-8") as file_name:
                json.dump(list_tasks, file_name, ensure_ascii=False, indent=4)
        else:
            with open(file_name, "r", encoding="utf-8") as f:
                list_tasks = json.load(f)
            if num is None:
                num = len(list_tasks)
            else:
                num = min(len(list_tasks), num)
            self.arrival_times = [list_tasks[i]["arrive_time"] for i in range(num)]
            self.packages = [list_tasks[i]["pkgs"] for i in range(num)]
            self.launch_and_running_time = [list_tasks[i]["run_time"] for i in range(num)]
            self.task_num = num
        
        print("generate tasks finished")
        
    
    def get_last_arrival_time(self):
        return self.arrival_times[self.task_num-1]

    def get_packages(self):
        return self.packages 
    
    def get_arrivals(self):
        return self.arrival_times
    
    def get_launch_and_running_time(self, Function_ID):
        return self.launch_and_running_time[Function_ID - 1]

    def get_task_num(self):
        return self.task_num

    def get_arrival_time(self,Function_ID):
        if Function_ID>self.task_num:
            return float("inf")
        return self.arrival_times[Function_ID-1]
    
    def get_package_info(self,Function_ID):
        # self.packages[Function_ID]["name"]
        # self.packages[Function_ID]["size"]
        # self.packages[Function_ID]["import_time"]
        # self.packages[Function_ID]["populatiry"]
        return self.packages[Function_ID-1]
    
    def get_largest_package_info(self,Function_ID):
        max_size = -1
        package_ID = -1
        for i,package in enumerate(self.packages[Function_ID-1]):
            if package["size"]>max_size:
                max_size = package["size"]
                package_ID = i
        return self.packages[Function_ID-1][package_ID]





