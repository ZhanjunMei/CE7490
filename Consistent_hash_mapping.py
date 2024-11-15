import hashlib
import random
from Data_Generator import read_package_data_from_csv


def hash_fn(key, salt=""):
    """使用标准哈希函数将 key + salt 映射到哈希环上的一个点。"""
    return int(hashlib.md5((key + salt).encode()).hexdigest(), 16) % 1000000



class ConsistentHashingWithPowerOfTwoChoices:
    def __init__(self, workers, pacakage_pth):
        self.worker_points = {}
        self.package_points = {}
        self.workers = workers
        self.package_path = pacakage_pth
        self.packages = read_package_data_from_csv(self.package_path)
        # print(self.packages[0][1])

        # 初始化所有 worker 节点和包在哈希环上的位置
        self._init_worker_points()
        self._init_package_points()

        self.salts = [f"salt_{i}" for i in range(len(workers))]

    def _init_worker_points(self):
        """将所有 worker 节点映射到哈希环上，确保每个 worker 的哈希值唯一。"""
        for worker in self.workers:
            point = hash_fn(worker.get_name())
            # 确保哈希值唯一，避免冲突
            while point in self.worker_points.values():
                point = hash_fn(worker.get_name(), salt=str(random.randint(0, 10000)))
            self.worker_points[worker.get_name()] = point
        # 按位置排序，以便顺时针查找最近节点
        self.worker_points = dict(sorted(self.worker_points.items(), key=lambda x: x[1]))

    def _init_package_points(self):
        """将所有包映射到哈希环上。"""
        for package in self.packages:
            point = hash_fn(package[0])
            self.package_points[package[0]] = point
        # 按位置排序，以便顺时针查找最近的 worker
        self.package_points = dict(sorted(self.package_points.items(), key=lambda x: x[1]))


    def _find_closest_workers(self, pkg_name, n):
        worker_names = set()
        workers = []
        for i in range(n):
            if i >= len(self.workers):
                break
            salt = self.salts[i]
            point_i = hash_fn(pkg_name, salt)
            worker = self._find_closest_worker(point_i)
            while worker in worker_names:
                salt = str(random.randint(0, 10000))
                point_i = hash_fn(pkg_name, salt)
                worker = self._find_closest_worker(point_i)
            workers.append(worker)
        while len(workers) < n:
            workers.append(None)
        return workers


    def _find_two_closest_workers(self, package_name):
        """找到指定包最近的两个 worker 节点，一个使用原始名称，一个使用加盐名称。"""
        point1 = hash_fn(package_name)              # 使用原始名称哈希
        salt = "salt"                               # 加盐
        point2 = hash_fn(package_name, salt)        # 使用加盐哈希

        # 找到两个不同的 worker 节点
        worker1 = self._find_closest_worker(point1)
        worker2 = self._find_closest_worker(point2)

        # 确保 worker1 和 worker2 不同
        while worker2 == worker1:
            salt = str(random.randint(0, 10000))   # 再次生成新盐值
            point2 = hash_fn(package_name, salt)
            worker2 = self._find_closest_worker(point2)

        return worker1, worker2

    def _find_closest_worker(self, point):
        """顺时针查找最近的 worker。"""
        for worker, worker_point in self.worker_points.items():
            if point <= worker_point:
                return worker
        # 若未找到，则返回第一个节点
        return next(iter(self.worker_points))
    
    def _find_closest_worker_byname(self,package_name):
        """顺时针查找最近的 worker。"""
        point = hash_fn(package_name) 
        for worker, worker_point in self.worker_points.items():
            if point <= worker_point:
                return worker
        # 若未找到，则返回第一个节点
        return next(iter(self.worker_points))

    # def assign_worker_for_task(self, package_name):
    #     """根据包的哈希值，找到最优的 worker 节点来处理任务。"""
    #     worker1, worker2 = self._find_two_closest_workers(package_name)
        
    #     # 模拟根据负载情况选择负载更低的 worker（或其他自定义规则）
    #     return worker1 if random.choice([True, False]) else worker2

