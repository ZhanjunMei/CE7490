import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def draw_fin_t_w_new():
    x = [2, 5, 10, 20, 50, 100, 200, 500]
    pasch_1_y, pasch_y = [], []
    for w in x:
        dir = f"./logs/log_main_pasch3_w_{w}/"
        fname = dir + "pasch_1_log.txt"
        with open(fname, "r") as f:
            lines = f.readlines()
            for line in lines:
                if "finish mean time: " in line:
                    begin = line.find("finish mean time: ") + len("finish mean time: ")
                    num = float(line[begin:].replace(" s", ""))
                    pasch_1_y.append(num)
        fname = dir + "pasch_log.txt"
        with open(fname, "r") as f:
            lines = f.readlines()
            for line in lines:
                if "finish mean time: " in line:
                    begin = line.find("finish mean time: ") + len("finish mean time: ")
                    num = float(line[begin:].replace(" s", ""))
                    pasch_y.append(num)
    plt.plot(x, pasch_1_y, label='pasch_1')
    plt.plot(x, pasch_y, label='pasch')
    plt.xlabel('number of workers')
    plt.ylabel('averate finish time (s)')
    plt.yscale('log', )
    plt.title("Average finish time of pasch_1 and pasch")
    plt.legend()
    plt.savefig("fin_t_w_new.jpg")

draw_fin_t_w_new()


def draw_cv_w_new():
    x = [2, 5, 10, 20, 50, 100, 200, 500]
    pasch_1_y, pasch_y = [], []
    for w in x:
        dir = f"./logs/log_main_pasch3_w_{w}/"
        fname = dir + "pasch_1_log.txt"
        with open(fname, "r") as f:
            lines = f.readlines()
            for line in lines:
                if "co_val mean: " in line:
                    begin = line.find("co_val mean: ") + len("co_val mean: ")
                    num = float(line[begin:])
                    pasch_1_y.append(num)
        fname = dir + "pasch_log.txt"
        with open(fname, "r") as f:
            lines = f.readlines()
            for line in lines:
                if "co_val mean: " in line:
                    begin = line.find("co_val mean: ") + len("co_val mean: ")
                    num = float(line[begin:])
                    pasch_y.append(num)
    plt.plot(x, pasch_1_y, label='pasch_1')
    plt.plot(x, pasch_y, label='pasch')
    plt.xlabel('number of workers')
    plt.ylabel('coefficient of variation')
    plt.title("Coefficient of variation of pasch_1 and pasch with workers")
    plt.legend()
    plt.savefig("cv_w_new.jpg")


def draw_hr_w_new():
    x = [2, 5, 10, 20, 50, 100, 200, 500]
    pasch_1_y, pasch_y = [], []
    for w in x:
        dir = f"./logs/log_main_pasch3_w_{w}/"
        fname = dir + "pasch_1_log.txt"
        with open(fname, "r") as f:
            lines = f.readlines()
            for line in lines:
                if "rate: " in line:
                    begin = line.find("rate: ") + len("rate: ")
                    num = float(line[begin:])
                    pasch_1_y.append(num)
        fname = dir + "pasch_log.txt"
        with open(fname, "r") as f:
            lines = f.readlines()
            for line in lines:
                if "rate: " in line:
                    begin = line.find("rate: ") + len("rate: ")
                    num = float(line[begin:])
                    pasch_y.append(num)
    plt.plot(x, pasch_1_y, label='pasch_1')
    plt.plot(x, pasch_y, label='pasch')
    plt.xlabel('number of workers')
    plt.ylabel('hit rate')
    plt.title("Hit rate of pasch_1 and pasch")
    plt.legend()
    plt.savefig("hr_w_new.jpg")


def draw_hr_c_new():
    x = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 50, 100, 500]
    pasch_1_y = [0.009486, 0.0502, 0.0708, 0.163, 0.19775, 0.2838, 0.3253, 0.3798, 0.43278, 0.43529]
    pasch_y = [0.009389, 0.05091, 0.07492, 0.1595, 0.1843, 0.2593, 0.2980, 0.3598, 0.36743, 0.3670]
    plt.plot(x, pasch_1_y, label='pasch_1')
    plt.plot(x, pasch_y, label='pasch')
    plt.xlabel('cache memory (MB)')
    plt.ylabel('hit rate')
    plt.xscale('log')
    plt.xticks(x, [f'{xi}' for xi in x])
    plt.title("Hit rate of pasch_1 and pasch")
    plt.legend()
    plt.savefig("hr_c_new.jpg")


def draw_cv_c_new():
    x = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 50, 100, 500]
    pasch_1_y = [1.4362, 1.47255, 1.48395, 1.5501, 1.56286, 1.58879, 1.5644, 1.6201, 1.620455, 1.6084]
    pasch_y = [1.0900, 1.12613, 1.14157, 1.17646, 1.18561, 1.207, 1.2278, 1.17326, 1.16959, 1.1727]
    plt.plot(x, pasch_1_y, label='pasch_1')
    plt.plot(x, pasch_y, label='pasch')
    plt.xlabel('cache memory (MB)')
    plt.ylabel('coefficient of variation')
    plt.xscale('log')
    plt.xticks(x, [f'{xi}' for xi in x])
    plt.title("Coefficient of variation of pasch_1 and pasch")
    plt.legend()
    plt.savefig("cv_c_new.jpg")


def draw_fin_t_c_new():
    x = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 50, 100, 500]
    pasch_1_y = [14.8315, 14.3103, 14.0248, 12.727, 12.2262, 10.97065, 10.3769, 9.5765, 8.83305, 8.78114]
    pasch_y = [14.8204, 14.28102, 13.9575, 12.8145, 12.4442, 11.3618, 10.854, 9.92793, 9.75384, 9.7831]
    plt.plot(x, pasch_1_y, label='pasch_1')
    plt.plot(x, pasch_y, label='pasch')
    plt.xlabel('cache memory (MB)')
    plt.ylabel('averate finish time (s)')
    plt.xscale('log')
    plt.xticks(x, [f'{xi}' for xi in x])
    plt.title("Average finish time of pasch_1 and pasch")
    plt.legend()
    plt.savefig("fin_t_c_new.jpg")


def draw_fin_t_cache():
    x = [0.5, 1, 5, 10, 50, 100, 500]
    least_y = [14.47, 14.356, 13.883, 13.532, 12.789, 12.545, 12.617]
    hash_y = [123.74, 119.647, 114.237, 113.63, 41.66, 11.7368, 11.683]
    pasch_y = [12.86, 12.40, 11.458, 10.719, 9.9476, 9.738, 9.749]
    plt.plot(x, least_y, label='least loaded')
    plt.plot(x, hash_y, label='hash affinity')
    plt.plot(x, pasch_y, label='pasch')
    plt.xlabel('cache memory (MB)')
    plt.ylabel('finish time (s)')
    plt.legend()
    plt.savefig("fin_t_cache.jpg")

draw_fin_t_cache()


def draw_alloc_t():
    data_least = pd.read_csv(
        "logs\log_main\leastloaded_finishtime.csv", 
    )
    least_y = data_least["alloc"].to_numpy() - data_least["arrive"].to_numpy()

    data_hash = pd.read_csv(
        "logs\log_main\hash_finishtime.csv", 
    )
    hash_y = data_hash["alloc"].to_numpy() - data_hash["arrive"].to_numpy()

    data_pasch = pd.read_csv(
        "logs\log_main\pasch_finishtime.csv", 
    )
    pasch_y = data_pasch["alloc"].to_numpy() - data_pasch["arrive"].to_numpy()

    plt.hist(least_y, bins=80, density=True, label="least loaded", color='skyblue', edgecolor='darkblue', alpha=0.7)
    plt.hist(hash_y, bins=80, density=True, label="hash affinity",color='lightcoral', edgecolor='darkred', alpha=0.7)
    plt.hist(pasch_y, bins=80, density=True, label="pasch",color='lightgreen', edgecolor='forestgreen', alpha=0.7)
    plt.xlabel('Allocation time (s)')
    plt.ylabel('Probability Density')
    plt.title("Allocation time with Probability Density")
    plt.legend()
    print("alloc mean")
    print(least_y.mean(), hash_y.mean(), pasch_y.mean())
    plt.savefig("alloc_t.jpg")


def draw_load_t():
    data_least = pd.read_csv(
        "logs\log_main\leastloaded_finishtime.csv", 
    )
    least_y = data_least["load"].to_numpy() - data_least["alloc"].to_numpy()

    data_hash = pd.read_csv(
        "logs\log_main\hash_finishtime.csv", 
    )
    hash_y = data_hash["load"].to_numpy() - data_hash["alloc"].to_numpy()

    data_pasch = pd.read_csv(
        "logs\log_main\pasch_finishtime.csv", 
    )
    pasch_y = data_pasch["load"].to_numpy() - data_pasch["alloc"].to_numpy()

    plt.hist(least_y, bins=80, density=True, label="least loaded", color='skyblue', edgecolor='darkblue', alpha=0.7)
    plt.hist(hash_y, bins=80, density=True, label="hash affinity",color='lightcoral', edgecolor='darkred', alpha=0.7)
    plt.hist(pasch_y, bins=80, density=True, label="pasch",color='lightgreen', edgecolor='forestgreen', alpha=0.7)
    plt.xlabel('Loading time (s)')
    plt.ylabel('Probability Density')
    plt.title("Loading time with Probability Density")
    plt.legend()
    print("load mean")
    print(least_y.mean(), hash_y.mean(), pasch_y.mean())
    plt.savefig("load_t.jpg")


def draw_fin_t():
    data_least = pd.read_csv(
        "logs\log_main\leastloaded_finishtime.csv", 
    )
    least_y = data_least["value"].to_numpy()

    data_hash = pd.read_csv(
        "logs\log_main\hash_finishtime.csv", 
    )
    hash_y = data_hash["value"].to_numpy()

    data_pasch = pd.read_csv(
        "logs\log_main\pasch_finishtime.csv", 
    )
    pasch_y = data_pasch["value"].to_numpy()

    plt.hist(least_y, bins=80, density=True, label="least loaded", color='skyblue', edgecolor='darkblue', alpha=0.7)
    plt.hist(hash_y, bins=80, density=True, label="hash affinity",color='lightcoral', edgecolor='darkred', alpha=0.7)
    plt.hist(pasch_y, bins=80, density=True, label="pasch",color='lightgreen', edgecolor='forestgreen', alpha=0.7)
    plt.xlabel('Finish time (s)')
    plt.ylabel('Probability Density')
    plt.title("Finish time with Probability Density")
    plt.legend()
    plt.savefig("fin_t.jpg")


def draw_acc_0():
    data_pasch = pd.read_csv(
        "logs\log_main\pasch_finishtime.csv", 
        usecols=['id', 'value']).sort_values(by="id", ascending=True)
    
    data_pasch0 = pd.read_csv(
        "logs\log_main_0cache\pasch_finishtime.csv", 
        usecols=['id', 'value']).sort_values(by="id", ascending=True)
    
    data = []
    for i in range(3000):
        acc = data_pasch0["value"][i] / data_pasch["value"][i]
        data.append(acc)
    
    plt.hist(data, bins=50, color='skyblue', edgecolor='black', density=True,)

    counts, bin_edges = np.histogram(data, bins=30, density=True)
    cdf = np.cumsum(counts) * np.diff(bin_edges)

    plt.plot(bin_edges[1:], cdf, linestyle='-', color='r')

    plt.title('Speedup with Probability Density and CDF')
    plt.xlabel('Value')
    plt.ylabel('Probability Density and CDF')

    plt.savefig("acc0.jpg")


def draw_cache():
    x = [0.5, 1, 5, 10, 50, 100, 500]
    least_y = [0.03707, 0.044913, 0.0837, 0.10028, 0.16029, 0.168328, 0.16368, ]
    hash_y = [0.16668, 0.201626, 0.29203, 0.33462, 0.3988965, 0.443229, 0.4458426]
    pasch_y = [0.15652, 0.184396, 0.254767, 0.29900, 0.35756, 0.3670506, 0.365598]
    plt.plot(x, least_y, label='least loaded')
    plt.plot(x, hash_y, label='hash affinity')
    plt.plot(x, pasch_y, label='pasch')
    plt.xlabel('cache memory (MB)')
    plt.ylabel('hit rate')
    plt.legend()
    plt.savefig("cache_hr.jpg")


def draw_cv_5min():
    data_least = pd.read_csv(
        "logs\log_main_4min\leastloaded_coval.csv", 
        usecols=['abs_time', 'value'])
    least_x = data_least["abs_time"].to_numpy()
    least_y = data_least["value"].to_numpy()

    data_hash = pd.read_csv(
        "logs\log_main_4min\hash_coval.csv", 
        usecols=['abs_time', 'value'])
    hash_x = data_hash["abs_time"].to_numpy()
    hash_y = data_hash["value"].to_numpy()

    data_pasch = pd.read_csv(
        "logs\log_main_4min\pasch_coval.csv", 
        usecols=['abs_time', 'value'])
    pasch_x = data_pasch["abs_time"].to_numpy()
    pasch_y = data_pasch["value"].to_numpy()

    plt.plot(least_x, least_y, label='least loaded')
    plt.plot(hash_x, hash_y, label='hash affinity')
    plt.plot(pasch_x, pasch_y, label='pasch')

    plt.xlabel('time (s)')
    plt.ylabel('coefficient of variation')
    plt.legend()
    plt.savefig("cv4.jpg")


def draw_cv_main():
    data_least = pd.read_csv(
        "logs\log_main\leastloaded_coval.csv", 
        usecols=['abs_time', 'value'])
    least_x = data_least["abs_time"].to_numpy()
    least_y = data_least["value"].to_numpy()

    data_hash = pd.read_csv(
        "logs\log_main\hash_coval.csv", 
        usecols=['abs_time', 'value'])
    hash_x = data_hash["abs_time"].to_numpy()
    hash_y = data_hash["value"].to_numpy()

    data_pasch = pd.read_csv(
        "logs\log_main\pasch_coval.csv", 
        usecols=['abs_time', 'value'])
    pasch_x = data_pasch["abs_time"].to_numpy()
    pasch_y = data_pasch["value"].to_numpy()

    plt.plot(least_x, least_y, label='least loaded')
    plt.plot(hash_x, hash_y, label='hash affinity')
    plt.plot(pasch_x, pasch_y, label='pasch')

    plt.xlabel('time (s)')
    plt.ylabel('coefficient of variation')
    plt.legend()
    plt.savefig("cv.jpg")

# draw_cv_main()

