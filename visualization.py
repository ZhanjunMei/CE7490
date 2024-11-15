import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


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

