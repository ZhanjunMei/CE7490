import requests
import random
import csv

def get_pypi_package_data(sample_size=1000):
    # PyPi 的简单 API 列出所有包名称
    index_url = "https://pypi.org/simple/"
    response = requests.get(index_url)
    
    if response.status_code != 200:
        print("Failed to fetch package list.")
        return []

    # 解析包名称
    package_names = [line.split('>')[1].split('<')[0] for line in response.text.splitlines() if 'href' in line]
    # 随机选择样本中的包
    sample_names = random.sample(package_names, sample_size)
    
    # 获取每个包的大小
    package_data = []
    for name in sample_names:
        url = f"https://pypi.org/pypi/{name}/json"
        package_info = requests.get(url).json()
        
        # 解析包大小信息
        sizes = []
        for releases in package_info.get('releases', {}).values():
            for release in releases:
                if 'size' in release:
                    sizes.append(release['size'])
        
        # 如果找到了大小信息，则将平均大小添加到数据中
        if sizes:
            avg_size = sum(sizes) / len(sizes)  # 取平均大小，单位为字节
            package_data.append((name, avg_size / 1024))  # 转换为 KB
        else:
            print(f"No size data found for package: {name}")
    
    return package_data

def save_package_data_to_csv(package_data, filename="pypi_package_data.csv"):
    # 将包数据保存到CSV文件
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Package Name", "Average Size (KB)"])
        for pkg in package_data:
            writer.writerow(pkg)

# 获取 1000 个包的名称和平均大小（单位 KB）
package_data = get_pypi_package_data(1000)

# 保存数据到当前目录的文件
save_package_data_to_csv(package_data)

print(f"Package data saved to 'pypi_package_data.csv'")



#



