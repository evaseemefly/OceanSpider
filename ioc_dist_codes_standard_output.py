import os
import pandas as pd


def process_standard_csv_files(read_path: str, out_put: str):
    """
    读取指定目录下的所有 CSV 文件，处理并保存结果。

    :param directory: CSV 文件所在目录路径
    """
    # 遍历目录下的所有文件
    for file_name in os.listdir(read_path):
        if file_name.endswith('.csv'):  # 只处理 CSV 文件
            file_path = os.path.join(read_path, file_name)
            print(f"正在处理文件: {file_name}")

            # 读取 CSV 文件
            try:
                df = pd.read_csv(file_path)

                # 检查是否包含所需的列
                required_columns = ['station_code', 'surge', 'ts', 'gmt_realtime']
                if not all(col in df.columns for col in required_columns):
                    print(f"文件 {file_name} 缺少必要的列，跳过...")
                    continue

                # 转换 ts 列为整数时间戳，gmt_realtime 列为 datetime 格式
                # TODO:[-] 25-01-17 去掉了 ts 的转换，以 gmt_realtime 为准
                # df['ts'] = pd.to_datetime(df['ts'], unit='s')  # 假设 ts 是秒级时间戳
                df['gmt_realtime'] = pd.to_datetime(df['gmt_realtime'])  # 转换为 datetime

                # 筛选条件：时间为每分钟的 00 秒
                # filtered_df = df[(df['ts'].dt.second == 0) & (df['gmt_realtime'].dt.second == 0)]
                filtered_df = df[(df['gmt_realtime'].dt.second == 0)]

                # 保存处理后的数据
                output_file = os.path.join(out_put, f"processed_{file_name}")
                filtered_df.to_csv(output_file, index=False)
                print(f"文件 {file_name} 处理完成，结果保存为 {output_file}")

            except Exception as e:
                print(f"处理文件 {file_name} 时发生错误: {e}")


if __name__ == '__main__':
    read_path: str = r'./data'
    out_put_path: str = r'./out_put_data'
    process_standard_csv_files(read_path, out_put_path)
