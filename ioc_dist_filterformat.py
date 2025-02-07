# TODO:[-] 25-02-06 step4: 对于差值后的 csv 文件过滤掉重复的ts行，以及按照起止时间进行筛选并输出

import os
import pathlib
from typing import List

import pandas as pd
from pathlib import Path
import arrow

START_DT = arrow.Arrow(2024, 1, 1, 0, 0)
END_DT = arrow.Arrow(2025, 1, 12, 0, 0)

READ_PATH: str = r'./out_put_inter'
OUT_PUT_PATH: str = r'./out_put_format'


def process_read_files(read_path: str) -> List[str]:
    """
        获取指定目录下的所有文件全路径
    :param read_path:
    :return:
    """
    return [str(Path(read_path) / temp) for temp in os.listdir(read_path)]


def process_filter(files_full_path: List[str], start: arrow.Arrow, end: arrow.Arrow,
                   out_put: str = r'./finial_out_put'):
    """
        对所有文件根据起止时间进行筛选操作
        只保留 [start,end]
    :param files_full_path:
    :param start:
    :param end:
    :return:
    """
    start_ts: int = start.int_timestamp
    """起始时间戳"""
    end_ts: int = end.int_timestamp
    """结束时间戳"""

    filter_col_name: str = 'ts'
    for temp_path in files_full_path:
        try:
            df: pd.DataFrame = pd.read_csv(temp_path)
            required_cols = [filter_col_name]
            if not all(col in df.columns for col in required_cols):
                print(f"文件:{str(temp_path)}缺少必须列，跳出!")
                continue

            filter_df: pd.DataFrame = df[(df[filter_col_name] >= start_ts) & (df[filter_col_name] <= end_ts)]
            """step1:过滤后的df"""
            # 对于过滤后的文件判断指定列是否存才重复的值
            unique_df: pd.DataFrame = filter_df.drop_duplicates(subset=filter_col_name, keep='first')
            """step2:删除 ts 列中重复的值，并保留第一次出现的行 """
            read_file_name: str = pathlib.Path(temp_path).name
            """读取的文件名"""
            output_file: str = str(pathlib.Path(out_put) / read_file_name)
            """输出的文件——全路径"""
            unique_df.to_csv(output_file, index=False)
            print(f'处理文件:{temp_path}时完成.')
        except Exception as e:
            print(f'处理文件:{temp_path}时出错:{e}!')


if __name__ == '__main__':
    read_path: str = READ_PATH
    out_put_path: str = OUT_PUT_PATH
    files: List[str] = process_read_files(read_path)
    process_filter(files, START_DT, END_DT, out_put_path)
    pass
