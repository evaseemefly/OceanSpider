import pandas as pd
import numpy as np

import requests
from lxml import etree
from typing import List
import arrow
import datetime
import pandas as pd
import numpy as np
import pathlib
import matplotlib.pyplot as plt

from common.common import LIST_STATION


def init_all_year_ser(year: int, freq: str = '1H'):
    """
        生成指定年份的整点series
    :param year:
    :return:
    """
    start_utc_ar = arrow.get(year, 1, 1)
    end_utc_ar = start_utc_ar.shift(days=365).shift(seconds=-1)

    dt_index = pd.date_range(start_utc_ar.datetime, end_utc_ar.datetime, freq=freq, name='ts')
    return dt_index


def init_dt_range_index_split(start_dt_utc: arrow.Arrow, end_dt_utc: arrow.Arrow, freq: str = '1H'):
    """
        根据起止时间生成小时间隔的时间索引集合
    :param start_dt_utc:
    :param end_dt_utc:
    :param freq:
    :return:
    """
    dt_index = pd.date_range(start_dt_utc.datetime, end_dt_utc.datetime, freq=freq, name='ts')
    return dt_index
    pass


def to_store(dir: str, file_name: str, df: pd.DataFrame = None):
    """
        将 df -> dir/file_name
    :param dir:
    :param file_name:
    :param df:
    :return:
    """
    dir_path: pathlib.Path = pathlib.Path(dir)
    full_path: pathlib.Path = dir_path / file_name
    if dir_path.exists() is not True:
        dir_path.mkdir(parents=True)
    if df is not None:
        df.to_csv(str(full_path))
    pass


def inter_rad_month(start, end, df: pd.DataFrame, rad_stamp: str = 'rad') -> pd.DataFrame:
    '''
        根据 起止时间 对 df 进行线性插值
    '''
    # step1: datetime.datetime -> timestamp
    start_ts = pd.Timestamp(start).timestamp()
    end_ts = pd.Timestamp(end).timestamp()
    con1 = df['ts'] >= start_ts
    con2 = df['ts'] <= end_ts
    reslt_filter_df = df[con1 & con2]
    reslt_source_df = reslt_filter_df.copy()
    rad_inter_list = reslt_filter_df[rad_stamp].interpolate(method='linear', limit=60, limit_direction='backward')
    reslt_filter_df[rad_stamp] = rad_inter_list
    return [reslt_source_df, reslt_filter_df]


def load_data(dir_path_str: str, station_code: str, start_dt_utc: arrow.Arrow, end_dt_utc: arrow.Arrow,
              area_stamp: str, year_str: str):
    dir_path = pathlib.Path(dir_path_str)
    # TODO:[-] 25-01-17 EG: processed_zldw_2024
    file_name: str = f'processed_{station_code}_{year_str}.csv'
    full_path_str: str = str(dir_path / file_name)
    if pathlib.Path(full_path_str).exists() is False:
        return None

    # step2: 数据标准化
    # TODO:[*] 此处在提取整点时刻的数据时出错！,注意
    read_df = pd.read_csv(full_path_str, parse_dates=['ts'])

    # step2-1: 将爬取的原始数数据按照时间间隔为1min提取为分钟数据，并按照向上填充的方式进行填充
    def str2int64(val: str):
        return np.int64(val)

    def dt64ToTs(val64):
        """
            datetime64[ns] -> timestamp
        :param val:
        :return:
        """
        # 注意此处需要转换为 int 时间戳
        dt: datetime.datetime = pd.Timestamp(val64).to_pydatetime()
        return arrow.get(dt).int_timestamp

    # 将时间戳 str -> int
    read_df['ts'] = read_df.apply(lambda x: str2int64(x['ts']), axis=1)
    read_df.set_index('ts')
    # 生成 分钟级的索引列表
    # dt_split_minute_indexs = init_all_year_ser(2021, '60s')
    # TODO:[*] 22-09-26 此处修改为按照起止时间来获取 dt_index
    dt_split_minute_indexs = init_dt_range_index_split(start_dt_utc, end_dt_utc, '60s')
    # 注意其中的 dt 为 numpy.datetime64 类型！
    dt_split_minute_stamp_df = pd.DataFrame(
        {'index': np.arange(0, len(dt_split_minute_indexs)), 'gmt_realtime': dt_split_minute_indexs})
    # 将匹配的全部分钟df加入时间戳
    dt_split_minute_stamp_df['ts'] = dt_split_minute_stamp_df.apply(lambda x: dt64ToTs(x['gmt_realtime']), axis=1)
    # 设置 ts 为 index
    dt_split_minute_stamp_df.set_index('ts')
    # 将该年份所有分钟数据按照就近原则进行填充
    reslt_all_df = pd.merge(dt_split_minute_stamp_df, read_df, on='ts', how='outer')

    return reslt_all_df


def conduct_nan(df: pd.DataFrame, count_max: int, nan_val: np.float64, col_name: str):
    '''
        处理 df 中的nan
        对于连续 len 的 nan，找到最后的nan index，并将 [index-len,index] 均设置为 nan_val，直到连续的 nan < len 为止
    '''
    df_copy = df.copy()
    if len(df_copy) < count_max:
        return
    # 逐行遍历
    for index in range(len(df_copy)):
        print(f'正在处理{index}行数据')
        temp = df_copy.iloc[index][col_name]
        # 判断是否为 nan 且 是否为超出 df 的行范围
        if pd.isna(temp) and len(df_copy) > index + 1:
            count = 0
            count = count + 1
            next_index = 0
            last_index = 0
            while True:
                next_index = next_index + 1
                # 总的数据量 > 当前 index + 增量
                if len(df_copy) > index + next_index:
                    # TODO:[*] 若最后一个是 nan
                    # IndexError: single positional indexer is out-of-bounds
                    next_temp = df_copy.iloc[index + next_index][col_name]
                    if pd.isna(next_temp):
                        count = count + 1
                        # last_index = next_index
                        # if count > count_max:
                        #     #  注意此处应该是将所有的 在 [index,last_index] 均设置为 nan_val
                        #     df.iloc[index: next_index][col_name] = nan_val
                        # else:
                        #     pass
                    else:
                        break
                else:
                    break
            if count > count_max:
                #  注意此处应该是将所有的 在 [index,last_index] 均设置为 nan_val
                # TODO:[*] - 22-05-09 注意此处 range(a,b) b一定要大于a!
                for df_index in range(index, index + count):
                    if len(df_copy) >= df_index:
                        try:
                            # TODO:[-] 22-05-10 切记不要通过 iloc[index][column_name] 的方式进行赋值!
                            df_copy.loc[df_index, col_name] = nan_val
                        except IndexError as e:
                            msg = f'处理到{df_index}时出错,msg:{e}'
                            print(msg)

    return df_copy


def get_df_diffent(df1: pd.DataFrame, df2: pd.DataFrame, col_name: str) -> pd.DataFrame:
    '''
        找到 df1 与 df2 中不同的部分并以 pd.DataFrame 返回
    :param df1:
    :param df2:
    :return:
    '''
    list_serise = []
    if len(df1) == len(df2):

        for index in range(len(df1) - 1):
            if df1.iloc[index][col_name] != df2.iloc[index][col_name]:
                list_serise.append(df2.iloc[index])

    df: pd.DataFrame = pd.concat(list_serise, axis=0)
    return df


def convert_inter_df_2_nan(df1: pd.DataFrame, df2: pd.DataFrame, col_name: str, limit: int) -> pd.DataFrame:
    '''
        判断 df1 中连续的 nan 是否超过 limit ,若超过则重新赋值为 nan
    :param df1: 原始df
    :param df2: 插值后的df
    :param col_name:
    :param limit:
    :return:
    '''
    # 差值后的 df
    df_new: pd.DataFrame = df2.copy()
    if len(df1) == len(df_new):
        for index in range(len(df1) - 1):
            # 找到原始数据与差值后的数据不同的位置
            if df1.iloc[index][col_name] != df_new.iloc[index][col_name]:
                # 原始位置当前位置是否为 nan
                if pd.isna(df1.iloc[index][col_name]):
                    # 若为空开始计数，向后若连续出现 nan 则 count+1
                    count = 0
                    index_same_nan = 0  # 当前位置 index 之后连续出现 nan 的 index
                    first_nan_index: int = index  # 当前 index 第一次出现 nan 的位置
                    while True:
                        index_same_nan = index_same_nan + 1
                        # 若为空开始计数，向后若连续出现 nan 则 count+1
                        if len(df1) <= index + index_same_nan and pd.isna(df1.iloc[index + index_same_nan][col_name]):
                            count = count + 1
                        else:
                            break
                    if count >= limit:
                        for num in range(count):
                            # KeyboardInterrupt
                            df_new.loc[first_nan_index + num, col_name] = np.nan

        pass
    return df_new


def get_all_filenames(dir):
    """
    获取指定目录下的所有文件名。

    :param directory: 文件所在的目录路径
    :return: 文件名列表
    """
    try:
        # 使用 os.listdir 获取目录下的所有文件和文件夹
        filenames = os.listdir(dir)

        # 过滤掉目录，只保留文件
        files = [f for f in filenames if os.path.isfile(os.path.join(dir, f))]

        # 打印文件名
        print("目录下的所有文件名：")
        for file in files:
            print(file)

        return files
    except Exception as e:
        print(f"获取文件名时发生错误: {e}")
        return []


def main():
    '''
        大体思路
        1: 先将原始数据，按照 limit 设置一定长度，进行线性插值，获得 原始 与 插值后 两个 df
        2: 对差值后的 df 与 原始df进行比对处理，将在原始数据中连续nan超过60并进行差值的数据还原为 nan
    :return:
    '''

    # TODO:[-] 25-01-16 根据文件名获取所有的站点名

    read_path: str = r'./out_put_data'
    # files: List[str] = get_all_filenames(read_path)
    # eg: processed_abas_2024

    # station_codes: List[str] = [temp.split('_')[1] for temp in files]
    station_codes: List[str] = LIST_STATION

    for station_temp in station_codes:
        # TODO:[-] 25-02-05 获取 2024-01-01 00:00 -> 25-01-15 的数据并差值
        start_arrow = arrow.Arrow(2024, 1, 1, 00, 00)
        end_arrow = arrow.Arrow(2025, 1, 15, 23, 59)
        start = start_arrow.datetime
        end = end_arrow.datetime
        reslt_all_df = load_data(read_path, station_temp, start_arrow, end_arrow, 'all', '2024')
        if reslt_all_df is not None:
            # TODO:[-] 25-02-05 reslt_all_df 包含 00:00 ，转换后的 dfs 起始时间为00:01
            dfs = inter_rad_month(start, end, reslt_all_df, 'surge')
            start_filter = datetime.datetime(2024, 1, 1, 0, 0)
            end_filter = datetime.datetime(2025, 1, 15, 23, 59)
            start_filter_pd_ts = pd.Timestamp(start_filter)
            start_filter_ts = start_filter_pd_ts.timestamp()
            end_pd_ts = pd.Timestamp(end_filter)
            end_filter_ts = end_pd_ts.timestamp()
            # step1: 按照 起止 时间进行数据过滤
            # 获取 原始 dfs
            dfs_source = dfs[0]
            con1 = dfs_source['ts'] >= start_filter_ts
            con2 = dfs_source['ts'] <= end_filter_ts
            dfs_source_res = dfs_source[con1 & con2]
            # 重置索引，此时的索引已有问题
            dfs_source_res.reset_index(inplace=True, drop=True)
            # 获取经过差值处理后的 dfs
            dfs_inter = dfs[1]
            con1 = dfs_inter['ts'] >= start_filter_ts
            con2 = dfs_inter['ts'] <= end_filter_ts
            dfs_inter_res = dfs_inter[con1 & con2]
            # step2: 对差值处理后的数据进行 nan limit 的过滤
            df_new = convert_inter_df_2_nan(dfs_source_res, dfs_inter_res, 'surge', 60)
            # plt.plot(df_new['dt_x'], df_new['rad'])
            # plt.show()
            to_store(read_path, f'{station_temp}_2024_inter.csv', df_new)
            print(f'差值:{station_temp}站点运算结束')
    pass


if __name__ == '__main__':
    main()
