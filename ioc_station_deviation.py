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


def inter_rad_month(start, end, df: pd.DataFrame) -> pd.DataFrame:
    '''
        根据 起止时间 对 df 进行线性插值
    '''
    # step1: datetime.datetime -> timestamp
    start_ts = pd.Timestamp(start).timestamp()
    end_ts = pd.Timestamp(end).timestamp()
    con1 = df['ts'] > start_ts
    con2 = df['ts'] < end_ts
    reslt_filter_df = df[con1 & con2]
    reslt_source_df = reslt_filter_df.copy()
    rad_inter_list = reslt_filter_df['rad'].interpolate(method='linear', limit=60, limit_direction='backward')
    reslt_filter_df['rad'] = rad_inter_list
    return [reslt_source_df, reslt_filter_df]


def load_data(dir_path_str: str, station_code: str, start_dt_utc: arrow.Arrow, end_dt_utc: arrow.Arrow,
              area_stamp: str):
    # dir_path_str: str = r'D:\01Proj\OceanSpider\qinglan_2021_source'
    dir_path = pathlib.Path(dir_path_str)
    full_path_str: str = str(dir_path / f'{station_code}_2021_{area_stamp}_all.csv')
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
        {'index': np.arange(0, len(dt_split_minute_indexs)), 'dt': dt_split_minute_indexs})
    # 将匹配的全部分钟df加入时间戳
    dt_split_minute_stamp_df['ts'] = dt_split_minute_stamp_df.apply(lambda x: dt64ToTs(x['dt']), axis=1)
    # 设置 ts 为 index
    dt_split_minute_stamp_df.set_index('ts')
    # 将该年份所有分钟数据按照就近原则进行填充
    reslt_all_df = pd.merge(dt_split_minute_stamp_df, read_df, on='ts', how='outer')
    # -----
    # step2-2: 将分钟数据提取整点时刻并存储至新的文件
    # dt_split_hours_indexs = init_all_year_ser(2021)
    # # 整点时间标记df
    # dt_split_hours_stamp_df = pd.DataFrame(
    #     {'index': np.arange(0, len(dt_split_hours_indexs)), 'dt': dt_split_hours_indexs})
    # dt_split_hours_stamp_df['ts'] = dt_split_hours_stamp_df.apply(lambda x: dt64ToTs(x['dt']), axis=1)
    # dt_split_hours_stamp_df.set_index('ts')
    # # 将 read_df 与 dt_split_hours_stamp_df 合并
    # # TODO: [*] 22-04-27 注意 使用 how='outer' 时会出现 : ValueError: Timezones don't match. 'tzutc()' != 'UTC'
    # # 使用 int timestamp 后 ,merge 出现 : {ValueError}You are trying to merge on int64 and object columns. If you wish to proceed you should use pd.concat
    # # 注意 read_df 中的 ts 是 str | dt_split_hours_stamp_df 中的 ts 是 int64
    # # 切记此处由于是需要将全部分钟数据提取为 整点数据，所以此处切记不要写成reslt_df，这有一个隐藏bug，注意！
    # reslt_df = pd.merge(dt_split_hours_stamp_df, reslt_all_df, on='ts', how='left')
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


def test_inter():
    df1 = pd.DataFrame({'A': [1, np.nan, np.nan, np.nan, 3, np.nan, np.nan, 5, 6, 6, np.nan, 3, 2]})
    series1 = df1['A']
    series_inter = series1.interpolate(method='linear', limit=2, limit_direction='forward')
    df2 = pd.DataFrame({'A': series_inter})
    # get_df_diffent(df1, df2, 'A')
    df_new = convert_inter_df_2_nan(df1, df2, 'A', 2)
    # df2 = pd.DataFrame({'A': [1, 3, 2, 2, 3, 2, 3, 4, 2, 3, 3, 2, np.nan, ]})
    pass


def main():
    '''
        大体思路
        1: 先将原始数据，按照 limit 设置一定长度，进行线性插值，获得 原始 与 插值后 两个 df
        2: 对差值后的 df 与 原始df进行比对处理，将在原始数据中连续nan超过60并进行差值的数据还原为 nan
    :return:
    '''
    list_station: List[str] = [
        # 'rodr',
        # 'ptlu',
        # 'blueb',
        # 'reun2',
        # 'toam2',
        # 'dzao2',
        # 'laru2',
        # 'garc',
        # 'zanz',
        # 'momb',
        # 'lamu',
        # 'sala',
        # 'duqm',
        # ---
        # 'masi',
        # 'ashk',
        # 'suro',
        # 'qura',
        # 'musc',
        # 'wuda',
        # 'maji',
        # 'diba',
        # 'gwda',
        # 'orma',
        # 'kara',
        # 'marm',
        # 'coch',
        # 'mini',
        # 'hani',
        # 'male',
        # 'colo',
        # --
        # 'trin',
        # 'chenn',
        # 'vish',
        # 'chtt',
        # 'sitt',
        # 'hain',
        # 'ptbl',
        # 'nanc',
        # 'saba',
        # 'lank'
        # --
        # 'ms002',
        # 'ms004',
        # 'ms005',
        # 'ms006',
        # 'sibo',
        # 'sebe',
        # 'maju',
        # 'koli',
        # 'pand',
        # 'cili',
        # 'sema',

        # --
        # 'sadp',
        # 'prig',
        # 'sura',
        'beno',
        'lemba',
        'ambon',
        'saum',
        'bitu'
    ]
    list_station: List[str] = [
        'waka',
        'abas',
        'hana',
        'kush',
        'hako',
        'fuka',
        'ofun',
        'ofun',
        'sado',
        'noto',
        'mera',
        'omae',
        'kusm',
        'saig',
        'hmda',
        'tosa',
        'naga',
        'abur'
    ]
    # - 22-08-30
    list_station: List[str] = [
        # TODO:[*] 22-09-26 太平洋沿岸潮位站
        'waka',
        'abas',
        'hana',
        'kush',
        'hako',
        'fuka',
        'ofun',
        'sado',
        'noto',
        'toya',
        'mera',
        'omae',
        'kusm',
        'saig',
        'hmda',
        'tosa',
        'naga',
        'abur',
        'naha',
        # - 22-09-22
        'ishi',
        'busa',
        'curri',
        'subi',
        'mani',
        'luba',
        'lega',
        'davo',
        'quin',
        'vtau',
        'darw',
        'brom',
        'phcp',
        'pmur',
        'cuvie',
        'gpab',
        'hill',
        'espe',
        'thev',
        'porl',
        'barn',
        'sprg',
        'bapj',
        'spjy',
        'tbwc',
        'pkem',
        'gcsb',
        'ross',
        'ferg',
        'lirf',
        # --
        # 'trst',
        # 'groo',
        # 'lomb',
        # 'tare',
        # 'solo',
        # 'lata',
        # 'luga',
        # 'litz',
        # 'vanu',
        # 'lena',
        # 'hien',
        # 'thio',
        # 'ouin',
        # 'numbo',
        # 'ouve',
        # 'lifo',
        # 'vati',
        # 'levu',
        # 'viti',
        # 'upol',
        # 'pago',
        # 'ncpt',
        # 'gbit',
        # 'auct',
        # 'mnkt',
        # 'taut',
        # 'lott',
        # 'gist',
        # 'napt',
        # 'cpit',
        # 'wlgt',
        # 'chst',
        #
        # 'kait',
        # 'sumt',
        # 'jack',
        # 'otat',
        # 'puyt',
        # 'chit',
        # 'quepo',
        # 'psjs',
        # 'psdn',
        # 'laun',
        # 'lalb',
        # 'acaj',
        # 'made',
        # 'sali2',
        # 'huat2',
        # 'ptan2',
        # 'acap2',
        # 'zihu2',
        # 'laza2',
        # 'mnza',
        # 'puert',
        # 'maza2',
        # 'lpaz2',
        # 'tuxp2',
        # 'vera2',
        # 'alva2',
        # 'smag2',
        # 'ccar',
        # 'camt',
        # 'sisa',
        # 'telc',
        # 'imuj2', -- 待重录
        # 'pumo2',
        # 'clst',
        # 'sand',
        # 'lajo2', -- 待重录
        # 'losa',
        # 'pslu', -- 待重录
        # 'mont2',
        # 'alam', -- 待重录
        # part2
        'fpnt',
        'aren2',
        'nspi2',
        'cres',
        'char',
        'sbea2',
        'asto2',
        'wpwa',
        'epme2',
        'cwme2',
        'bame2',
        'ptme',
        'boma',
        'wood2',
        'mony2',
        'bgct2',
        'btny2',
        'acnj2',
        'cmnj2',
        'bamd2',
        'cbmd2',
        'dpnc',
        'benc2',
        'fpga2',
        'fbfl2',
        'fmfl2',
        'cwfl2',
        'apfl2',
        'pnfl2',
        'dial',
        'wlms',
        'psla2',
        'gila',
        'apla2',
        # 'cpla',
        # 'gptx',
        # 'cctx2',
        # 'lime2',
        # 'stcr2',
        # 'lame',
        # 'amal2',
        # 'cule2',
        # 'isab',
        # 'vieq2',
        # 'faja',
        # 'yabu',
        # 'sanj2',
        # 'stdpr',
        # 'aracS',
        # 'gtdpr',
        # 'maya',
        # 'mona2',
    ]
    # TODO:[-] 23-01-11 2021->2022-01-15 大西洋数据
    list_station: List[str] = [
        'setp1',
        'pobe',
        'cabc',
        'prba',
        'pcor',
        'rtas',
        'ceib',
        'cois',
        'sana',
        'limon',
        'bdto',
        'elpo',
        'sapz2',
        'cove',
        'inav',
        'cart',
        'sama',
        'oran',
        'bull',
        'gale',
        'ptsp',
        'scar',
        'pric',
        'calq',
        'chat',
        'stlu4',
        'stlu3',
        'stlu2',
        'stlu',
        'ftfr2',
        'lero',
        'rose',
        'ptmd2',
        'ptpt2',
        'desi',
        'desh',
        'barb',
        'bass',
        'blow',
        'ptca',
        'ptpl',
        'bara',
        'caph',
        'ptpr',
        'slds',
        'jrmi',
        'ptro',
    ]

    # TODO:[-] 23-01-12 爬取 补充部分
    list_station: List[str] = [
        # 'epme2',
        # 'cwme2',
        # 'bame2',
        # 'ptme',
        # 'boma',
        # 'wood2',
        # 'mony2',
        # 'bgct2',
        # 'btny2',
        # 'acnj2',
        # 'cmnj2',
        # 'bamd2',
        # 'cbmd2',
        # 'dpnc',
        # 'benc2',
        # 'fpga2',
        # 'fbfl2',
        # 'fmfl2',
        # 'cwfl2',
        # 'apfl2',
        # 'pnfl2',
        # 'dial',
        # 'wlms',
        # 'psla2',
        # 'gila',
        # 'apla2',
        # 'cpla',
        # 'gptx',
        # 'cctx2',
        # 'tuxp2',
        # 'vera2',
        # 'alva2',
        # 'smag2',
        # 'ccar',
        # 'camt',
        # 'clst',
        # 'sisa',
        # 'telc',
        # 'imuj2',
        # 'pumo2',
        # 'cabc',
        # 'pcor',
        # 'cove',
        # 'calq',
        # 'chat',
        # 'barb',
        # 'lime2',
        # 'stcr2',
        # 'lame',
        # 'amal2',
        # 'cule2',
        # 'isab',
        # 'vieq2',
        # 'faja',
        # 'yabu',
        # 'sanj2',
        # 'stdpr',
        # 'aracS',
        # 'gtdpr',
        # 'maya',
        # 'mona2',
        'caph',
        'slds',
        'jrmi',
        'ptro',
    ]

    # - 23-02-03 重新爬取太平洋沿岸站点
    list_station: List[str] = [
        # 'waka',
        # 'abas',
        # 'hana',
        # 'kush',
        # 'hako',
        # 'fuka',
        # 'ofun',
        # 'sado',
        # 'noto',
        # 'toya',
        # 'mera',
        # 'omae',
        # 'kusm',
        # 'saig',
        # 'hmda',
        # 'tosa',
        # 'naga',
        # 'abur',
        # 'naha',
        # 'ishi',
        # 'busa',
        #
        # 'curri',
        # 'subi',
        # 'mani',
        # 'luba',
        # 'lega',
        # 'davo',
        # 'quin',
        # 'vtau',
        # 'darw',
        # 'brom',
        # 'phcp',
        # 'pmur',
        # 'cuvie',
        # 'gpab',
        # 'hill',
        # 'espe',
        # 'thev',
        # 'porl',
        # 'barn',
        # 'sprg',
        # 'bapj',
        # 'part2',  # - 22 - 11 - 24
        # 'spjy',
        # 'tbwc',
        # 'pkem',
        # 'gcsb',
        # 'ross',
        # 'ferg',
        # 'lirf',
        # 'trst',
        # 'groo',
        # 'lomb',
        # 'tare',
        # 'solo',
        # 'lata',
        # 'luga',
        # 'litz',
        # 'vanu',
        # 'lena',
        # 'hien',
        # 'thio',
        # 'ouin',
        # 'numbo',
        # 'ouve',
        # 'lifo',
        # 'vati',
        # 'levu',
        # 'viti',
        # 'upol',
        # 'pago',
        # 'ncpt',
        # 'gbit',
        # 'auct',
        # 'mnkt',
        # 'taut',
        # 'part3',
        # 'lott',
        # 'gist',
        # 'napt',
        # 'cpit',
        # 'wlgt',
        # 'chst',
        # 'kait',
        # 'sumt',
        # 'jack',
        # 'otat',
        # 'puyt',
        # 'chit',
        # 'quepo',
        # 'psjs',
        # 'psdn',
        # 'laun',
        # 'lalb',
        # 'acaj',
        # 'made', # 出现错误
        # 'sali2',
        # 'huat2', # 出现错误
        # 'ptan2',
        # 'acap2',
        # 'zihu2',
        # 'laza2',
        # 'mnza',
        # 'puert',
        # 'maza2',
        # 'lpaz2',# 出现错误
        # 'part4',  # 以下有需要重新生成的文件
        # 'tuxp2',
        # 'vera2',
        # 'alva2',
        # 'smag2',
        # 'ccar',
        # 'camt',
        # 'sisa',
        # 'telc',
        # 'imuj2',
        # 'pumo2',
        # 'clst',
        # 'sand',
        # 'lajo2',
        # 'losa',
        # 'pslu',
        # 'mont2',
        # 'alam',
        # 'fpnt',
        # 'aren2',
        # 'nspi2',
        # 'cres',
        # 'char',
        # 'sbea2',
        # 'asto2',
        # 'wpwa',
        # 'epme2',
        # 'cwme2',
        # 'bame2',
        # 'ptme',
        # 'boma',
        # 'wood2',
        # 'mony2',
        # 'bgct2',
        # 'btny2',
        # 'acnj2',
        # 'part5',
        # 'cmnj2',
        # 'bamd2',
        # 'cbmd2',
        # 'dpnc',
        # 'benc2',
        # 'fpga2',
        # 'fbfl2',
        # 'fmfl2',
        # 'cwfl2',
        # 'apfl2',
        # 'pnfl2',
        # 'dial',
        # 'wlms',
        # 'psla2',
        # 'gila',
        # 'apla2',
        # 'cpla',
        # 'gptx',
        'cctx2',
        'lime2',
        'stcr2',
        'lame',
        'amal2',
        'cule2',
        'isab',
        'vieq2',
        'faja',
        'yabu',
        'sanj2',
        'stdpr',
        'aracS',
        'gtdpr',
        'maya',
        'mona2',
    ]

    # 印度洋列表
    list_station_indian: List[str] = [
        # 'rodr',
        # 'ptlu',
        # 'blueb',
        # 'reun2',
        'toam2',
        'dzao2',
        'laru2',
        'garc',
        'zanz',
        'momb',
        'lamu',
        'sala',
        'duqm',
        'masi',
        'ashk',
        'suro',
        'qura',
        'musc',
        'wuda',
        'maji',
        'diba',
        'gwda',
        'orma',
        'kara',
        'verav',
        'marm',
        'coch',
        'mini',
        'hani',
        'male',
        'colo',
        'trin',
        'chenn',
        'vish',
        'chtt',
        'sitt',
        'hain',
        'ptbl',
        'nanc',
        'saba',
        'lank',
        'ms002',
        'ms004',
        'ms005',
        'ms006',
        'sibo',
        'sebe',
        'maju',
        'koli',
        'pand',
        'cili',
        'sema',
        'sadp',
        'prig',
        'sura',
        'beno',
        'lemba',
        'ambon',
        'saum',
        'bitu'
    ]

    # 定义 起止 的时间(ts)
    # TODO:[-] 22-05-17 注意使用的是docker容器，注意路径不要用错!

    dir_path_str: str = r'/opt/project/data'
    dir_path_str = r'D:\05data\02spider_ocean_data\2021_indian'
    for station_temp in list_station_indian:

        start_arrow = arrow.Arrow(2021, 1, 1)
        end_arrow = arrow.Arrow(2022, 1, 15)
        start = start_arrow.datetime
        end = end_arrow.datetime
        reslt_all_df = load_data(dir_path_str, station_temp, start_arrow, end_arrow, 'indian')
        if reslt_all_df is not None:
            dfs = inter_rad_month(start, end, reslt_all_df)
            start_filter = datetime.datetime(2021, 1, 1)
            end_filter = datetime.datetime(2022, 1, 15)
            start_filter_pd_ts = pd.Timestamp(start_filter)
            start_filter_ts = start_filter_pd_ts.timestamp()
            end_pd_ts = pd.Timestamp(end_filter)
            end_filter_ts = end_pd_ts.timestamp()
            # step1: 按照 起止 时间进行数据过滤
            # 获取 原始 dfs
            dfs_source = dfs[0]
            con1 = dfs_source['ts'] > start_filter_ts
            con2 = dfs_source['ts'] < end_filter_ts
            dfs_source_res = dfs_source[con1 & con2]
            # 重置索引，此时的索引已有问题
            dfs_source_res.reset_index(inplace=True, drop=True)
            # 获取经过差值处理后的 dfs
            dfs_inter = dfs[1]
            con1 = dfs_inter['ts'] > start_filter_ts
            con2 = dfs_inter['ts'] < end_filter_ts
            dfs_inter_res = dfs_inter[con1 & con2]
            # step2: 对差值处理后的数据进行 nan limit 的过滤
            df_new = convert_inter_df_2_nan(dfs_source_res, dfs_inter_res, 'rad', 60)
            plt.plot(df_new['dt_x'], df_new['rad'])
            plt.show()
            to_store(dir_path_str, f'{station_temp}_2021_inter.csv', df_new)
    pass


if __name__ == '__main__':
    main()
