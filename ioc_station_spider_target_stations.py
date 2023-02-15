import requests
from lxml import etree
from typing import List
import arrow
import datetime
import pandas as pd
import numpy as np
import pathlib


def get_station_rad_df(end_dt_str: str, station_code: str, stamp_list: List[str] = ['rad']) -> pd.DataFrame:
    """
        - 22-04-26
          测试爬取路径:
          http://www.ioc-sealevelmonitoring.org/bgraph.php?code=qing&output=tab&period=7
          根据日期爬取的路径:
          https://www.ioc-sealevelmonitoring.org/bgraph.php?code=shek&output=tab&period=30&endtime=2022-01-01
    :return:
    """
    print(f'准备爬取的end_dt为:{end_dt_str},station:{station_code}')
    url = f'http://www.ioc-sealevelmonitoring.org/bgraph.php?code={station_code}&output=tab&period=30&endtime={end_dt_str}'
    print(f'爬取url:{url}')
    html = requests.get(url)
    # 页面的内容
    html_content = html.text
    '''
        eg:
            <div align=center>
                <table border="1">
                    <th colspan="2">Tide gauge at Qinglan</th>
                    <tr><td>Time (UTC)</td><td class=field>rad(m)</td></tr>
                    <tr><td>2022-04-19 07:04:00</td><td>1
        TODO:[-] 22-09-27 
        出现了 stamp 为 wls(m) 
    '''
    # print(html_content)
    etree_htm = etree.HTML(html_content)
    df: pd.DataFrame = None
    # 定位到 table -> tbody
    # TODO:[-] 22-04-26 注意此处 由于本身页面有 html > body > div 而打印出来的只保留了 div 中的部分，缺失了前面的 html > body
    try:
        content = etree_htm.xpath('/html/body/div/table/tr')
        # print(content)
        table_name_index: int = -1
        list_station_rad: List[dict] = []
        rad_stamp_list: List[str] = stamp_list
        rad_col_index: int = -1

        for index, tr in enumerate(content):
            # print(tr)
            td_list = tr.xpath('td')
            '''
                <td>Time (UTC)</td><td class=field>rad(m)</td>
            '''
            # TODO:[-] 22-05-17 注意此处可能出现多个 td 需要先找到 rad 所在的列的 index
            try:
                if len(td_list) >= 2:
                    table_name = td_list[0].text
                    for td_index, td_name in enumerate(td_list):
                        # TODO:[-] 22-11-18 由于标志符目前看可能不止一种，修改为数组
                        if td_name.text in rad_stamp_list:
                            rad_col_index = td_index
                    if table_name == 'Time (UTC)':
                        table_name_index = index
                        # print(f'找到Time表头所在行index为:{table_name_index}')
                    else:
                        if rad_col_index >= 0:
                            temp_dict = {}
                            # <tr><td>2022-04-19 07:04:00</td><td>1
                            # '2022-04-19 13:31:00'
                            temp_dt_str: str = td_list[0].text
                            # 2022-04-19T13:31:00+00:00
                            # 注意时间为 UTC 时间
                            temp_dt = arrow.get(temp_dt_str).datetime
                            temp_rad_str: str = td_list[rad_col_index].text
                            # TODO:[-] 22-04-27 注意此处的 时间戳转为换 int ，不要使用 float
                            # ts:1548260567 10位
                            temp_dict = {'dt': temp_dt,
                                         'rad': float(np.nan if temp_rad_str.strip() == '' else temp_rad_str),
                                         'ts': arrow.get(temp_dt_str).int_timestamp}
                            list_station_rad.append(temp_dict)
                            # for td in td_list[index:]:
                            #     print(td)
            except Exception as ex:
                print(ex.args)
        # 将 list_station_red -> dataframe

        if len(list_station_rad) > 0:
            df = pd.DataFrame(list_station_rad)
            # 设置 ts 为索引
            df.set_index('ts')
            print('处理成功~')
        else:
            print('处理失败!')
    except Exception as e:
        print(e.args)
    return df
    # pass


def get_target_year_all_dt(year: int) -> List[arrow.Arrow]:
    """
        获取指定年份的全部时间节点
        目前支持的时间间隔有 12h,1d,7d,30d
    :param year:
    :return: {'end_dt','per'} : 结束时间,时间间隔
    """
    # 当前年份的首日
    # year_first_day: datetime.datetime = datetime.datetime(year=year, month=1, day=1)
    # year_last_day: datetime.datetime = year_first_day + datetime.timedelta(days=365) + datetime.timedelta(seconds=-1)
    year_first_day: arrow.Arrow = arrow.get(year, 1, 1)
    year_last_day: arrow.Arrow = year_first_day.shift(years=1).shift(seconds=-1)
    list_dt: List[arrow.Arrow] = []
    temp_dt = year_first_day
    # temp_dt_arrow = arrow.get(temp_dt)
    # list_dt.append(temp_dt_arrow)
    while temp_dt <= year_last_day:
        temp_dt = temp_dt.shift(days=30)
        list_dt.append(temp_dt)
    return list_dt


def get_target_dt_range_all_dt(start: arrow.Arrow, end: arrow.Arrow) -> List[arrow.Arrow]:
    """
        + 22-09-21 根据起止时间获取对应的连续月份集合
    :param start: 起始时间 (只能为 每月的 1 00:00
    :param end: 结束时间 (只能为 每月的 1 00:00)
    :return:
    """
    start_day: arrow.Arrow = start
    end_day: arrow.Arrow = end.shift(seconds=-1)
    list_dt: List[arrow.Arrow] = []
    temp_dt = start_day
    while temp_dt <= end_day:
        temp_dt = temp_dt.shift(days=30)
        list_dt.append(temp_dt)
    return list_dt


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


def split_hour_df(df: pd.DataFrame):
    """
        对 df 只取整点数据
    :param df:
    :return:
    """
    df['ts']


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


def main():
    # step1: 爬取指定海洋站原始数据并存储
    # - 22-05-17 新加入了需要爬取的站点列表
    # 印度洋列表
    list_station_indian: List[str] = [
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
        # 'verav',
        # 'marm',
        # 'coch',
        # 'mini',
        # 'hani',
        # 'male',
        # 'colo',
        # 'trin',
        # 'chenn',
        # 'vish',
        # 'chtt',
        # 'sitt',
        # 'hain',
        # 'ptbl',
        # 'nanc',
        # 'saba',
        # 'lank',
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
        # 'sadp',
        # 'prig',
        # 'sura',
        'beno',
        'lemba',
        'ambon',
        'saum',
        'bitu'
    ]
    # + 22-09-21 TODO[*] 22-09-21 新加入的根据起止时间爬取对应的数据
    start: arrow.Arrow = arrow.Arrow(2021, 1, 1)
    end: arrow.Arrow = arrow.Arrow(2022, 1, 15)
    list_all_split_dt = get_target_dt_range_all_dt(start, end)
    # - 22-08-12 新要爬取的日本沿岸潮位站数据
    list_year_split_dt = get_target_year_all_dt(2021)
    # - 22-08-30 修改为爬取全球
    # list_station: List[str] = [
    #     'waka',
    #     'abas',
    #     'hana',
    #     'kush',
    #     'hako',
    #     'fuka',
    #     'ofun',
    #     'ofun',
    #     'sado',
    #     'noto',
    #     'mera',
    #     'omae',
    #     'kusm',
    #     'saig',
    #     'hmda',
    #     'tosa',
    #     'naga',
    #     'abur'
    # ]

    # TODO:[-] 22-09-21 之前爬取有问题，此处修改为爬取太平洋的数据
    # TODO:[-] 22-11-14 爬取 太平洋 19-20 年数据(优先爬取20年)，暂时不再爬取
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
        # -- 22-11-22
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
        #
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
        # 'kait',
        # 'sumt',
        # - 22-11-24 part3
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
        # 22-11-28 part4
        # 'tuxp2',
        # 'vera2',
        # 'alva2',
        # 'smag2',
        # 'ccar',
        # 'camt',
        # 'sisa',
        # 'telc',
        # 'imuj2', #-- 待重录
        # 'pumo2',
        # 'clst',
        # 'sand',
        # 'lajo2',
        # 'losa',
        # 'pslu',
        # 'mont2',
        # 'alam', #-- 待重录
        # 'fpnt',
        # 'aren2',
        # 'nspi2',
        # 'cres',
        # 22-11-28 part5
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
        # 'cmnj2',
        # 'bamd2',
        # 'cbmd2',
        # 'dpnc',
        # 'benc2',
        # 'fpga2',
        # 'fbfl2',
        # 'fmfl2',  # 总是会中断，暂时跳过
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
        # 'lime2',
        # 'stcr2',  # 出错
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
        'darw',

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
        # 'cwfl2', 无法下载
        # 'apfl2',
        # 'pnfl2',
        # 'dial',
        # 'wlms',
        # 'psla2',
        # 'gila',
        # 'apla2',
        # 'cpla',
        'gptx',
        # 'cctx2',
        # 'tuxp2',
        'vera2',
        'alva2',
        'smag2',
        'ccar',
        'camt',
        'clst',
        'sisa',
        'telc',
        'imuj2',
        'pumo2',
        'cabc',
        'pcor',
        'cove',
        'calq',
        'chat',
        'barb',
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
        'caph',
        'slds',
        'jrmi',
        'ptro',
    ]
    # TODO:[-] 23-02-03 重新爬取太平洋沿岸站点
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
        # 'part4',  # 以下有需要重新生成的文件
        # 'tuxp2',
        # 'vera2',
        # 'alva2',
        # 'smag2',
        # 'ccar',
        # 'camt',
        # 'sisa',
        # 'telc',
        # 'imuj2',  # -- 待重录
        # 'pumo2',
        # 'clst',
        # 'sand',
        # 'lajo2',
        # 'losa',
        # 'pslu',
        # 'mont2',
        # 'alam',  # -- 待重录
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
        # 'fmfl2',  # 总是会中断，暂时跳过
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
        # 'lime2',
        # 'stcr2',  # 出错
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

    spder_stamp_list: List[str] = ['rad(m)', 'wls', 'prs(m)', 'aqu(m)', 'pwl(m)', 'flt(m)', 'bwl(m)', 'pwl(m)',
                                   'wls(m)']
    for station_temp in list_station_indian:
        # TODO:[-] 22-05-18 注意此处的 all_data_df 定义时一定要注意是在每次循环 list_station 时要重新赋值为None，注意！不然会引发一个隐藏的bug
        all_data_df: pd.DataFrame = None
        for dt_temp in list_all_split_dt:
            dt_str: str = dt_temp.format('YYYY-MM-DD')
            df = None
            df = get_station_rad_df(dt_str, station_temp, spder_stamp_list)
            if all_data_df is None:
                all_data_df = df
            else:
                if df is not None:
                    all_data_df = pd.concat([all_data_df, df])
        # 对 df 按照 ts index进行去重
        if all_data_df is not None:
            all_data_df.drop_duplicates(subset=['ts'], keep='first', inplace=True)
            # 存储
            # TODO:[-] 22-08-12 此处暂时修改存储路径的根目录
            root_path: str = r'/opt/project'
            root_path = r'D:\05data\02spider_ocean_data\2021_indian'
            to_store(root_path, f'{station_temp}_2021_indian_all.csv', all_data_df)
            print(f'爬取站点:{station_temp},2021年份数据成功!')
        else:
            print(f'爬取站点:{station_temp},2021年份数据失败!')

    # step2: 数据标准化
    # TODO:[*] 此处在提取整点时刻的数据时出错！,注意
    read_df = pd.read_csv('/opt/project/qinglan_2021_all.csv', parse_dates=['ts'])

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
    dt_split_minute_indexs = init_all_year_ser(2021, '60s')
    # 注意其中的 dt 为 numpy.datetime64 类型！
    dt_split_minute_stamp_df = pd.DataFrame(
        {'index': np.arange(0, len(dt_split_minute_indexs)), 'dt': dt_split_minute_indexs})
    # 将匹配的全部分钟df加入时间戳
    dt_split_minute_stamp_df['ts'] = dt_split_minute_stamp_df.apply(lambda x: dt64ToTs(x['dt']), axis=1)
    # 设置 ts 为 index
    dt_split_minute_stamp_df.set_index('ts')
    # 将该年份所有分钟数据按照就近原则进行填充
    reslt_all_df = pd.merge(dt_split_minute_stamp_df, read_df, on='ts', how='outer')
    reslt_all_df = reslt_all_df.fillna(method='ffill', axis=0)[['dt_x', 'ts', 'rad']]
    to_store('/opt/project', 'qinglan_2021_fill_minutes.csv', reslt_all_df)
    # -----
    # step2-2: 将分钟数据提取整点时刻并存储至新的文件
    dt_split_hours_indexs = init_all_year_ser(2021)
    # 整点时间标记df
    dt_split_hours_stamp_df = pd.DataFrame(
        {'index': np.arange(0, len(dt_split_hours_indexs)), 'dt': dt_split_hours_indexs})

    dt_split_hours_stamp_df['ts'] = dt_split_hours_stamp_df.apply(lambda x: dt64ToTs(x['dt']), axis=1)

    dt_split_hours_stamp_df.set_index('ts')
    # 将 read_df 与 dt_split_hours_stamp_df 合并
    # TODO: [*] 22-04-27 注意 使用 how='outer' 时会出现 : ValueError: Timezones don't match. 'tzutc()' != 'UTC'
    # 使用 int timestamp 后 ,merge 出现 : {ValueError}You are trying to merge on int64 and object columns. If you wish to proceed you should use pd.concat
    # 注意 read_df 中的 ts 是 str | dt_split_hours_stamp_df 中的 ts 是 int64
    # 切记此处由于是需要将全部分钟数据提取为 整点数据，所以此处切记不要写成reslt_df，这有一个隐藏bug，注意！
    reslt_df = pd.merge(dt_split_hours_stamp_df, reslt_all_df, on='ts', how='left')
    # 注意此处不需要再执行填充操作了，因为上面的全分钟df已经包含了可能的全部时间
    # reslt_df = reslt_df.fillna(method='ffill', axis=0)[['dt_x', 'ts', 'rad']]
    # to_store('/opt/project', 'qinglan_2021_fill.csv', reslt_df)
    to_store('/opt/project', 'qinglan_2021_split_hours.csv', reslt_df)

    # step3: .csv -> .txt
    full_path: str = ''
    read_df = pd.read_csv('/opt/project/qinglan_2021_split_hours.csv', parse_dates=['ts'])
    with open('/opt/project/qinglan_2021_split_hours.txt', 'w') as f:
        rows_count = len(read_df)
        hours_num = 0
        for rows_index in range(int(rows_count / 24)):
            temp_row = read_df.iloc[rows_index * 24:(rows_index + 1) * 24]
            temp_rad_series = temp_row['rad']
            temp_rad_list = temp_rad_series.tolist()
            temp_rad_list = [str(x) for x in temp_rad_list]
            temp_rad_str: str = '\t'.join(temp_rad_list) + '\n'
            f.write(temp_rad_str)
            pass
            # if hours_num < 24:
            #
            #     hours_num = hours_num + 1
            # elif hours_num == 24:
            #     hours_num = 0

    pass


if __name__ == '__main__':
    main()
