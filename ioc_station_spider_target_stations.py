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
    start: arrow.Arrow = arrow.Arrow(2023, 1, 1)
    end: arrow.Arrow = arrow.Arrow(2024, 1, 15)
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

    # TODO:[-] 23-02-17 北海站点
    list_station_northsea: List[str] = [
        # 'malo',
        # 'treg',
        # 'kungr',
        # 'smog',
        # 'brof',
        # 'mard',
        # 'sten',
        # 'udde',
        # 'vin2',
        # 'gokr',
        # 'goto',
        # 'tang',
        # 'onsa',
        # 'ring',
        # 'varb',
        # 'falk',
        # 'halm',
        # 'vikew',
        # 'hels',
        # 'bars',
        # 'maha',
        # 'klag',
        # 'sass',
        # 'warn',
        # 'horn',
        # 'helg',
        # 'cuxh',
        # 'bork',
        # 'delf',
        # 'ters',
        # 'harl',
        # 'denh',
        # 'sche',
        # 'hoek',
        # 'euro',
        # 'vlrn',
        # 'vlis',
        # 'trnz',
        # 'oste',
        # 'dunk2',
        # 'cala2',
        # 'boul2',
        # 'diep2',
        # 'eha2',
        # 'ouis2',
        # 'cher2',
        # 'diel2',
        # 'wick',
        # 'abed',
        # 'leit',
        # 'nshi',
        # 'whit',
        # 'immi',
        # 'crom',
        # 'lowe',
        # 'harw',
        # 'shee',
        # 'hbay',
        # 'dove',
        # 'hasp',
        # 'nhav',
        # 'ptmt',
        # 'bour',
        # 'weym',
        # 'wbhb',
        # 'plym',
        # 'ilfa',
        # 'barm',
        # 'work',
        # 'mill',
        # 23-02-26 需要补充的之前没有的站点
        # 'trmz',
        # 'dunk',
        # 'eha2',
        # 'wick',
        # 'abed',
        # 'leit',
        # 'nshi',
        # 'whit',
        # 'immi',
        # 'crom',
        # 'lowe',
        # 'harw',
        # 'shee',
        # 'hbay',
        # 'dove',
        # 'hasp',
        # 'nhav',
        # 'ptmt',
        'wbhb',

    ]

    # TODO:[-] 24-01-02 重新爬取2023年的全部站点数据
    list_station_part1: List[str] = [
        # 'abas',
        # 'abed',
        # 'abur',
        # 'acaj',
        # 'acap2',
        # 'acnj2',
        # 'acor1',
        # 'adak2',
        # 'agua',
        # 'aigi',
        # 'ajac',
        # 'ajac2',
        # 'alac2',
        # 'alak',
        # 'alak2',
        # 'alam',
        # 'alawa',
        # 'alcu',
        # 'ales',
        # 'alex1',
        # 'alex2',# 暂时跳过
        # 'alex3',
        # 'alge',
        # 'alme',
        # 'alme2',
        # 'alva',
        # 'alva2',
        # 'amal',
        # 'amal2',
        # 'amas', # 暂时跳过
        # 'AN15',
        # 'anch2',
        # 'ancu',
        # 'ancu2',
        # 'ande',
        # 'anta',# 暂时跳过
        # 'anto',
        # 'anto2',
        # 'apfl',
        # 'apfl2',
        # 'apla2',
        # 'aracS',
        # 'arca',
        # 'arca2',
        # 'aren',
        # 'aren2',# 暂时跳过
        # 'aric',
        # 'aric2',
        # 'arko',
        # 'arre',
        # 'arsu',
        # 'ascen',
        # 'asto',
        # 'asto2',
        # 'auasi',
        # 'auct',
        # 'audi',
        # 'audi2',#错误
        # 'avon',
        # 'AZ42',
        # 'BA05',
        # 'ball',
        # 'balt',
        # 'bamd',
        # 'bame',
        # 'bame2',
        # 'bang',
        # 'bapj',
        # 'bara',
        # 'barc',
        # 'barc2',
        # 'barn',
        # 'bars',
        # 'bbbc',
        # 'bdto',
        # 'bele',
        # 'benc',
        # 'benc2',
        # 'beno',
        # 'berg',
        # 'bgct',
        # 'bgct2',
        # 'bil3',
        # 'bitu',
        # 'blow',
        # 'blueb',
        # 'bmda',
        # 'bmsa',
        # 'bmsa2',
        # 'bmsg',
        # 'bmso',
        # 'bodo',
        # 'bodri',
        # 'boma',
        # 'bon2',
        # 'bona',
        # 'bork',
        # 'bouc',  # 错误
        # 'boul',
        # 'boul2',
        # 'bour',
        # 'boye',
        # 'boye2',
        # 'bozc',
        # 'bozy',
        # 'bres',# 错误
        # 'brof',
        # 'brom',
        # 'brpt',
        # 'brsk',
        # 'brur',
        # 'btny',
        # 'btny2',
        # 'bull',  # 暂时跳过
        # 'busa',
        # 'buve',
        # 'buve2',
        # 'CA02',
        # 'cabc',
        # 'cadi',
        # 'cagb',
        # 'cagt',
        # 'cala',
        # 'cala2',
        # 'cald',
        # 'cald2',
        # 'call',
        # 'caph',
        # 'carb',# 错误
        # 'cart',
        # 'casc',
        # 'cast',
        # 'cbmd',
        # 'cbmd2',
        # 'ccar',# 错误
        # 'cent',# 错误
        # 'cent2',
        # 'ceut',
        # 'CF06',
        # 'chala',
        # 'char',
        # 'char2',
        # 'chat',
        # 'chenn',
        # 'cher',
        # 'cher2',# 错误
        # 'chia',# 错误
        # 'chij',
        # 'chimb',
        # 'chit',
        # 'chnr',
        # 'chnr2',
        # 'chrp',
        # 'chrp2',
        # 'chrs',
        # 'chst',
        # 'chtt',
        # 'chuu',
        # 'CI20',
        # 'ciut',
        # 'clst',
        # 'cmet',
        # 'cmet3',
        # 'cmnj',
        # 'cmnj2',
        # 'cocb',
        # 'coch',
        # 'cocos',
        # 'coli',
        # 'coli2',
        # 'colo',  # 暂时跳过
        # 'conc',
        # 'conc2',
        # 'cons2',
        # 'const',
        # 'coqu',
        # 'coqu2',
        # 'cor2',
        # 'cord',# 暂时跳过
        # 'cord2',
        # 'corf',
        # 'corn',
        # 'corr',
        # 'corr2',
        # 'coru',  # 暂时跳过
        # 'cove',
        # 'cpit',
        # 'CR08',
        # 'crbc',
        # 'cres',
        # 'cres2',  # 暂时跳过
        # 'crnl',# 暂时跳过
        # 'crnl2',
        # 'crom',
        # 'csta2',
        # 'cstr',
        # 'cstr2',
        # 'CT03',
        # 'cule',  # 暂时跳过
        # 'cule2',
        # 'cuvie',
        # 'cuxh',# 暂时跳过
        # 'cwfl',
        # 'cwfl2',
        # 'cwme',
        # 'cwme2',
        # 'dakar',
        # 'darw',
        # 'davo',#错误
        # 'deke',
        # 'delf',
        # 'denh',
        # 'dese',
        # 'desh',#错误
        # 'dial',
        # 'diel',
        # 'diel2',
        # 'diep',
        # 'diep2',  # 暂时跳过
        # 'dlpr',
        # 'dove',
        # 'dumo',
        # 'dunk',
        # 'dunk2',
        # 'duqm',
        # 'dutc2',
        # 'dzao',
        # 'dzao2',  # 暂时跳过
        # 'east',
        # 'east2',
        # 'efjm',
        # 'elak',
        # 'elak2',
        # 'elja1',
        # 'elpo',
        # 'epme',
        # 'epme2',
        # 'erdek',
        # 'esme',
        # 'espe',
        # 'euro',
        # 'exmt',
        # 'faja',
        # 'falk',
        # 'fbfl',
        # 'fbfl2',
        # 'fer1',  # 暂时跳过
        # 'fer2',
        # 'ferg',
        # 'ffcj',
        # 'figu',
        # 'figu2',
        # 'fish',
        # 'fmfl',
        # 'fmfl2',
        # 'fong',
        # 'form',
        # 'fors',
        # 'fort',
        # 'fosm2',
        # 'fpga2',
        # 'fpnt2',
        # 'fren',
        # 'frot',
        # 'ftfr',
        # 'ftfr2',
        # 'fue2',
        # 'fuer2',
        # 'fuka',
        # 'furu',
        # 'futu',
        # 'GA37',
        # 'gale',
        # 'gamb',
        # 'gand',
        # 'ganm',
        # 'garc',
        # 'gbit',
        # 'gcsb',  # 暂时跳过
        # 'GE25',
        # 'GI20',
        # 'gibr3',
        # 'gij2',  # 暂时跳过
        # 'gist',  # 暂时跳过
        # 'goag',
        # 'goer',
        # 'gohi',
        # 'gokr',
        # 'goti',
        # 'gptx',
        # 'greg',
        # 'greg2',
        # 'groo',
        # 'gtdpr',
        # 'gyer',
        # 'hako',
        # 'halei',
        # 'halm',
        # 'hamm',
        # 'hana',
        # 'harl',
        # 'hars',
        #
        # 'harw',
        # 'hasp',
        # 'heeia',
        # 'heim',
        # 'helg',
        # 'helgz',
        # 'hels',
        # 'herb',
        # 'herb2',
        # 'heys',
        # 'hie2',
        # 'hien',
        # 'hilo2',
        # 'hink',
        # 'hirt',
        # 'hiva',#错误
        # 'hmda',
        # 'hoek',
        # 'holm',#错误
        # 'holy2',
        # 'honn',
        # 'hono2',
        # 'horn',
        # 'hrak',
        # 'huahi',#错误
        # 'huas2',#错误
        # 'huas3',
        # 'huat',#错误
        # 'huat2',
        # 'huel',
        # 'iaix',
        # 'ibiz',
        # 'iclr',
        # 'iera',
        # 'igne',
        # 'iler',
        # 'ilfa',
        # 'IM01',
        # 'imbi',
        # 'imbt',
        # 'immi',
        # 'imuj',
        # 'imuj2',
        # 'iqui',
        # 'iqui2',
        # 'isab',
        # 'isfu',
        # 'ishig',
        # 'ista',
        # 'IT45',
        # 'itea',
        # 'jack',
        # 'jers',
        # 'john',
        # 'juac',
        # 'juan',
        # 'juan2',  # 暂时跳过
        # 'jute',
        # 'kabe',
        # 'kahu',
        # 'kahu2',
        # 'kait',
        # 'kaka',
        # 'kalit',
        # 'kalm',#错误
        # 'kant',
        # 'kapi',
        # 'kaps',
        # 'kara',
        # 'karl',
        # 'kawa',#错误
        # 'kawa2',
        # 'keehi',
        # 'kepo1',#错误
        # 'kerg',
        # 'kerg2',
        # 'kiel',#错误
        # 'kinl',
        # 'kisr',
        # 'kjni',#错误
        # 'klag',
        # 'kodi2',
        # 'koli',
        # 'koli2',
        # 'koro',
        # 'kris',
        # 'kung',
        # 'kungr',
        # 'kush',
        # 'kusm',
        # 'kwaj2',
        # 'LA23',
        # 'LA38',
        # 'lago',
        # 'lajo',
        # 'lajo2',
        # 'lalb',
        # 'lali',
        # 'lame',
        # 'lamu',
        # 'land',
        # 'lang',
        # 'lapa',  # 有问题
        # 'larn2',
        # 'larp',  # 有问题
        # 'larp2',
        # 'laru2',
        # 'lasp',
        # 'laun',
        # 'laza',
        # 'laza2',
        # 'lebu',  # 有问题
        # 'lebu2',
        # 'leco',
        # 'leco2',
        # 'lecy',
        # 'lega',
        # 'leha',
        # 'leha2',
        # 'leir',
        # 'leit',
        # 'lemba',
        # 'leme',
        # 'lena',
        # 'lero',  # 有问题
        # 'lerw2',
        # 'leso',
        # 'leso2',
        # 'levu',
        # 'lgos',
        # 'LI11',
        # 'lifo',
        # 'lime',
        # 'lime2',
        # 'limon',
        # 'lirf',  # 有问题
        # 'litz',
        # 'live',
        # 'ljus',
        # 'lmma',
        # 'lobos',
        # 'lomb',
        # 'lott',
        # 'lowe',
        # 'lpaz2',
        # 'lpbc',
        # 'luba',
        # 'lund',
        # 'made',
        # 'madry',
        # 'maer',
        # 'magi',
        # 'magi2',
        # 'maha',
        # 'maho',
        # 'maji',
        # 'make',
        # 'mal3',  # 有问题
        # 'mala',
        # 'male',
        # 'malh',
        # 'mali',
        # 'malo',
        # 'mang',
        # 'mani',
        # 'mant',
        # 'mard',
        # 'mare',
        # 'mari',
        # 'marm',
        # 'mars',
        # 'marsh',
        # 'masi',  # 有问题
        # 'masi2',
        # 'mata',
        # 'maya',
        # 'MC41',  # 有问题
        # 'ME13',
        # 'meji',
        # 'meji2',
        # 'meli',
        # 'ment',  # 有问题
        # 'mera',
        # 'mhav',
        # 'midx',
        # 'mill',
        # 'mimz',
        # 'mini',
        # 'mins',
        # 'miqu',
        # 'mnkt',
        # 'mnza',
        # 'momb',
        # 'mona',
        # 'mona2',
        # 'monc',
        # 'monc2',
        # 'mont',
    ]

    list_station: List[str] = [
        # 'mony2',
        # 'motr',
        # 'mrms',# 错误
        # 'mumb',
        # 'murc2',
        # 'musc',# 错误
        # 'musc3',
        # 'NA23',  # 有错误
        # 'naga',  # 有错误
        # 'naha',
        # 'napt',
        # 'narv',
        # 'nauu',
        # 'nawi',
        # 'nbrt',
        # 'ncpt',
        # 'newl2',
        # 'nhav',
        # 'nhte',
        # 'nice',
        # 'nice2',
        # 'nkfa',
        # 'noct',
        # 'nome',
        # 'noto',  # 有错误
        # 'npor',
        # 'nshi',
        # 'nspi',
        # 'nspi2',
        # 'ntue',  # 有错误
        # 'ntue2',
        # 'nuk1',
        # 'nuku',  # 有错误
        # 'numbo',
        # 'nyal',
        # 'nyna',
        # 'ofuas',
        # 'ofun',
        # 'ohig',
        # 'ohig3',  # 有错误
        # 'olan',
        # 'omae',
        # 'onsa',
        # 'OR24',
        # 'osca',
        # 'oska',
        # 'oslo',
        # 'oste',
        # 'OT15',# 错误
        # 'otat',
        # 'ouin',# 错误
        # 'ouis',# 错误
        # 'ouis2',
        # 'ouve',
        # 'oxel',
        # 'PA07',
        # 'paak',
        # 'pada',
        # 'pagi',
        # 'pagi2',
        # 'pago',
        # 'paita',
        # 'palb',
        # 'pale',
        # 'palm',
        # 'palm1',
        # 'pano',
        # 'pape',
        # 'pape2',
        # 'papo',
        # 'papo2',
        # 'pata', # 错误
        # 'pata2',
        # 'pbol',
        # 'pcha',
        # 'pcha2',
        # 'pdas',
        # 'PE09',
        # 'pedn',# 错误
        # 'pedn2',
        # 'penr',
        # 'phbc',# 错误
        # 'pich',
        # 'pich2',
        # 'pisa',
        # 'pisa2',
        # 'pisco',
        # 'pkem',
        # 'PL14',
        # 'plat2',
        # 'plmy',  # 错误
        # 'plym',
        # 'pmel',  # 错误
        # 'pmel2',
        # 'pmon',
        # 'pmon2',
        # 'pmur',
        # 'pnat',
        # 'pnat2',
        # 'pnfl2',#错误
        # 'PO40',
        # 'porl',
        # 'porp',
        # 'port',
        # 'port2',
        # 'prat3',
        # 'prba',  # 错误
        # 'prec',
        # 'prev',
        # 'pric',  # 错误
        # 'prus',
        # 'psla2',
        # 'pslu',
        # 'pslu2',  # 错误
        # 'PT17',
        # 'ptal',
        # 'ptal2',# 错误
        # 'ptan',
        # 'ptan2',# 错误
        # 'ptar',
        # 'ptar2',
        # 'ptbc',# 错误
        # 'ptbl',
        # 'ptca',
        # 'ptch',
        # 'ptfe',
        # 'ptfe2',#错误
        # 'ptln',
        # 'ptln2',
        # 'ptlu',
        # 'ptmd2',
        # 'ptme',#错误
        # 'ptmt',
        # 'ptpl',  # 错误
        # 'ptpt',
        # 'ptsc',
        # 'ptsp',
        # 'ptve',
        # 'ptve2',
        # 'puer2',
        # 'puert',  # 错误
        # 'pumo',
        # 'pumo2',
        # 'puna',
        # 'puyt',
        # 'pwil',
        # 'pwil2',
        # 'qaqo',
        # 'qcbc',
        # 'qing',
        # 'qtro',
        # 'qtro2',
        # 'quar',
        # 'quel',
        # 'quel2',
        # 'quepo',
        # 'quir',
        # 'quir2',
        # 'qura',
        # 'RA10',
        # 'rangi',
        # 'raro',
        # 'rata',
        # 'rbct',
        # 'RC09',
        # 'reun',
        # 'reun2',
        # 'rfrt',
        # 'ring',
        # 'rodr',# 错误
        # 'rorv',
        # 'rosc',
        # 'rosc2',
        # 'rose',
        # 'ross',  # 错误
        # 'rothe',
        # 'rous',
        # 'rous2',
        # 'SA16',
        # 'saba',
        # 'sado',
        # 'sadp',
        # 'sagu',
        # 'saig',
        # 'sain',
        # 'sain2',
        # 'saip',# 错误
        # 'sala',
        # 'salav',
        # 'sali',
        # 'sali2',  # 错误
        # 'salv',
        # 'sama',
        # 'sama2',
        # 'san2',
        # 'sana',
        # 'sand',
        # 'Sandn',
        # 'sanf',
        # 'sanf3',
        # 'sanj2',  # 错误
        # 'sano',
        # 'sano2',
        # 'sant',
        # 'sass',
        # 'SB36',
        # 'SC43',
        # 'sche',
        # 'scoa',
        # 'scoa2',
        # 'scor',
        # 'sdom',
        # 'sdpr',
        # 'sdpt2',
        # 'sema',
        # 'sete',
        # 'sete2',
        # 'setp1',
        # 'sev2',
        # 'sewa2',
        # 'shee',
        # 'shek',
        # 'shen',
        # 'sile',
        # 'simp',
        # 'simr',
        # 'sino',  # 错误
        # 'sire',
        # 'sisa',
        # 'sitk2',
        # 'sitt',
        # 'sjuan',
        # 'skagk',
        # 'skan',
        # 'slds',
        # 'slor',
        # 'smag',
        # 'smag2',  # 错误
        # 'smar',
        # 'smar2',
        # 'smog',  # 错误
        # 'smth',
        # 'sobr',
        # 'sola',
        # 'sole',  # 错误
        # 'sole2',
        # 'solo',
        # 'spik',
        # 'spjy',# 错误
        # 'spmi',
        # 'sprg',
        # 'sscr',
        # 'stan',
        # 'stari',
        # 'stav',
        # 'stcr',# 错误
        'stcr2',
        'stdpr',
        'sten',
        'sthl2',
        'sthm',
        'stlu',
        'stlu2',
        'stlu3',
        'stlu4',
        'stma',
        'stma2',
        'stmr',
        'stor',
        'stqy',
        'stqy2',
        'stro',
        'subi',
        'sura',
        'swpr',
        'syow',
        'TA18',
        'tala',
        'talc',
        'talc2',
        'talt',
        'talt2',
        'tang',
        'tara',
        'tara1',
        'tare',
        'tari',
        'tarr',
        'tasu',
        'tauas',
        'taut',
        'taza',
        'tbwc',
        'tcsb',
        'tene',
        'ters',
        'tfbc',
        'thev',
        'thio',
        'Thio2',
        'thul',
        'tn031',
        'tobe',
        'toco2',
        'toco3',
        'tosa',
        'toul',
        'toul2',
        'toya',
        'TR22',
        'trab',
        'treg',
        'trin',
        'trnz',
        'trom',
        'tron',
        'trst',
        'tubua',
        'tudy',
        'tumc',
        'turb',
        'tuxp',
        'tuxp2',
        'UAPO',
        'udde',
        'ulla',
        'uper',
        'upol',
        'ushu',
        'vair',
        'vald2',
        'vale',
        'valp',
        'valp2',
        'vanu',
        'varb',
        'vard',
        'VE19',
        'vera',
        'verav',
        'vern',
        'vhbc',
        'VI12',
        'vibc',
        'vieq',
        'vieq2',
        'vig2',
        'vike',
        'vikew',
        'vil2',
        'vin2',
        'visb',
        'vish',
        'viti',
        'vlis',
        'vlrn',
        'vsfb',
        'vvik',
        'waian',
        'waka',  # 错误
        'wake2',
        'wall',
        'warn',
        'weym',
        'whbc',
        'whit',
        'wick',
        'wlgt',  # 错误
        'wood2',
        'work',
        'wpwa',
        'wuda',  # 错误
        'xmas',
        'yaku',
        'yaku2',  # 错误
        'yalo',
        'yapi',
        'ysta',
        'zanz',
        'zihu',
        'zihu2',
        'zkth',

    ]

    # TODO:[-] 24-10-14 补录的部分全球站点
    list_station: List[str] = [
        'deke',
        'upol',
        'nkfa',
        'raro',
        'kawa',
        'vhbc',
        'qtro2',
        'gyer',
    ]

    spder_stamp_list: List[str] = ['rad(m)', 'wls', 'prs(m)', 'aqu(m)', 'pwl(m)', 'flt(m)', 'bwl(m)', 'pwl(m)',
                                   'wls(m)', 'bub(m)', 'stp(m)']
    for station_temp in list_station:
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
            root_path = r'D:\05data\02spider_ocean_data\2023_all_global_24'
            to_store(root_path, f'{station_temp}_2023_all.csv', all_data_df)
            print(f'爬取站点:{station_temp},2023年份数据成功!')
        else:
            print(f'爬取站点:{station_temp},2023年份数据失败!')

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
