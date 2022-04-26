import requests
from lxml import etree
from typing import List
import arrow
import pandas as pd
import numpy as np
import pathlib


def get_station_rad_df() -> pd.DataFrame:
    """
        - 22-04-26
          测试爬取路径: http://www.ioc-sealevelmonitoring.org/bgraph.php?code=qing&output=tab&period=7
    :return:
    """
    html = requests.get('http://www.ioc-sealevelmonitoring.org/bgraph.php?code=qing&output=tab&period=7')
    # 页面的内容
    html_content = html.text
    '''
        eg:
            <div align=center>
                <table border="1">
                    <th colspan="2">Tide gauge at Qinglan</th>
                    <tr><td>Time (UTC)</td><td class=field>rad(m)</td></tr>
                    <tr><td>2022-04-19 07:04:00</td><td>1
    '''
    # print(html_content)
    etree_htm = etree.HTML(html_content)
    # 定位到 table -> tbody
    # TODO:[-] 22-04-26 注意此处 由于本身页面有 html > body > div 而打印出来的只保留了 div 中的部分，缺失了前面的 html > body
    content = etree_htm.xpath('/html/body/div/table/tr')
    # print(content)
    table_name_index: int = -1
    list_station_rad: List[dict] = []

    for index, tr in enumerate(content):
        # print(tr)
        td_list = tr.xpath('td')
        '''
            <td>Time (UTC)</td><td class=field>rad(m)</td>
        '''
        if len(td_list) == 2:
            table_name = td_list[0].text
            if table_name == 'Time (UTC)':
                table_name_index = index
                print(f'找到Time表头所在行index为:{table_name_index}')
            else:
                temp_dict = {}
                # <tr><td>2022-04-19 07:04:00</td><td>1
                # '2022-04-19 13:31:00'
                temp_dt_str: str = td_list[0].text
                # 2022-04-19T13:31:00+00:00
                # 注意时间为 UTC 时间
                temp_dt = arrow.get(temp_dt_str).datetime
                temp_rad_str: str = td_list[1].text
                temp_dict = {'ts': temp_dt, 'rad': float(temp_rad_str)}
                list_station_rad.append(temp_dict)
                # for td in td_list[index:]:
                #     print(td)
    # 将 list_station_red -> dataframe
    df = pd.DataFrame(list_station_rad)
    # 设置 ts 为索引
    df.set_index('ts')
    return df
    # pass


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


def main():
    df = get_station_rad_df()
    to_store('/opt/project', 'qinglan_220426.csv', df)
    pass


if __name__ == '__main__':
    main()
