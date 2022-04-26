import requests
from lxml import etree


def get_station_rad():
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
    print(content)
    for tr in content:
        print(tr)
        td_list= tr.xpath('/tr/td')
        '''
            <td>Time (UTC)</td><td class=field>rad(m)</td>
        '''
    pass


def main():
    get_station_rad()
    pass


if __name__ == '__main__':
    main()
