# 25-01-16 从数据库中导出爬取站点数据

import pathlib

import pandas as pd
from sqlalchemy import create_engine

from _privacy import DB_CONFIG
from common.common import LIST_STATION

db_url = f"mysql+pymysql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# db_url = "mysql+mysqldb://root:Nmefc_814@128.5.9.79:3306/surge_global_sys"

# 创建数据库连接
engine = create_engine(db_url)
# engine = create_engine(
#     f"mysql+mysqldb://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}/'?charset={DB_CONFIG['charset']}"
# )

# 需要查询的 station_code 列表
station_codes = ['lecy', ]

# 按月分表的表名列表（可动态生成）
# TODO:[-] 25-02-05 由于需要生成约370天的数据，所以需要加入跨年后的数据
monthly_tables = [
    'station_realdata_specific_202401',
    'station_realdata_specific_202402',
    'station_realdata_specific_202403',
    'station_realdata_specific_202404',
    'station_realdata_specific_202405',
    'station_realdata_specific_202406',
    'station_realdata_specific_202407',
    'station_realdata_specific_202408',
    'station_realdata_specific_202409',
    'station_realdata_specific_202410',
    'station_realdata_specific_202411',
    'station_realdata_specific_202412',
    'station_realdata_specific_202501',
]

# 输出文件路径
output_dir = "./data"  # 确保该目录存在


def main():
    YEAR_STR: str = '2024'
    # 遍历 station_code 并分别查询和保存结果
    for station_code in LIST_STATION:
        all_data = []  # 用于存储该 station_code 的所有数据

        for table in monthly_tables:
            # 构造 SQL 查询
            query = f"""
            SELECT station_code, surge, ts, gmt_realtime
            FROM {table}
            WHERE station_code = '{station_code}'
            ORDER BY ts ASC;
            """
            # query = "SELECT * FROM station_realdata_specific_202402;"
            try:
                # 执行查询并将结果转换为 DataFrame
                # TODO:[*] 25-01-02 __init__() got multiple values for argument 'schema'
                df = pd.read_sql(query, con=engine)
                # df = pd.read_sql_query(query, engine, schema=None)
                all_data.append(df)
                print(f'[-]读取station:{station_code}——table:{table}完毕!')
            except Exception as e:
                print(f"查询表 {table} 时出错: {e}")
                continue

        # 合并所有数据
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)

            file_name_temp: str = f'{station_code}_{YEAR_STR}.csv'
            # 保存为 CSV 文件
            output_fullpath: str = str(pathlib.Path(output_dir) / file_name_temp)
            combined_df.to_csv(output_fullpath, index=False)
            print(f"数据已保存到文件: {output_fullpath}")
        else:
            print(f"未查询到 {station_code} 的数据。")


if __name__ == '__main__':
    main()
