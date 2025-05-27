# -*- coding: utf-8 -*-


# 导入依赖
import pandas as pd
import json


# 数据集路径配置
LOCATION_DATASETS = {
    '省市': 'data/locations/province_city.json',
    '省市简': 'data/locations/province_city_short.json',
    '省': 'data/locations/province.json'
}


# 数据集匹配优先级设置
location_types = ['省市', '省市简', '省']


# 加载地理位置数据集
def load_location_data():
    location_data = {}
    try:
        for dataset_name, file_path in LOCATION_DATASETS.items():
            with open(file_path, 'r', encoding='utf-8') as f:
                location_data[dataset_name] = json.load(f)
    except FileNotFoundError as e:
        print(f"[错误] 加载地理位置数据失败: {e}")
        return None
    return location_data


# 查找最后出现的地理位置并返回省份
def find_province(text, location_data):
    if not text or not location_data:
        return None

    # 按优先级遍历位置类型
    for loc_type in location_types:
        if loc_type in location_data:
            # 在当前位置类型中搜索每个省份的位置
            for province, locations in location_data[loc_type].items():

                # 如果：locations为列表
                if isinstance(locations, list):
                    # 按长度降序排序位置，优先查找最长匹配
                    sorted_locations = sorted(locations, key=len, reverse=True)
                    for location in sorted_locations:
                        # 查找文本中最后出现的位置
                        last_index = text.rfind(location)
                        if last_index != -1:
                            # 找到匹配项，返回省份
                            return province

                # 再如果：locations为字符串
                elif isinstance(locations, str):
                    # 在文本中查找省份简称最后出现的位置
                    last_index = text.rfind(locations)
                    if last_index != -1:
                        # 找到匹配项，返回省份
                        return province

    # 否则：所有位置类型都没有匹配项，返回None
    return None


# 主要处理逻辑
def process_excel(input_path, output_path, location_data):
    if not location_data:
        print("[错误] 地理位置数据未加载，程序退出。")
        return

    try:
        # 读取Excel文件中的所有工作表
        excel_file = pd.ExcelFile(input_path)
        all_sheets = excel_file.sheet_names

        # 用于存储处理后的数据框的字典
        processed_sheets = {}

        for sheet_name in all_sheets:
            print(f"[处理中] 正在处理工作表: {sheet_name}")
            df = excel_file.parse(sheet_name)

            # 查找'招标信息'列
            tender_info_col = None
            if '招标信息' in df.columns:
                tender_info_col = '招标信息'
            else:
                # 通过检查第一行内容查找列
                for col in df.columns:
                    if df.iloc[0][col] == '招标信息':
                        tender_info_col = col
                        # 如果以这种方式找到，移除标题行
                        df.columns = df.iloc[1]
                        df = df[2:].reset_index(drop=True)
                        break

            if tender_info_col is None:
                print(f"[警告] 在工作表'{sheet_name}'中未找到'招标信息'列，已跳过。")
                processed_sheets[sheet_name] = df  # 如果未找到列，保留原始数据框
                continue

            # 查找'招标时间'列以确定在需要时插入'所属省份'的位置
            tender_time_col_index = -1
            try:
                tender_time_col_index = df.columns.get_loc('招标时间')
            except KeyError:
                print(
                    f"[警告] 在工作表'{sheet_name}'中未找到'招标时间'列。如果'所属省份'列不存在，将添加在末尾。")

            # 查找'所属省份'列或确定插入位置
            province_col_index = -1
            if '所属省份' in df.columns:
                province_col_index = df.columns.get_loc('所属省份')
            elif tender_time_col_index != -1:
                # 在'招标时间'后插入'所属省份'
                province_col_index = tender_time_col_index + 1
                df.insert(province_col_index, '所属省份', None)
            else:
                # 在末尾添加'所属省份'
                df['所属省份'] = None
                province_col_index = df.columns.get_loc('所属省份')

            # 确保'所属省份'列为字符串类型
            df['所属省份'] = df['所属省份'].astype(str)

            # 处理'招标信息'列中的每一行
            for index, row in df.iterrows():
                tender_info = row[tender_info_col]
                if pd.notna(tender_info):  # 检查单元格是否为空
                    province = find_province(str(tender_info), location_data)
                    if province:
                        df.at[index, '所属省份'] = province

            processed_sheets[sheet_name] = df

        # 将修改后的数据框写入新的Excel文件
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet_name, df in processed_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"[成功] 已处理并保存到 {output_path}")

    except FileNotFoundError:
        print(f"[错误] 在 {input_path} 未找到输入文件")
    except Exception as e:
        print(f"[错误] 发生异常: {e}")


# 主程序执行
if __name__ == "__main__":
    input_excel_path = 'docs/input/招标信息汇总.xlsx'
    output_excel_path = 'docs/output/招标信息汇总.xlsx'

    locations = load_location_data()
    if locations:
        process_excel(input_excel_path, output_excel_path, locations)
