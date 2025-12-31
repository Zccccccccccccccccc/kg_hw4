import pandas as pd
import numpy as np

def clean_data(input_path, output_path):
    print(f"正在读取原始数据: {input_path}")
    try:
        df = pd.read_csv(input_path, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(input_path, encoding='ISO-8859-1')

    # 1. 基础清理：去除列名空格并删除核心空值
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=['App_Id', 'App_Name'])

    # 2. 处理 App_Id (Bundle ID 保持字符串格式)
    df['App_Id'] = df['App_Id'].astype(str).str.strip()

    # 3. 数值转换与清洗
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce').fillna(0.0)
    df['Average_User_Rating'] = pd.to_numeric(df['Average_User_Rating'], errors='coerce').fillna(0.0)
    df['Reviews'] = pd.to_numeric(df['Reviews'], errors='coerce').fillna(0).astype(int)
    df['Size_Bytes'] = pd.to_numeric(df['Size_Bytes'], errors='coerce').fillna(0)

    # 4. 离散化处理 (Bucketing)
    
    # 价格档位
    def get_price_tier(p):
        if p == 0: return "Free"
        if p < 4.99: return "Affordable (<$5)"
        return "Premium (>$5)"
    df['Price_Tier'] = df['Price'].apply(get_price_tier)

    # 空间占用档位
    def get_size_bucket(b):
        mb = b / (1024 * 1024)
        if mb < 100: return "Small (<100MB)"
        if mb < 1000: return "Medium (100MB-1GB)"
        return "Large (>1GB)"
    df['Size_Bucket'] = df['Size_Bytes'].apply(get_size_bucket)

    # 5. 时间处理 (提取年份)
    df['Release_Year'] = pd.to_datetime(df['Released'], errors='coerce').dt.year.fillna(0).astype(int).astype(str)

    # 6. 填充其他文本分类缺失值
    df['Primary_Genre'] = df['Primary_Genre'].fillna('Unknown')
    df['Developer'] = df['Developer'].fillna('Unknown Developer')
    df['Content_Rating'] = df['Content_Rating'].fillna('Not Rated')
    df['Required_IOS_Version'] = df['Required_IOS_Version'].fillna('Unknown')

    # 7. 去重并保存
    df = df.drop_duplicates(subset=['App_Id'])
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"清洗完成！有效记录: {len(df)} 条。")
    print(f"清洗后的数据已保存至: {output_path}")

if __name__ == "__main__":
    clean_data('appleAppData.csv', 'AppleStore_Cleaned.csv')