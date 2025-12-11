import pandas as pd
import pickle
import numpy as np
import os

# --- 1. 配置部分 ---
# 原始数据根目录
raw_dir = 'data/raw/WESAD'
# 输出文件路径 (我们将把所有人合并成这一个文件)
output_path = 'data/clean/wesad_all_subjects.csv'

# 获取所有以 'S' 开头的文件夹列表 (自动排除 .DS_Store 或 pdf)
# 也就是 S2, S3, ..., S17
subject_ids = [d for d in os.listdir(raw_dir) if d.startswith('S') and os.path.isdir(os.path.join(raw_dir, d))]
subject_ids.sort() # 排序，保证顺序 S2, S3...

print(f"📋 检测到 {len(subject_ids)} 个受试者: {subject_ids}")

# 用于暂时存放每个人的小表格
all_data_list = []

# --- 2. 循环处理每个人 ---
for subj in subject_ids:
    pkl_path = os.path.join(raw_dir, subj, f'{subj}.pkl')
    
    if not os.path.exists(pkl_path):
        print(f"⚠️ 跳过 {subj}: 找不到文件 {pkl_path}")
        continue
        
    print(f"🔄 正在处理 {subj} ...")
    
    try:
        # 读取 .pkl
        with open(pkl_path, 'rb') as file:
            data = pickle.load(file, encoding='latin1')
            
        # 提取 Chest 数据
        chest = data['signal']['chest']
        labels = data['label']
        
        # 建立 DataFrame
        df = pd.DataFrame({
            'Subject': subj,  # 新增一列：记录是谁的数据
            'EDA': chest['EDA'].flatten(),
            'Temp': chest['Temp'].flatten(),
            'ECG': chest['ECG'].flatten(),
            'Resp': chest['Resp'].flatten(),
            'Label': labels
        })
        
        # 过滤 Label (只保留定义的活动)
        # 1=Baseline, 2=Stress, 3=Amusement, 4=Meditation
        df = df[df['Label'].isin([1, 2, 3, 4])]
        
        # 降采样 (每 70 行取 1 行, 700Hz -> 10Hz)
        df_small = df.iloc[::70, :].copy()
        
        # 映射标签名
        label_map = {1: 'Baseline', 2: 'Stress', 3: 'Amusement', 4: 'Meditation'}
        df_small['Condition'] = df_small['Label'].map(label_map)
        
        # 将处理好的这一小块数据加入列表
        all_data_list.append(df_small)
        
        # 释放内存 (Python 会自动回收，但显式删除好习惯)
        del data, df, chest
        
    except Exception as e:
        print(f"❌ 处理 {subj} 时出错: {e}")

# --- 3. 合并并保存 ---
if all_data_list:
    print("📦 正在合并所有受试者数据...")
    final_df = pd.concat(all_data_list, ignore_index=True)
    
    # 确保保存目录存在
    os.makedirs('data/clean', exist_ok=True)
    
    final_df.to_csv(output_path, index=False)
    
    print("-" * 30)
    print(f"✅ 大功告成！所有数据已合并保存至: {output_path}")
    print(f"📊 总数据行数: {len(final_df)}")
    print(f"👥 包含受试者: {final_df['Subject'].unique()}")
    print("-" * 30)
else:
    print("❌ 没有处理任何数据。")
