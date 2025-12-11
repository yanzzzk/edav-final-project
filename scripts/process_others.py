import pandas as pd
import numpy as np
import os
import glob
import zipfile

print("🚀 脚本开始运行 (v2.0 - 支持 Zip 读取)...")

# ==========================================
# 任务 1: 处理 SWELL (办公压力)
# ==========================================
swell_path = 'data/raw/Behavioral-features - per minute.xlsx'
swell_out = 'data/clean/swell_processed.csv'

# 为了节省时间，如果已经处理好了，这次就提示一下
if os.path.exists(swell_out):
    print("✅ SWELL 数据之前已处理好，跳过。(如需重新处理请删除 clean 下的 csv)")
elif os.path.exists(swell_path):
    print(f"正在读取 SWELL Excel: {swell_path}")
    try:
        df_swell = pd.read_excel(swell_path)
        df_swell['Dataset'] = 'SWELL'
        os.makedirs('data/clean', exist_ok=True)
        df_swell.to_csv(swell_out, index=False)
        print(f"✅ SWELL 处理成功: {swell_out}")
    except Exception as e:
        print(f"❌ SWELL 失败: {e}")
else:
    print(f"⚠️ 找不到 SWELL 文件，跳过。")

print("-" * 30)

# ==========================================
# 任务 2: 处理 AffectiveROAD (驾驶压力) - Zip版
# ==========================================
road_base_dir = 'data/raw/AffectiveROAD_Data/Database/E4'
road_out = 'data/clean/road_hr_all.csv'

drive_folders = glob.glob(os.path.join(road_base_dir, '*-E4-*'))

if not drive_folders:
    print(f"⚠️ 在 {road_base_dir} 没找到文件夹，请检查路径。")
else:
    print(f"🚗 找到 {len(drive_folders)} 个驾驶记录，准备从 Zip 中提取数据...")
    all_drives = []
    
    for folder in drive_folders:
        drive_id = os.path.basename(folder)
        
        # 优先找 Left.zip (通常戴左手), 如果没有找 Right.zip
        zip_path = os.path.join(folder, 'Left.zip')
        if not os.path.exists(zip_path):
            zip_path = os.path.join(folder, 'Right.zip')
            
        if os.path.exists(zip_path):
            try:
                # 打开 Zip 文件
                with zipfile.ZipFile(zip_path, 'r') as z:
                    # 检查 HR.csv 是否在压缩包里
                    if 'HR.csv' in z.namelist():
                        # 直接从内存读取 HR.csv
                        with z.open('HR.csv') as f:
                            # E4 格式: 前两行是 metadata
                            # 技巧: 先读全部，再分割
                            df_raw = pd.read_csv(f, header=None)
                            
                            start_time = df_raw.iloc[0, 0]
                            sample_rate = df_raw.iloc[1, 0]
                            hr_values = df_raw.iloc[2:, 0].values
                            
                            # 生成时间轴
                            seconds = np.arange(len(hr_values)) / sample_rate
                            
                            # 创建 DataFrame
                            df_temp = pd.DataFrame({
                                'DriveID': drive_id,
                                'Time_Rel': seconds,
                                'HR': hr_values,
                                'Dataset': 'AffectiveROAD'
                            })
                            all_drives.append(df_temp)
                            print(f"  -> 已提取: {drive_id} (来自 {os.path.basename(zip_path)})")
                    else:
                        print(f"  ⚠️ {drive_id}: 压缩包里没找到 HR.csv")
            except Exception as e:
                print(f"  ❌ 读取 {drive_id} 失败: {e}")
        else:
            print(f"  ⚠️ 跳过 {drive_id}: 没找到 Left.zip 或 Right.zip")
    
    # 合并保存
    if all_drives:
        final_df = pd.concat(all_drives, ignore_index=True)
        final_df.to_csv(road_out, index=False)
        print(f"✅ AffectiveROAD (驾驶) 数据合并成功! 保存至 {road_out}")
        print(f"📊 总数据点: {len(final_df)}")
    else:
        print("❌ 依然没有提取到驾驶数据，请检查 Zip 包内容。")

print("🎉 脚本结束！")
