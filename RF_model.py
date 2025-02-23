import numpy as np
import pandas as pd
import glob  # 用于查找多个文件
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics

# 读取数据
# csv_files = glob.glob("datasets/Training_data/*.csv")  # 调整路径
csv_files = glob.glob("datasets/Training_data/XGL_results_20250223_091807.csv")  # 调整路径
df_list = [pd.read_csv(file) for file in csv_files]  # 加载所有CSV文件
df = pd.concat(df_list, ignore_index=True)  # 合并所有数据

# 定义需要移除的特征列
drop_cols = ["business_id", "name", "address", "city", "state", "postal_code", "geometry", "latitude", "longitude", "is_open"]
df = df.drop(columns=drop_cols)

# 处理类别变量（One-Hot Encoding）
categories = df['categories'].str.split(',').explode().unique()  # 获取所有唯一类别
category_dict = {category: i for i, category in enumerate(categories)}  # 创建类别字典

def one_hot_encode(categories_list):
    one_hot = np.zeros(len(categories))
    for category in categories_list:
        if category in category_dict:
            one_hot[category_dict[category]] = 1
    return one_hot

# 对 'categories' 进行 One-Hot 编码
df['categories'] = df['categories'].apply(lambda x: one_hot_encode(str(x).split(',')))

# 合并所有One-Hot编码的类别列
category_df = pd.DataFrame(df['categories'].tolist(), columns=categories)
df = pd.concat([df.drop(columns='categories'), category_df], axis=1)

# 处理数值特征（填充缺失值）
df.fillna(df.median(), inplace=True)

# 确定标签列
target_column = "success_index"
df[target_column] = pd.to_numeric(df[target_column], errors='coerce')

# 分离特征和标签
X = df.drop(columns=[target_column])
y = df[target_column]

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=45)

X_train.columns = X_train.columns.astype(str)
X_test.columns = X_test.columns.astype(str)

# 训练 Random Forest Regressor 模型
rf_regressor = RandomForestRegressor(n_estimators=100, random_state=42)
rf_regressor.fit(X_train, y_train)

# 预测测试数据
y_pred = rf_regressor.predict(X_test)

# 评估模型性能
r2_score = metrics.r2_score(y_test, y_pred)
mae = metrics.mean_absolute_error(y_test, y_pred)
mse = metrics.mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)

# 打印指标
print(f'R² Score: {r2_score:.4f}')
print(f'MAE: {mae:.4f}')
print(f'MSE: {mse:.4f}')
print(f'RMSE: {rmse:.4f}')

# 画图
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

# 残差分布图
residuals = y_test - y_pred
ax[0].hist(residuals, bins=20, edgecolor='black', alpha=0.7)
ax[0].set_title("Residual Distribution")
ax[0].set_xlabel("Residuals")
ax[0].set_ylabel("Frequency")

# 真实值 vs 预测值
ax[1].scatter(y_test, y_pred, alpha=0.5, color="blue", edgecolor="black")
ax[1].plot([min(y_test), max(y_test)], [min(y_test), max(y_test)], "--r", label="Ideal Fit")
ax[1].set_title("Actual vs Predicted")
ax[1].set_xlabel("Actual Values")
ax[1].set_ylabel("Predicted Values")
ax[1].legend()

plt.tight_layout()
plt.show()
