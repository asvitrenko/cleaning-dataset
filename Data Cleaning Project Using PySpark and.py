#Data Cleaning Project Using PySpark and pandas

# Importing necessary libraries
import os
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np

# Importing the dataset
file_path = os.path.join("/Applications", "developer_directory", "unclean_smartwatch_health_data.csv")
if not os.path.exists(file_path):
    raise FileNotFoundError(f"Файл {file_path} не найден. Проверьте путь.")
smartwatch = pd.read_csv(file_path)

# Checking the dataset for any inconsistencies
print(smartwatch.info())
print(smartwatch.isnull().sum())
print(smartwatch.head(20))

# Dropping rows where 'User ID' is NaN
smartwatch = smartwatch.dropna(subset=['User ID'])

# Converting 'User ID' from float to int
smartwatch['User ID'] = smartwatch['User ID'].astype('int64')

# Removing duplicates if any
smartwatch.drop_duplicates(inplace=True)

# Converting 'Sleep Duration (hours)' column to float and replacing "ERROR" with 0
smartwatch['Sleep Duration (hours)'] = smartwatch['Sleep Duration (hours)'].replace("ERROR", '0').astype('float64')

# Fixing hyphens in 'Activity Level' column
smartwatch['Activity Level'] = smartwatch['Activity Level'].str.replace("_", " ")

# Fixing 'Stress Level' column: Replacing 'Very High' with 9
smartwatch['Stress Level'] = smartwatch['Stress Level'].replace('Very High', 9).astype('float64')

# Fixing the 'Actve' error in 'Activity Level' column
smartwatch['Activity Level'] = smartwatch['Activity Level'].str.replace('Actve', 'Active')
smartwatch['Activity Level'] = smartwatch['Activity Level'].str.replace('Seddentary', 'Sedentary')

# Filling empty entries with mean, median, or mode
smartwatch['Heart Rate (BPM)'] = smartwatch['Heart Rate (BPM)'].fillna(smartwatch['Heart Rate (BPM)'].median())
smartwatch['Blood Oxygen Level (%)'] = smartwatch['Blood Oxygen Level (%)'].fillna(smartwatch['Blood Oxygen Level (%)'].median())
smartwatch['Step Count'] = smartwatch['Step Count'].fillna(smartwatch['Step Count'].median())
smartwatch['Sleep Duration (hours)'] = smartwatch['Sleep Duration (hours)'].fillna(smartwatch['Sleep Duration (hours)'].median())
smartwatch['Stress Level'] = smartwatch['Stress Level'].fillna(smartwatch['Stress Level'].mode()[0])
smartwatch['Activity Level'] = smartwatch['Activity Level'].fillna(smartwatch['Activity Level'].mode()[0])

# Reducing decimal values to 2 for float columns
smartwatch['Heart Rate (BPM)'] = smartwatch['Heart Rate (BPM)'].round(2)
smartwatch['Blood Oxygen Level (%)'] = smartwatch['Blood Oxygen Level (%)'].round(2)
smartwatch['Step Count'] = smartwatch['Step Count'].round(2)
smartwatch['Sleep Duration (hours)'] = smartwatch['Sleep Duration (hours)'].round(2)

# Removing outliers using Interquartile Range (IQR)
def remove_outliers(df, col):
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower_limit = Q1 - 1.5 * IQR
    upper_limit = Q3 + 1.5 * IQR
    df = df[(df[col] <= upper_limit) & (df[col] >= lower_limit)]
    return df

columns_to_check = ['Heart Rate (BPM)', 'Blood Oxygen Level (%)', 'Step Count', 'Sleep Duration (hours)', 'Stress Level']
for column in columns_to_check:
    smartwatch = remove_outliers(smartwatch, column)

# Removing rows with illogical values based on domain knowledge
smartwatch = smartwatch[(smartwatch['Heart Rate (BPM)'] <= 220) & (smartwatch['Heart Rate (BPM)'] >= 40)]
smartwatch = smartwatch[(smartwatch['Blood Oxygen Level (%)'] <= 100) & (smartwatch['Blood Oxygen Level (%)'] >= 70)]
smartwatch = smartwatch[(smartwatch['Step Count'] >= 0)]
smartwatch = smartwatch[(smartwatch['Sleep Duration (hours)'] <= 24) & (smartwatch['Sleep Duration (hours)'] > 0)]
smartwatch = smartwatch[(smartwatch['Stress Level'] <= 10) & (smartwatch['Stress Level'] >= 1)]

# Converting 'Stress Level' and 'Step Count' to integers after fixing data
smartwatch['Stress Level'] = smartwatch['Stress Level'].astype('int64')
smartwatch['Step Count'] = smartwatch['Step Count'].astype('int64')

# Checking for inconsistent categorical values in 'Activity Level' column
smartwatch['Activity Level'] = smartwatch['Activity Level'].str.strip().str.lower()

# Resetting the index after all changes
smartwatch = smartwatch.reset_index(drop=True)

# Checking if all changes have been successfully implemented
print(smartwatch.info())
print(smartwatch.isnull().sum())
print(smartwatch.head(20))

# Saving the cleaned dataset to a new CSV file
smartwatch.to_csv('Stop_Watch_Health_Dataset_Cleaned.csv', index=False)

# The dataset is now clean and ready for analysis.


