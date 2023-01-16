import pandas as pd

dataFrame = pd.read_excel('customer-list.xlsx',"Customer List")

print(dataFrame.shape)

print('Completed pipeline')