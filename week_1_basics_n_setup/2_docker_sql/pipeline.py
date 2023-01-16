import pandas as pd
import sys

print(sys.argv)


dataFrame = pd.read_excel(sys.argv[1],0)

print(dataFrame.shape)

print('Completed pipeline')