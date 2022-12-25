#CMPT353 - Fall 2022
#Armin Hatami
#301278375

import pandas as pd

#Load data
totals = pd.read_csv('totals.csv').set_index(keys=['name'])
counts = pd.read_csv('counts.csv').set_index(keys=['name'])

#Print statistics
print("City with the lowest total precipitation:")
print(totals.sum(axis=1).idxmin())
print("Average precipitation in each month:")
print(totals.sum(axis=0)/counts.sum(axis=0))
print("Average precipitation in each city:")
print(totals.sum(axis=1)/counts.sum(axis=1))