#CMPT353 - Fall 2022
#Armin Hatami
#301278375


import numpy as np

# Load data
data = np.load('monthdata.npz')
totals = data['totals']
counts = data['counts']

#Print statistics
print("City with the lowest total precipitation:")
print(np.argmin(np.sum(totals, axis=1)))
print("Average precipitation in each month:")
print(np.sum(totals, axis=0) / np.sum(counts, axis=0))
print("Average precipitation in each city:")
print(np.sum(totals, axis=1) / np.sum(counts, axis=1))
print("Quarterly precipitation totals:")
print(np.sum(np.reshape(totals, [totals.shape[0], 4, 3]), axis=2))
