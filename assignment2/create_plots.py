import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


#Get file names from command line
filename1 = sys.argv[1]
filename2 = sys.argv[2]

#Read files
file1 = pd.read_csv(filename1, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes'])

file2 = pd.read_csv(filename2, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes'])

#Sorting data by the number of views, descending order
file1 = file1.sort_values('views', ascending=False).reset_index()
file2 = file2.sort_values('views', ascending=False).reset_index()

file1 = file1.rename(columns={"views":"views1"})
file2 = file2.rename(columns={"views":"views2"})
file3 = pd.merge(file1,file2,left_on="page",right_on="page")


plt.figure(figsize=(15, 7)) # change the size to something sensible'
plt.subplot(1, 2, 1) # subplots in 1 row, 2 columns, select the first
plt.plot(np.arange(len(file1['views1'])),file1['views1'], linewidth=4)
plt.xlabel("Rank, descending", size="x-large")
plt.ylabel("Views",  size="x-large")
plt.title("Popularity Distribution", size="x-large")
plt.subplot(1, 2, 2) # ... and then select the second
plt.scatter(file3['views1'],file3['views2'], s=60, alpha=0.7, edgecolors="k") # build plot 2; styling adapted fromhttps://www.python-graph-gallery.com/scatterplot-and-log-scale-in-matplotlib
plt.xscale('log')
plt.yscale('log')
plt.xlabel("Views, 1st hour", size="x-large")
plt.ylabel("Views, 2nd hour",  size="x-large")
plt.title("Hourly Correlation", size="x-large")
plt.savefig('wikipedia.png')

plt.savefig('wikipedia.png')
