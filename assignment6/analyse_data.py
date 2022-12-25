import pandas as pd
import numpy as np
import math
from implementations import all_implementations
from matplotlib import pyplot as plt
from scipy import stats

OUTPUT_TEMPLATE = (
'Qs1 mean time {qs1_mean:.3g}: seconds\n'
'Qs2 mean time {qs2_mean:.3g}: seconds\n'
'Qs3 mean time {qs3_mean:.3g}: seconds\n'
'Qs4 mean time {qs4_mean:.3g}: seconds\n'
'Qs5 mean time {qs5_mean:.3g}: seconds\n'
'mergesort mean time {mergesort_mean:.3g}: seconds\n'
'partitionsort mean time {partition_mean:.3g}: seconds\n\n\n'
'partition sort < qs1 t-test: p-value {ttest_merge_part:.3g}\n'
'Qs2 sort < merge sort t-test: p-value {ttest_qs2_merge:.3g}\n'
'Qs3 sort < merge sort t-test: p-value {ttest_qs3_merge:.3g}\n'
'Qs5 sort < Qs4 sort t-test: p-value {ttest_qs3_qs4:.3g}\n'
)

def main():
    
    
    n = len(all_implementations)
    combs = n*(n-1)/2
    print("# of T-Test combinations for " + str(n) + " sorting functions are " + str(int(combs)) + ".") 
    input_data = pd.read_csv('data.csv')
    print(f"The probability of coming to a false conclusion is {0.05/combs:0.4f}.")
    
    


    input_data = pd.read_csv('data.csv')

    qs1_mean_ = input_data.iloc[:,0].mean()
    qs2_mean_ = input_data.iloc[:,1].mean()
    qs3_mean_ = input_data.iloc[:,2].mean()
    qs4_mean_ = input_data.iloc[:,3].mean()
    qs5_mean_ = input_data.iloc[:,4].mean()
    mergesort_mean_ = input_data.iloc[:,5].mean()
    partition_mean_ = input_data.iloc[:,6].mean()

    ttest_merge_part_ = stats.ttest_ind(input_data.iloc[:,0], input_data.iloc[:,6], alternative='greater')[1]
    ttest_qs2_merge_ = stats.ttest_ind(input_data.iloc[:,5],input_data.iloc[:,1], alternative='greater')[1]
    ttest_qs3_merge_ = stats.ttest_ind(input_data.iloc[:,2],input_data.iloc[:,5], alternative='less')[1]
    ttest_qs3_qs4_ = stats.ttest_ind(input_data.iloc[:,4],input_data.iloc[:,3], alternative='less')[1]


    # Output
    print(OUTPUT_TEMPLATE.format(
        qs1_mean = qs1_mean_,
        qs2_mean = qs2_mean_,
        qs3_mean = qs3_mean_,
        qs4_mean = qs4_mean_,
        qs5_mean = qs5_mean_,
        mergesort_mean = mergesort_mean_,
        partition_mean = partition_mean_,
        ttest_merge_part = ttest_merge_part_,
        ttest_qs2_merge = ttest_qs2_merge_,
        ttest_qs3_merge = ttest_qs3_merge_,
        ttest_qs3_qs4 = ttest_qs3_qs4_,
    ))
    
    plt.style.use('seaborn')
    print("Comment out plotting for loop to see figure.")
    ######not quite sure if I am allowed to use for loops for plotting purposes
    #for i in range(len(all_implementations)): 
    #    plt.hist(input_data.iloc[:,i],alpha=0.5, bins='auto',label=(all_implementations[i].__name__))

    plt.xlabel('Time, seconds')
    plt.ylabel('Counts')
    plt.legend()
    plt.savefig('sort-implementation.pdf')
 
    
    

if __name__ == '__main__':
    main()