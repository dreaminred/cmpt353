import pandas as pd
import numpy as np
import time
from implementations import all_implementations

def main():
    
    n=10**3
    n_samples = int(n/10)


    results = np.zeros([n_samples,len(all_implementations)])

    for i in np.arange(n_samples):

        myArray = np.random.randint(10000, 100 * n + 10000, size=(n,))

        for j in np.arange(len(all_implementations)):
            start = time.time()
            res = all_implementations[j](myArray)
            end = time.time()
            elapsedTime = end-start

            results[i][j] = elapsedTime

    data = pd.DataFrame(results, columns=[all_implementations])
    data.to_csv('data.csv', index=False)
    
    
    
if __name__ == '__main__':
    main()