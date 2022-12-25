import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from pykalman import KalmanFilter
from statsmodels.nonparametric.smoothers_lowess import lowess


def main():
    
    input_csv = sys.argv[1]
    
    cpu_data = pd.read_csv(input_csv)
    
    cpu_data['timestamp'] = pd.to_datetime(cpu_data['timestamp'])
    
    lowess_smoothed = lowess(cpu_data['temperature'], cpu_data['timestamp'], frac= 0.01)
    
    kalman_data = cpu_data[['temperature', 'cpu_percent', 'sys_load_1', 'fan_rpm']]

    initial_state = kalman_data.iloc[0]
    observation_covariance = np.diag([0.4, 0.1, 0.1, 0.01]) ** 2 # TODO: shouldn't be zero
    transition_covariance = np.diag([0.1, 0.3, 0.3, 0.1]) ** 2 # TODO: shouldn't be zero
    transition = [[0.96,0.5,0.2,0.001], [0.1,0.4,2.3,0], [0,0,0.96,0], [0,0,0,1]] # TODO: shouldn't (all) be zero

    kf = KalmanFilter(
    initial_state_mean = initial_state,
    initial_state_covariance = observation_covariance,
    observation_covariance = observation_covariance,
    transition_covariance = transition_covariance,
    transition_matrices = transition
    )
    
    kalman_smoothed, _ = kf.smooth(kalman_data)
    
    plt.figure(figsize=(14,7))
    plt.plot(cpu_data['timestamp'],lowess_smoothed[:,1], 'r-', label = "LOESS")
    plt.xlabel("Time")
    plt.xticks(rotation=25);
    plt.ylabel("Temperature, Celsius")

    plt.plot(cpu_data['timestamp'], kalman_smoothed[:, 0], 'g-', label = "Kalman Filter")
    plt.xticks(rotation=25);
    plt.xlabel("Time")
    plt.ylabel("Temperature, Celsius")

    plt.plot(cpu_data['timestamp'], cpu_data['temperature'], 'b.', alpha=0.5, label = "Raw data")
    plt.xticks(rotation=90)
    plt.xlabel("Time, minutes")
    
    plt.title("CPU temperature time series")

    plt.legend()
    plt.savefig('cpu.svg')
    #plt.savefig('cpu.svg') # for final submission


if __name__ == '__main__':
    main()
