import sys
import pandas as pd
import numpy as np
import scipy.stats
from matplotlib import pyplot as plt


def isWeekday(rowEntry):
    if rowEntry.dayofweek < 5:
        return True;
    else:
        return False

def getYear(rowEntry):
    return rowEntry.isocalendar()[0];


def getWeek(rowEntry):
    return rowEntry.isocalendar()[1];

OUTPUT_TEMPLATE = (
    "Initial T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann-Whitney U-test p-value: {utest_p:.3g}"
)


def main():
    reddit_counts = sys.argv[1]

    # ...

    counts = pd.read_json(reddit_counts, lines=True)

    # Filter subreddits to r/canada only
    counts = counts[counts['subreddit'] == 'canada']

    # Filter year
    counts['year'] = counts.apply(lambda row: getYear(row['date']), axis=1)
    counts = counts[(counts['year'] < 2014) & (counts['year'] > 2011)]

    # Add week number
    counts['week'] = counts.apply(lambda row: getWeek(row['date']), axis=1)

    # Separating weekday and weekend data
    counts['Weekday'] = counts.apply(lambda row: isWeekday(row['date']), axis=1)
    counts_weekend = counts[counts['Weekday'] == False]
    counts_weekday = counts[counts['Weekday'] == True]

    # Initial Student T-test

    initial_ttest_p_ = \
    scipy.stats.ttest_ind(counts_weekend['comment_count'], counts_weekday['comment_count'], equal_var=True)[1]
    initial_weekday_normality_p_ = scipy.stats.normaltest(counts_weekday['comment_count'])[1]
    initial_weekend_normality_p_ = scipy.stats.normaltest(counts_weekend['comment_count'])[1]
    initial_levene_p_ = scipy.stats.levene(counts_weekend['comment_count'], counts_weekday['comment_count'])[1]

    # Tranforming the data

    countWkday = counts_weekday['comment_count'].to_numpy()
    countWkend = counts_weekend['comment_count'].to_numpy()

    countWkday = np.log(countWkday)
    countWkend = np.log(countWkend)

    transformed_weekday_normality_p_ = scipy.stats.normaltest(countWkday)[1]
    transformed_weekend_normality_p_ = scipy.stats.normaltest(countWkend)[1]
    transformed_levene_p_ = scipy.stats.levene(countWkday, countWkend)[1]

    # Central Limit Theorem

    countsWeekdayCLT = counts_weekday.groupby(['year', 'week']).aggregate('mean').reset_index()
    countsWeekendCLT = counts_weekend.groupby(['year', 'week']).aggregate('mean').reset_index()

    weekly_ttest_p_ = \
    scipy.stats.ttest_ind(countsWeekdayCLT['comment_count'], countsWeekendCLT['comment_count'], equal_var=True)[1]
    weekly_levene_p_ = scipy.stats.levene(countsWeekdayCLT['comment_count'], countsWeekendCLT['comment_count'])[1]
    weekly_weekday_normality_p_ = scipy.stats.normaltest(countsWeekdayCLT['comment_count'])[1]
    weekly_weekend_normality_p_ = scipy.stats.normaltest(countsWeekendCLT['comment_count'])[1]

    # U-test

    utest_p_ = \
    scipy.stats.mannwhitneyu(counts_weekday['comment_count'], counts_weekend['comment_count'], alternative='two-sided')[
        1]

    # ...
    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p=initial_ttest_p_,
        initial_weekday_normality_p=initial_weekday_normality_p_,
        initial_weekend_normality_p=initial_weekend_normality_p_,
        initial_levene_p=initial_levene_p_,
        transformed_weekday_normality_p=transformed_weekday_normality_p_,
        transformed_weekend_normality_p=transformed_weekend_normality_p_,
        transformed_levene_p=transformed_levene_p_,
        weekly_weekday_normality_p=weekly_weekday_normality_p_,
        weekly_weekend_normality_p=weekly_weekend_normality_p_,
        weekly_levene_p=weekly_levene_p_,
        weekly_ttest_p=weekly_ttest_p_,
        utest_p=utest_p_,
    ))

if __name__ == '__main__':
    main()