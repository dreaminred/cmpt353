import sys
import pandas as pd 
import numpy as np
from scipy import stats
import warnings
warnings.filterwarnings('ignore')


OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value:  {more_searches_p:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value:  {more_instr_searches_p:.3g}'
)

def apply_ID_Filter(uid):
    if(uid%2==0):
        return "control"
    else:
        return "treatment"
    
def search_filter(search_count):
    return(search_count!=0)



def main():
    searchdata_file = sys.argv[1]

    # ...
    
    data = pd.read_json('searches.json',orient='records', lines=True)
    data_instructor = data[data['is_instructor'] == True]
    
    odd_treatment = data[data['uid']%2==1]
    even_control = data[data['uid']%2==0]

    data['class'] = data.apply(lambda x: apply_ID_Filter(x['uid']), axis=1)
    data['Search > 0'] = data.apply(lambda x: search_filter(x['search_count']),axis=1)
    
    contingency = pd.crosstab(data['class'],data['Search > 0'])
    
    c, p_chi2, dof, expected = stats.chi2_contingency(contingency)
    u ,p_utest =stats.mannwhitneyu(odd_treatment['search_count'], even_control['search_count'], alternative='two-sided')
    
    odd_treatment = data_instructor[data_instructor['uid']%2==1]
    even_control = data_instructor[data_instructor['uid']%2==0]

    data_instructor['class'] = data_instructor.apply(lambda x: apply_ID_Filter(x['uid']), axis=1)
    data_instructor['Search > 0'] = data_instructor.apply(lambda x: search_filter(x['search_count']),axis=1)
    contingency = pd.crosstab(data_instructor['class'],data_instructor['Search > 0'])
    
    c, p_chi2_2, dof, expected = stats.chi2_contingency(contingency) 
    
    u_inst,p_utest_2 = stats.mannwhitneyu(odd_treatment['search_count'], even_control['search_count'], alternative='two-sided')


    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p=p_chi2,
        more_searches_p=p_utest,
        more_instr_p=p_chi2_2,
        more_instr_searches_p=p_utest_2,
    ))


if __name__ == '__main__':
    main()
