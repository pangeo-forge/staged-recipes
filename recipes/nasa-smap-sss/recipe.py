# +
import pandas as pd

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# +
daily_dates = pd.date_range('2015-05-04','2021-05-06',freq='D')
daily_dates = [str(daily_dates[i])[:10].replace('-','') for i in range(len(daily_dates))]

monthly_dates = pd.date_range('2015-04','2021-05',freq='M')
monthly_dates = [str(monthly_dates[i])[:8].replace('-','') for i in range(len(monthly_dates))]

# +
regular_year_count = list(range(1,366))

counter_list = (
    list(range(120,366)) # 2015 (mission start)
    + list(range(1,367)) # 2016 (leap year)
    + regular_year_count # 2017 
    + regular_year_count # 2018
    + regular_year_count # 2019
    + list(range(1,367)) # 2020 (leap year)
    + list(range(1,123)) # 2021 (current year)
)
counter_list = [f"{n:03}/" for n in counter_list]
# -

assert len(counter_list)==len(daily_dates), "Length mismatch!"
counter_dict = {daily_dates[i]:counter_list[i] for i in range(len(counter_list))}


# +
# a file pattern function

def make_full_path(algorithm, freq, date, counter_dict=counter_dict):
    '''
    Parameters
    ----------
    algorithm : str
        One of 'JPL' or 'RSS'
    freq : str
        One of '8day_running' or 'monthly'
    date : str
        if freq == '8day_running': 8-character numerical string of format YYYYMMDD
        else: 6-character numerical string of format YYYYMM
    counter_dict : dict
        Dictionary mapping for every year published here:
            https://podaac-opendap.jpl.nasa.gov/opendap/allData/smap/L3/JPL/V5.0/8day_running/
        ...to the daily count strings listed, e.g, here:
            https://podaac-opendap.jpl.nasa.gov/opendap/allData/smap/L3/JPL/V5.0/8day_running/2015/contents.html
    '''    
    assert algorithm in ('JPL', 'RSS'), 'freq must be one of \'JPL\' or \'RSS\''
    assert freq in ('8day_running', 'monthly'), (
        'algorithm must be one of \'8day_running\' or \'monthly\'')
    
    if freq == '8day_running':
        assert len(date) == 8, 'if freq==\'8day_running\', len(date) must == 8'
        count_string = counter_dict[date]
    else:
        assert len(date) == 6, 'if freq==\'monthly\', len(date) must == 6'
        count_string = ''
    
    url_base = 'https://podaac-opendap.jpl.nasa.gov/opendap/allData/smap/L3/'
    yr, mo = date[:4], date[4:6]
    day = date[6:8] if len(date) == 8 else None
    freq_caps = '8DAYS' if freq == '8day_running' else 'MONTHLY'
    file_date = f'{yr}{mo}{day}' if freq == '8day_running' else f'{yr}{mo}'
    url_tail = f'SMAP_L3_SSS_{file_date}_{freq_caps}_V5.0.nc'
    
    return url_base + f'{algorithm}/V5.0/{freq}/{yr}/{count_string}' + url_tail


# -

all_urls = {
    'jpl_eightday': [make_full_path('JPL', '8day_running', date=d) for d in daily_dates],
    'jpl_monthly': [make_full_path('JPL', 'monthly', date=d) for d in monthly_dates],
    'rss_eightday': [make_full_path('RSS', '8day_running', date=d) for d in daily_dates],
    'rss_monthly': [make_full_path('RSS', 'monthly', date=d) for d in monthly_dates],
}

list(all_urls)


