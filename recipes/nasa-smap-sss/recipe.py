# +
import pandas as pd

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


# +
def _return_jpl_dates():
    '''
    '''
    daily_dates = pd.date_range('2015-05-04','2021-05-06',freq='D')
    daily_dates = [str(daily_dates[i])[:10].replace('-','') for i in range(len(daily_dates))]

    monthly_dates = pd.date_range('2015-04','2021-05',freq='M')
    monthly_dates = [str(monthly_dates[i])[:8].replace('-','') for i in range(len(monthly_dates))]
    
    return daily_dates, monthly_dates

def _create_jpl_counter_dict():
    '''
    '''
    daily_dates, _ = _return_jpl_dates()
    
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
    counter_list = [f"{n:03}" for n in counter_list]
    assert len(counter_list)==len(daily_dates), "Length mismatch!"
    return {daily_dates[i]:counter_list[i] for i in range(len(counter_list))}


# +
def _make_rss_counter(level):
    '''
    '''
    if level == "inner":
        regular_year_count = list(range(5,366)) + list(range(1,5))
        day_counter = (
            list(range(90,366)) + list(range(1,5))  # 2015
            + list(range(5,367)) + list(range(1,5)) # 2016 (leap year)
            + regular_year_count # 2017 
            + regular_year_count # 2018
            + regular_year_count # 2019
            + list(range(5,367)) + list(range(1,5)) # 2020 (leap year)
            + list(range(5,122)) # 2021 (current year)
        )
    elif level == "outer":
        regular_year_count = list(range(1,366))
        day_counter = (
            list(range(86,366))  # 2015
            + list(range(1,367)) # 2016 (leap year)
            + regular_year_count # 2017 
            + regular_year_count # 2018
            + regular_year_count # 2019
            + list(range(1,367)) # 2020 (leap year)
            + list(range(1,118)) # 2021 (current year)
        )
    return day_counter 

def _return_rss_dates():
    '''
    '''
    daily_dates = pd.date_range("2015-03-31", "2021-05-01", freq='D')
    daily_dates = [str(daily_dates[i])[:4] for i in range(len(daily_dates))]

    counter = _make_rss_counter("inner")
    assert len(daily_dates) == len(counter), "Length mismatch!"
    daily_dates = [daily_dates[n] + f"{counter[n]:03}" for n in range(len(daily_dates))]
    
    monthly_dates = pd.date_range("2015-04", "2021-04", freq='M')
    monthly_dates = [str(monthly_dates[i])[:8].replace('-','') for i in range(len(monthly_dates))]
    
    return daily_dates, monthly_dates

def _create_rss_counter_dict():
    '''
    '''
    daily_dates, _ = _return_rss_dates()
    
    counter_list = _make_rss_counter("outer")
    counter_list = [f"{n:03}" for n in counter_list]
    assert len(counter_list)==len(daily_dates), "Length mismatch!"
    return {daily_dates[i]:counter_list[i] for i in range(len(counter_list))}


# -

counter_dict = {
    'JPL' : _create_jpl_counter_dict(),
    'RSS' : _create_rss_counter_dict(),
}


def make_full_path(algorithm, freq, date, counter_dict=counter_dict):
    '''
    Parameters
    ----------
    algorithm : str
        One of 'JPL' or 'RSS'
    freq : str
        One of '8day_running' or 'monthly'
    date : str
        if freq == '8day_running': 
            and algorithm == 'JPL': 8-character numerical string of format YYYYMMDD
            and algorithm == 'RSS': 7-character numerical string of format YYYYDDD
        else: 6-character numerical string of format YYYYMM
    counter_dict : dict
        A dict of dicts.
    '''    
    assert algorithm in ('JPL', 'RSS'), "algorithm must be one of 'JPL' or 'RSS'"
    assert freq in ('8day_running', 'monthly'), "freq must be one of '8day_running' or 'monthly'"
    
    if freq == '8day_running':
        if algorithm == 'JPL':
            assert len(date) == 8, "if freq=='8day_running' and algo=='JPL', len(date) must == 8"
        elif algorithm == 'RSS':
            assert len(date) == 7, "if freq=='8day_running' and algo=='RSS', len(date) must == 7"
        count = counter_dict[algorithm][date]
    else:
        assert len(date) == 6, 'if freq==\'monthly\', len(date) must == 6'
        count = ''
    
    url_base = 'https://podaac-opendap.jpl.nasa.gov/opendap/allData/smap/L3/'
    yr = date[:4]
    
    if algorithm == 'JPL':
        mo = date[4:6]
        day = date[6:8] if len(date) == 8 else None
        freq_caps = '8DAYS' if freq == '8day_running' else 'MONTHLY'
        file_date = f'{yr}{mo}{day}' if freq == '8day_running' else f'{yr}{mo}'
        url_tail = f'JPL/V5.0/{freq}/{yr}/{count}/SMAP_L3_SSS_{file_date}_{freq_caps}_V5.0.nc'
    else:
        if len(date) == 7:
            seq_day = date[4:7]
            assert int(seq_day) in range(1,367), 'Sequential day must be in range(1,367)'
            date_spec = seq_day
        else:
            month = date[4:6]
            assert int(month) in range(1,13), 'Month must be in range(1,13)'
            date_spec = month
        url_tail = f'RSS/V4/{freq}/SCI/{yr}/{count}/RSS_smap_SSS_L3_{freq}_{yr}_{date_spec}_FNL_v04.0.nc'

    return url_base + url_tail


# +
jpl_daily_dates, jpl_monthly_dates = _return_jpl_dates()
rss_daily_dates, rss_monthly_dates = _return_rss_dates()

all_urls = {
    'jpl_eightday': [make_full_path('JPL', '8day_running', date=d) for d in jpl_daily_dates],
    'jpl_monthly': [make_full_path('JPL', 'monthly', date=d) for d in jpl_monthly_dates],
    'rss_eightday': [make_full_path('RSS', '8day_running', date=d) for d in rss_daily_dates],
    'rss_monthly': [make_full_path('RSS', 'monthly', date=d) for d in rss_monthly_dates],
}

# +
import xarray as xr

for store in list(all_urls):
    print(f"{store} contains {len(all_urls[store])} urls.")
    print(f"""
    The coordinates of the first and last source files in this store are:
      {xr.open_dataset(all_urls[store][0]).coords}
      {xr.open_dataset(all_urls[store][-1]).coords}
    """)
# -


