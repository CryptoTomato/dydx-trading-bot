import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
from datetime import datetime
from constants import MAX_HALF_LIFE, MAX_PVALUE, MIN_ZERO_CROSSING, WINDOW

# Calculate Half Life
# https://www.pythonforfinance.net/2016/05/09/python-backtesting-mean-reversion-part-2/

def calculate_half_life(spread):
    df_spread = pd.DataFrame(spread, columns=["spread"])
    spread_lag = df_spread.spread.shift(1)
    spread_lag.iloc[0] = spread_lag.iloc[1]
    spread_ret = df_spread.spread - spread_lag
    spread_ret.iloc[0] = spread_ret.iloc[1]
    spread_lag2 = sm.add_constant(spread_lag)
    model = sm.OLS(spread_ret, spread_lag2)
    res = model.fit()
    halflife = round(-np.log(2) / res.params[1], 0)
    return halflife



# Calculate ZScore
def calculate_zscore(spread):
    spread_series = pd.Series(spread)
    mean = spread_series.rolling(center=False, window=WINDOW).mean()
    std = spread_series.rolling(center=False, window=WINDOW).std()
    x = spread_series.rolling(center=False, window=1).mean()
    zscore = (x - mean) / std
    return zscore

# Calculate Cointegration
def calculate_cointegration(series_1, series_2):
    series_1 = np.array(series_1).astype(np.float)
    series_2 = np.array(series_2).astype(np.float)
    coint_flag = 0
    coint_res = coint(series_1, series_2)
    coint_t = coint_res[0]
    p_value = coint_res[1]
    critical_value = coint_res[2][1]
    model = sm.OLS(series_1, series_2).fit()
    hedge_ratio = model.params[0]
    spread = series_1 - (hedge_ratio * series_2)
    z_score = calculate_zscore(spread).values.tolist()
    zero_crossing = 0
    for i in range(299,len(z_score)):#299 -> use only last 100 candlesticks (100 hours)
        if (i==0) or (i==len(z_score)-1):
            pass
        else:
            sign = np.sign(z_score[i])
            lag_sign = np.sign(z_score[i-1])
            if (sign==1 and lag_sign==-1) or (sign==-1 and lag_sign==1):
                zero_crossing+=1
    half_life = calculate_half_life(spread)
    t_check = coint_t < critical_value  
    coint_flag = 1 if p_value < MAX_PVALUE and t_check else 0 
    return coint_flag, p_value, hedge_ratio, half_life, zero_crossing 


# Store Cointegration Results 
def store_cointegration_results(df_market_prices):

    # Initialize
    markets = df_market_prices.columns.to_list()
    criteria_met_pairs = []

    # Find cointegrated pairs
    # Start with our base pair
    for index, base_market in enumerate(markets[:-1]):
        series_1 = df_market_prices[base_market].values.astype(float).tolist()

        # Get Quote Pair
        for quote_market in markets[index + 1:]:
            series_2 = df_market_prices[quote_market].values.astype(float).tolist()

            # Check cointegration 
            coint_flag, p_value, hedge_ratio, half_life, zero_crossing = calculate_cointegration(series_1, series_2)

            # Log pair 
            if coint_flag == 1 and half_life <= MAX_HALF_LIFE and half_life > 0 and zero_crossing >= MIN_ZERO_CROSSING:
                criteria_met_pairs.append({
                    "base_market": base_market,
                    "quote_market": quote_market,
                    "p_value": p_value,
                    "hedge_ratio": hedge_ratio,
                    "half_life": half_life,
                    "zero_crossing": zero_crossing
                })

    # Create and save DataFrame
    df_criteria_met = pd.DataFrame(criteria_met_pairs)
    df_criteria_met.to_csv("cointegrated_pairs.csv",index=False)
    del df_criteria_met

    #Save date of cointegration calculation
    calc_time = datetime.now()
    coint_calc_time = {"date" : [calc_time]}
    df_coint_calc_time  = pd.DataFrame(coint_calc_time)
    df_coint_calc_time.to_csv("coint_calc_time.csv",index=False)

    # Return result
    print("Cointegrated pairs successfully saved")
    return "saved"



