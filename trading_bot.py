import pandas as pd
import numpy as np
from datetime import date
from scipy.stats import linregress
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models, expected_returns
from pypfopt.discrete_allocation import DiscreteAllocation

def trading_bot(event, context):
    # get historical NASDAQ data
    # sort by index to ensure data is in correct order
    historical_query = """
        SELECT *
        FROM `oval-bot-232220.algorithmic_trader.daily_stock_data`
    """
    stocks = pd.read_gbq(historical_query).sort_values(by='index').reset_index(drop=True)

    # obtain symbols and most recent prices
    latest_prices = stocks.iloc[0]
    symbols = stocks.columns[1:].to_list()

    # get portfolio and sort by date to ensure data is in correct order
    portfolio_query = """
        SELECT *
        FROM `oval-bot-232220.algorithmic_trader.portfolio`
    """
    pf_history = pd.read_gbq(portfolio_query).sort_values(by='Date').reset_index(drop=True)

    # number of stocks to purchase
    pf_size = 10

    # obtain most recent portfolio and calculate its current value
    prev_pf = pf_history.iloc[-1]
    prev_symb = [prev_pf['stock{}'.format(stock_ind)] for stock_ind in range(pf_size)]
    prev_bought = [prev_pf['stock{}Bought'.format(stock_ind)] for stock_ind in range(pf_size)]
    pf_value = sum([latest_prices[prev_symb[i]]*prev_bought[i] for i in range(pf_size)]) + float(prev_pf['Unallocated'])

    # momentum score
    def momentum(closes):
        returns = np.log(closes)
        x = np.arange(len(returns))
        slope, _, rvalue, _, _ = linregress(x, returns)
        return ((1 + slope) ** 252) * (rvalue ** 2)  # annualize slope and multiply by R^2

    # get momentum scores
    momentums = pd.DataFrame()
    for symb in symbols:
        momentums[symb] = stocks[symb].rolling(90).apply(momentum, raw=False)

    # select best momentum scores
    bests = momentums.max().sort_values(ascending=False).index[:pf_size]

    # get price history, expected returns and covariance for stocks with best momentum scores
    best_prices = stocks[bests]
    best_latest = latest_prices[bests]
    mu = expected_returns.mean_historical_return(best_prices)
    S = risk_models.sample_cov(best_prices)

    # use sharpe ratio to obtain portfolio allocation weights
    ef = EfficientFrontier(mu, S, gamma=1) # Use regularization (gamma=1)
    ef.max_sharpe()
    cleaned_weights = ef.clean_weights()

    # allocate money in portfolio using weights 
    da = DiscreteAllocation(cleaned_weights, best_latest, total_portfolio_value=pf_value)
    allocation, unallocated = da.lp_portfolio()

    # create df to store info about purchased
    pf = {'Date': date.today().strftime("%Y-%m-%d"), 'Value': pf_value, 'Unallocated': unallocated}
    for stock_ind in range(pf_size):
        pf['stock{}'.format(stock_ind)] = bests[stock_ind]
        pf['stock{}Bought'.format(stock_ind)] = allocation.get(bests[stock_ind], 0)
    pf = pd.DataFrame(pf, index=[0])

    # add new portfolio to current portfolio data and upload to gbq
    pf_data = pd.concat([pf_history, pf], ignore_index=True)

    pf_data.to_gbq(destination_table='algorithmic_trader.portfolio', 
            project_id="oval-bot-232220",
            if_exists='replace')

print(trading_bot(0, 0))