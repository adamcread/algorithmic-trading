import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

def daily_stock_data(event, context):
    # make request for the webpage information and obtain the html for the nasdaq stock table
    symbol_page = requests.get('https://www.stockmonitor.com/nasdaq-stocks/')
    symbol_soup = BeautifulSoup(symbol_page.content, 'html.parser')
    symbol_table = symbol_soup.find('table')

    # obtain NASDAQ tickers from webpage table
    symbols = [tr.find('a').text for tr in symbol_table.find('tbody').find_all('tr')]

    historical_data = pd.DataFrame()
    for symb in symbols:
        # make request for stock historical data
        historical_page = requests.get('https://finance.yahoo.com/quote/{}/history'.format(symb))
        historical_soup = BeautifulSoup(historical_page.content, 'html.parser')
        historical_table = historical_soup.find('table', {'data-test': 'historical-prices'}).find('tbody')


        # store close data for stocks
        symb_historical = []
        for tr in historical_table.find_all('tr'):
            # if dividend row there will be an error so instead add null data
            spans = tr.find_all('span')
            symb_historical.append(float(spans[4].text.replace(',', '')) if len(spans)>=4 else np.nan)

        # remove null datapoints and reverse data order so it is oldest to newest
        # effectively moving null points to bottom row
        historical_data[symb] = pd.Series(symb_historical).dropna()[::-1].reset_index(drop=True)

    # upload historical nasdaq info to gbq
    historical_data.reset_index().to_gbq(destination_table='algorithmic_trader.daily_stock_data', 
        project_id="oval-bot-232220",
        if_exists='replace')

print(daily_stock_data("a", "b"))