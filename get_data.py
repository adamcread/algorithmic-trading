import requests
import pandas as pd
import time
from datetime import datetime
from bs4 import BeautifulSoup

def daily_stock_data(event, context):
    today = datetime.today().strftime('%Y-%m-%d')
    request = requests.get(
        url='https://api.tdameritrade.com/v1/marketdata/EQUITY/hours',
        params={
            'apikey': 'TWTKKV11WUTCWP2HCJHYHF6DF0GFCSJ5',
            'date': today
        }
    ).json()

    try:
        if request['equity']['EQ']['isOpen']:            
            # make request for the webpage information and obtain the html for the nasdaq stock table
            symbol_page = requests.get('https://www.stockmonitor.com/nasdaq-stocks/')
            symbol_soup = BeautifulSoup(symbol_page.content, 'html.parser')
            symbol_table = symbol_soup.find('table')

            # obtain tickers from webpage table
            symbols = [tr.find('a').text for tr in symbol_table.find('tbody').find_all('tr')]

            historical_data = pd.DataFrame()
            for symb in symbols:
                print(symb)
                historical_page = requests.get('https://finance.yahoo.com/quote/{}/history'.format(symb))
                historical_soup = BeautifulSoup(historical_page.content, 'html.parser')
                historical_table = historical_soup.find('table', {'data-test': 'historical-prices'}).find('tbody')

                symb_historical = pd.Series([tr.find_all('span')[1].text.replace(',', '') for tr in historical_table.find_all('tr')])
                symb_historical = symb_historical.apply(pd.to_numeric, errors='coerce')

                historical_data['{}Open'.format(symb)] = symb_historical.dropna().reset_index(drop=True)

            # upload historical nasdaq info to gbq
            historical_data.reset_index().to_gbq(destination_table='algorithmic_trader.daily_stock_data', 
                project_id="oval-bot-232220", 
                if_exists='replace')

            return "success"
        else:
            # market not open
            return "market closed"
    except KeyError:
        # not a weekday
        return "market not open today"

print(daily_stock_data("a", "b"))