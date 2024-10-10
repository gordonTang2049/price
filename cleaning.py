import textwrap as tw
import pandas as pd
import yfinance as yf

# if DB has 

class Cleaning:
    def __init__(self):
        return

    def yf_df_date(
        self, 
        ticker : str,
        start_date : str):
        
        try:
            ticker_data = yf.Ticker(ticker)

            hist = ticker_data.history(start=start_date, end=None).reset_index()
            
            # if dataframe is not empty
            if hist.empty == False: 
                hist['TICKER'] = ticker

                hist['SharePrice_id'] = hist.Date.dt.strftime('%Y%m%d') + '_' + hist.TICKER

                hist = hist[['SharePrice_id', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'TICKER']]
                
                columns_to_round = ['Open', 'High', 'Low', 'Close']

                hist[columns_to_round] = hist[columns_to_round].apply(lambda x: round(x, 4))

                return (True, hist.dropna()) 
                
            else:
                return (False, None) 
        except Exception as e:

            print('Error --> ', ticker, e)

            return (False, None)

    def yf_df_day(
        self, 
        ticker : str,
        period : str):
        
        try:
            ticker_data = yf.Ticker(ticker)
            hist = ticker_data.history(period=period).reset_index()
            
            # if dataframe is not empty
            if hist.empty == False: 
                hist['TICKER'] = ticker

                hist['SharePrice_id'] = hist.Date.dt.strftime('%Y%m%d') + '_' + hist.TICKER

                hist = hist[['SharePrice_id', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'TICKER']]
                
                columns_to_round = ['Open', 'High', 'Low', 'Close']

                hist[columns_to_round] = hist[columns_to_round].apply(lambda x: round(x, 4))

                return (True, hist.dropna())
                
            else:
                return (False, None)

        except Exception as e:
            
            print('Error --> ', ticker, e)

            return (False, None)

    