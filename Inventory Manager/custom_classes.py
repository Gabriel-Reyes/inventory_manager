import pandas as pd
import numpy as np


class cash_flow:
    def __init__(self, data):
        self.df = data
        self.df['Due Date'] = pd.to_datetime(self.df['Due Date'])
        self.df['Year'] = self.df['Due Date'].dt.year
        self.df['Month'] = self.df['Due Date'].dt.month

    def monthly(self):
        m_delta_3 = (pd.Timestamp.today() + pd.tseries.offsets.DateOffset(months=-3)).to_period('M').to_timestamp()
        df = self.df
        df['segment'] = df['Due Date'].apply(lambda x: '3+ Months Ago' if x < m_delta_3 else (x.year, x.month))
        df = pd.pivot_table(df, values='Balance', columns='segment', aggfunc=np.sum)
        cols = list(df.columns)
        cols = [cols[-1]] + cols[:-1]
        df = df[cols]
        return df

    def expanded_monthly(self):
        m_delta_3 = (pd.Timestamp.today() + pd.tseries.offsets.DateOffset(months=-3)).to_period('M').to_timestamp()
        df = self.df
        df['segment'] = df['Due Date'].apply(lambda x: '3+ Months Ago' if x < m_delta_3 else (x.year, x.month))
        df = pd.pivot_table(df, values='Balance', index='Account: BP Name', columns='segment', aggfunc=np.sum)
        cols = list(df.columns)
        cols = [cols[-1]] + cols[:-1]
        df = df[cols]
        return df


class payables(cash_flow):
    def __init__(self, data):
        super().__init__(data)
        fx_eur_usd = 1.20
        self.df['Balance'] = np.where(self.df['Doc Currency'] == '$', self.df['Total'] - self.df['Amount Paid'],
                                    ((self.df['Total'] - self.df['Amount Paid'])/self.df['Doc Rate']) * fx_eur_usd).round(2)


class receivables(cash_flow):
    def __init__(self, data):
        super().__init__(data)
        self.df = self.df[self.df['Balance'] > 0]