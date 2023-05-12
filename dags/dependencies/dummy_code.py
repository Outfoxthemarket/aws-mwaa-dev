import pandas as pd
import numpy as np
from dependencies import helper


def dummy():
    # generate dataframe with random historical closing prices for 10 different stocks (10 years of daily data)
    df = pd.DataFrame(np.random.randint(1, 1000, size=(252 * 10, 10)), columns=list('ABCDEFGHIJ'))
    df.index = pd.date_range(start='1/1/2010', end='12/31/2019', periods=len(df))
    df.index.name = 'Date'
    print(df.iloc[0, 0])
    helper.write_df_s3_csv(df, 'data/dummy.csv')
    return 999


def dummy2(ti):
    df = helper.read_df_s3_csv('data/dummy.csv')
    # print the first column of the first row
    print(df.iloc[0, 0])

