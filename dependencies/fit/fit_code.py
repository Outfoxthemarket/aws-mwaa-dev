import os
from datetime import timedelta
import pandas as pd


def download_raw_data(ti):
    base_path = "/usr/local/airflow/dependencies/fit/data"
    # Create a folder for the quarter
    quarter = {
        4: "Q4",  # During April we submit the Q4 report for the previous year
        7: "Q1",  # During July we submit the Q1 report for the current year
        10: "Q2",  # During October we submit the Q2 report for the current year
        1: "Q3",  # During January we submit the Q3 report for the current year
    }
    # assert datetime.now().month in quarter.keys(), "Month is not in the quarter dictionary, the workflow should not be running"
    # if quarter[datetime.now().month] == 'Q4':
    #     year = f'Y{datetime.now().year - 2010}'  # During April we submit the Q4 report for the previous year (2023 Q4 --> Y13)
    # else:
    #     year = f'Y{datetime.now().year - 2009}'  # During July, October and January we submit the report for the current year
    # PATH = f"/{BASE_PATH}/{year}/{quarter[datetime.now().month]}"
    # # Create a folder for the quarter
    # if not os.path.exists(PATH):
    #     print("Quarter folder created - ", PATH)
    #     os.makedirs(PATH)
    # else:
    #     print("Quarter folder already exists - ", PATH)
    # Download csv files from quicksight into the folder
    print("Data has been downloaded into the folder")
    # Push the path to the csv files to the XCom
    dummy = "/usr/local/airflow/dependencies/fit/data/Y13/Q4"
    ti.xcom_push(key="CSV_PATH", value=dummy)
    print("[XCOM Push] - CSV_PATH: /usr/local/airflow/dependencies/fit/data/Y13/Q4")


def merge_check_sum(ti):
    # Get the path to the csv files from the XCom
    csv_path = ti.xcom_pull(key="CSV_PATH", task_ids="Download_CVS_Quicksight")
    print("[XCOM Pull] - CSV_PATH: ", csv_path)
    print("Files in the folder: ", os.listdir(csv_path))
    # Open the both csv files and merge them into a single dataframe
    assert len(os.listdir(csv_path)) == 2, "There should only be two csv files in the folder"
    df1 = pd.read_csv(os.path.join(csv_path, os.listdir(csv_path)[0]))
    df2 = pd.read_csv(os.path.join(csv_path, os.listdir(csv_path)[1]))
    assert len(df1) == len(df2), "The two dataframes do not have the same number of rows"
    if len(df1.columns) > len(df2.columns):  # Make sure we merge the dataframe with the columns in order
        df = pd.merge(df1, df2, on=['settlement_date', 'settlement_code'])
    else:
        df = pd.merge(df2, df1, on=['settlement_date', 'settlement_code'])
    assert len(df) == len(
        df1), "The merged dataframe does not have the same number of rows as the two original dataframes"
    # Go over 2 size rolling windows to find the day we start getting only SF data
    flag = True
    for window in df.shift(1).rolling(window=2, step=2, min_periods=0):  # Shift by 1 so windows start on the same day
        if flag:  # Skip the first row since it is NaN
            flag = False
            continue
        if window.iloc[0]["settlement_code"] == window.iloc[1]["settlement_code"]:
            print("Break date - ", window.iloc[0]["settlement_date"])
            ti.xcom_push(key="BREAK_DATE", value=window.iloc[0]["settlement_date"])
            break
        assert window.iloc[0]["settlement_date"] == window.iloc[1]["settlement_date"], "Settlement dates are not equal"
    # Calculate the sum of all the profiled consumptions
    df['total_consumption'] = df.filter(regex='profiled_spm_consumption_sp*').sum(axis=1)
    df.drop(df.tail(1).index, inplace=True)  # Drop last row since it is NaN (should fix this)
    assert len(df1) == len(df2), "The two dataframes do not have the same number of rows"
    # Write the merged dataframe to a csv file
    df.to_csv(os.path.join(csv_path, "merged.csv"), index=False)
    print("Merged file has been saved")


def forecast_missing_sf(ti):
    # Read the merged csv file
    df = pd.read_csv(os.path.join(ti.xcom_pull(key="CSV_PATH", task_ids="Download_CVS_Quicksight"), "merged.csv"))
    print("Merged.csv file has been read")
    # Format the settlement_date columns into datetime
    df['settlement_date'] = pd.to_datetime(df['settlement_date'], format='%d/%m/%Y %H:%M')
    # Forecast the total consumption for the missing days of the last month using sf data
    print('Xcom pull - ', ti.xcom_pull(key="BREAK_DATE", task_ids="Merge_Check_Sum"))
    mean_only_sf = df.loc[
        df['settlement_date'] >= pd.to_datetime(ti.xcom_pull(key="BREAK_DATE", task_ids="Merge_Check_Sum"),
                                                format='%d/%m/%Y %H:%S'), 'total_consumption'].mean()
    print("Mean amount for SF only days - ", mean_only_sf)
    # Forecast the missing days
    print("Last day of actual data - ", df.loc[len(df) - 1, 'settlement_date'])
    last_day = df.loc[len(df) - 1, 'settlement_date'].to_pydatetime()
    flag = last_day
    while (flag + timedelta(days=1)).month == last_day.month:
        flag += timedelta(days=1)
        print('Forecasting row - ', flag)
        df = pd.concat([df, pd.DataFrame(
            {'settlement_date': [pd.to_datetime(flag, infer_datetime_format=True)], 'settlement_code': ['Forecasted'],
             'total_consumption': [mean_only_sf]})], ignore_index=True)
    # Write the merged dataframe to a csv file
    df.to_csv(os.path.join(ti.xcom_pull(key="CSV_PATH", task_ids="Download_CVS_Quicksight"), "merged.csv"), index=False,
              date_format='%d/%m/%Y %H:%M')
    print("Merged file has been saved")


def generate_fit_results(ti):
    # Read the merged csv file
    df = pd.read_csv(os.path.join(ti.xcom_pull(key="CSV_PATH", task_ids="Download_CVS_Quicksight"), "merged.csv"))
    print("Merged.csv file has been read")
    # Format the settlement_date columns into datetime
    df['settlement_date'] = pd.to_datetime(df['settlement_date'], format='%d/%m/%Y %H:%M')
    # Create pivot table hat shows row data by month and on the columns show the settlement code RF, SF and Total
    df_pivot = pd.pivot_table(df.reset_index(), index=df['settlement_date'].dt.month_name(), columns='settlement_code',
                              values='total_consumption', aggfunc='sum', fill_value=0)
    # Create a column that takes the either the RF or SF value depending on wheter the Forecast column is 0 or not. If forecast is 0 then use SF, else use R1
    df_pivot['Amount'] = df_pivot.apply(lambda x: x['R1'] if x['Forecasted'] == 0 else x['Forecasted'] + x['SF'],
                                        axis=1)
    df_pivot.loc['Total'] = df_pivot.sum()
    # Print the total amount figure
    print("Total amount - ", df_pivot.loc['Total', 'Amount'])
    # Write fit report to excel file
    df_pivot.to_excel(os.path.join(ti.xcom_pull(key="CSV_PATH", task_ids="Download_CVS_Quicksight"), "fit_results.xlsx"))
    print("Fit result report file has been saved")
