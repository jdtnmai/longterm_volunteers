from datetime import datetime
import pandas as pd
import re


def parse_time(input_string):
    # Adjust the regex to account for cases with or without minutes

    # Match hours and minutes, allowing for the absence of hours
    match = re.search(r'(?:(\d+)\s*valand(?:os)?)?\s*(?:(\d+)\s*minut(?:ė|ės)?)?(?:\s*(\d+)\s*sekund(?:ė|žių)?)?', input_string)

    if match:
        # Extract hours, minutes, and seconds, defaulting to 0 if not found
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0

        # Convert total time to minutes (1 hour = 60 minutes, seconds are converted to minutes)
        total_minutes = hours * 60 + minutes
        return total_minutes

    return -1  # Return 0 if no match is found

def fill_in_gaps(temp):
    temp.set_index('date', inplace=True)

    # Group by date and unique_code, then sum the minutes
    temp_grouped = temp.groupby(['date', 'unique_code']).sum()

    # Create a full date range
    full_date_range = pd.date_range(start=temp_grouped.index.get_level_values(0).min(), 
                                    end=temp_grouped.index.get_level_values(0).max(), 
                                    freq='ME')

    # Create a MultiIndex from full date range and unique codes
    multi_index = pd.MultiIndex.from_product([full_date_range, temp_grouped.index.get_level_values(1).unique()], 
                                            names=['date', 'unique_code'])

    # Reindex to include all dates and unique codes, filling missing values with 0
    temp_reindexed = temp_grouped.reindex(multi_index, fill_value=0)

    temp = temp_reindexed.reset_index()
    return temp

def get_rolling_minutes(temp):
    # Group by month and unique_code, then sum minutes
    monthly_minutes = temp.groupby([pd.Grouper(key='date', freq='ME'), 'unique_code'])['minutes'].sum().reset_index()

    # Rename columns for clarity
    monthly_minutes.columns = ['month', 'unique_code', 'total_minutes']

    # Sort the DataFrame by month
    monthly_minutes.sort_values('month', inplace=True)

    # Calculate the rolling sum over a 6-month window
    monthly_minutes['rolling_sum'] = monthly_minutes.groupby('unique_code')['total_minutes'].transform(lambda x: x.rolling(window=6, min_periods=1).sum())

    # Check for sums greater than 1800 minutes
    monthly_minutes['exceeds_1800'] = monthly_minutes['rolling_sum'] > 1800
    return monthly_minutes

def fix_data(df):
    df.date= pd.to_datetime(df.date)
    df['minutes'] = df.updated_at.map(lambda x: parse_time(x))
    df.unique_code = df.unique_code.map(str)
    return df

def enrich_monthly_mintutes(monthly_minutes, df):
    first_time_longterm = monthly_minutes[(monthly_minutes.exceeds_1800==True)].groupby(['unique_code'],as_index=False).month.min()
    first_time_longterm.columns = ['unique_code','first_month_longterm']
    monthly_minutes = monthly_minutes.merge(first_time_longterm, on='unique_code', how='left')
    meta_df = df[['unique_code', 'Pilnas vardas', 'miestas']].groupby('unique_code',as_index=False).last()
    monthly_minutes = monthly_minutes.merge(meta_df, on='unique_code', how='left')
    return monthly_minutes


def write_to_excel(monthly_minutes):
    min_month = str(monthly_minutes.month.min())[:7]
    max_month = str(monthly_minutes.month.max())[:7]
    today = datetime.now()
    today_string = today.strftime('%Y-%m-%d')

    file_name = f'savanoriai_{min_month}-{max_month}_sugeneruota_{today_string}.xlsx'
    with pd.ExcelWriter(f'output/{file_name}') as writer:
        monthly_minutes[(monthly_minutes.exceeds_1800==True) 
                        & (monthly_minutes.month==monthly_minutes.month.max()) 
                        & (monthly_minutes.total_minutes>0)].to_excel(writer, sheet_name='work', index=False)
        monthly_minutes[(monthly_minutes.exceeds_1800==True) 
                        & (monthly_minutes.month==monthly_minutes.month.max()) 
                        & (monthly_minutes.total_minutes==0)].to_excel(writer, sheet_name='did_not_work', index=False)
        monthly_minutes.to_excel(writer, sheet_name='all', index=False)


if __name__=="__main__":
    import os
    files = os.listdir('data')
    df = pd.concat(    [pd.read_excel(f'data/{i}') for i in files], ignore_index=True )
    df = df.drop_duplicates()
    df = fix_data(df)
    df.set_index('date', inplace=True)
    temp = df[['unique_code','minutes']].groupby([pd.Grouper(freq='ME'), 'unique_code']).sum().reset_index()
    temp = fill_in_gaps(temp)
    monthly_minutes = get_rolling_minutes(temp)
    monthly_minutes = enrich_monthly_mintutes(monthly_minutes, df)
    write_to_excel(monthly_minutes)
    
    
