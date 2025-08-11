import pandas as pd
from datetime import datetime, timedelta

def clean_twitter_data(file_path, sheet_name=None):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found. Please ensure the file name is correct.")
        return None
    except Exception as e:
        print(f"An error occurred while reading the Excel file: {e}")
        return None

    time_col = None
    for col in ['time', 'Time']:
        if col in df.columns:
            time_col = col
            break

    tweet_col = None
    for col in ['Tweet', 'TweetText']:
        if col in df.columns:
            tweet_col = col
            break

    if not time_col or not tweet_col:
        print("Error: The Excel file is missing the required 'time' and/or 'Tweet' columns.")
        print(f"Detected columns: {list(df.columns)}")
        print("Please check the column headers in your file and ensure they are present.")
        return None
    
    df.rename(columns={time_col: 'time', tweet_col: 'Tweet'}, inplace=True)
    

    df.dropna(subset=['time', 'Tweet'], inplace=True)
    
    df['time_utc'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M %z', errors='coerce')
    if df['time_utc'].isnull().any():
        print("Warning: Some 'time' values could not be converted. Check the format.")
        df.dropna(subset=['time_utc'], inplace=True)
    ist_offset = timedelta(hours=5, minutes=30)
    df['time_ist'] = df['time_utc'].apply(lambda x: x + ist_offset)
    
    df['date_ist'] = df['time_ist'].dt.date
    df['hour_ist'] = df['time_ist'].dt.hour
    df['day_of_week'] = df['time_ist'].dt.day_name()
    df['day_of_month'] = df['time_ist'].dt.day
    df['TweetWordCount'] = df['Tweet'].apply(lambda x: len(str(x).split()) if pd.notna(x) else 0)
    df['TweetCharCount'] = df['Tweet'].apply(lambda x: len(str(x)) if pd.notna(x) else 0)

    def categorize_tweet(row):
        if row.get('media views', 0) > 0:
            return 'Media'
        elif row.get('url clicks', 0) > 0:
            return 'Links'
        elif row.get('hashtag clicks', 0) > 0:
            return 'Hashtags'
        else:
            return 'Other'

    df['TweetCategory'] = df.apply(categorize_tweet, axis=1)

    df['user_profile_id'] = df['id']


    final_columns = df.columns.tolist() + [
        'time_utc', 'time_ist', 'date_ist', 'hour_ist', 'day_of_week', 'day_of_month',
        'TweetWordCount', 'TweetCharCount', 'TweetCategory', 'user_profile_id'
    ]

    final_df = df.reindex(columns=final_columns)

    return final_df

file_to_clean = "Tweet.xlsx"
sheet_name_to_use = "SocialMedia (1)"

final_df = clean_twitter_data(file_to_clean, sheet_name=sheet_name_to_use)

if final_df is not None:
    output_file_name = "final_cleaned_data_from_original.csv"
    final_df.to_csv(output_file_name, index=False)
    print(f"\nSuccessfully created and saved the cleaned data to '{output_file_name}'")
