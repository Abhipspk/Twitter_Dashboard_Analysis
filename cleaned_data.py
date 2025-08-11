import pandas as pd
from datetime import datetime, timedelta

def clean_twitter_data(file_path, sheet_name=None):
    """
    Cleans and processes raw Twitter data from an Excel file (.xlsx)
    to prepare it for the specified Power BI tasks.

    Args:
        file_path (str): The path to the raw Excel file.
        sheet_name (str, optional): The name of the sheet to read from.
                                     Defaults to None, which reads the first sheet.

    Returns:
        pandas.DataFrame: A cleaned and enhanced DataFrame with new columns.
    """
    try:
        # Load the original Excel file using pandas' read_excel function.
        # This requires the 'openpyxl' library to be installed.
        # Run: pip install openpyxl
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found. Please ensure the file name is correct.")
        return None
    except Exception as e:
        print(f"An error occurred while reading the Excel file: {e}")
        return None

    # --- Standardize Column Names for Robustness ---
    # We will first check for and standardize column names like 'time' and 'Tweet'
    # to handle capitalization or other minor variations.
    
    # Identify the correct 'time' column
    time_col = None
    for col in ['time', 'Time']:
        if col in df.columns:
            time_col = col
            break
    
    # Identify the correct 'Tweet' column
    tweet_col = None
    for col in ['Tweet', 'TweetText']:
        if col in df.columns:
            tweet_col = col
            break

    # If the required columns are not found, print an informative error
    if not time_col or not tweet_col:
        print("Error: The Excel file is missing the required 'time' and/or 'Tweet' columns.")
        print(f"Detected columns: {list(df.columns)}")
        print("Please check the column headers in your file and ensure they are present.")
        return None
    
    # Rename the columns to the standardized names for the rest of the script
    df.rename(columns={time_col: 'time', tweet_col: 'Tweet'}, inplace=True)

    # --- Data Type and Time Zone Cleaning ---
    
    # Drop rows with NaN in critical columns to ensure clean processing
    df.dropna(subset=['time', 'Tweet'], inplace=True)
    
    # Convert 'time' column to datetime objects. The format is '%Y-%m-%d %H:%M %z'.
    df['time_utc'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M %z', errors='coerce')
    
    # Check if the conversion was successful.
    if df['time_utc'].isnull().any():
        print("Warning: Some 'time' values could not be converted. Check the format.")
        df.dropna(subset=['time_utc'], inplace=True)

    # Convert UTC time to IST (Indian Standard Time, which is UTC+5:30)
    ist_offset = timedelta(hours=5, minutes=30)
    df['time_ist'] = df['time_utc'].apply(lambda x: x + ist_offset)
    
    # Create separate date, hour, and day-of-week columns from the IST time
    df['date_ist'] = df['time_ist'].dt.date
    df['hour_ist'] = df['time_ist'].dt.hour
    df['day_of_week'] = df['time_ist'].dt.day_name()
    # The day_of_month is needed for odd/even day filters
    df['day_of_month'] = df['time_ist'].dt.day

    # --- Feature Engineering for Tasks ---

    # Calculate Tweet Word Count
    df['TweetWordCount'] = df['Tweet'].apply(lambda x: len(str(x).split()) if pd.notna(x) else 0)

    # Calculate Tweet Character Count
    df['TweetCharCount'] = df['Tweet'].apply(lambda x: len(str(x)) if pd.notna(x) else 0)

    # Create a 'Tweet Category' column for Task 5
    def categorize_tweet(row):
        if row.get('media views', 0) > 0:
            return 'Media'
        elif row.get('url clicks', 0) > 0:
            return 'Links'
        elif row.get('hashtag clicks', 0) > 0:
            return 'Hashtags'
        else:
            return 'Other'

    # Apply the function to create the new 'TweetCategory' column
    df['TweetCategory'] = df.apply(categorize_tweet, axis=1)

    # For Task 6, a 'user profile' is needed. We'll use the 'id' column as a unique identifier.
    df['user_profile_id'] = df['id']

    # --- Final Column Selection and Reordering ---

    final_columns = df.columns.tolist() + [
        'time_utc', 'time_ist', 'date_ist', 'hour_ist', 'day_of_week', 'day_of_month',
        'TweetWordCount', 'TweetCharCount', 'TweetCategory', 'user_profile_id'
    ]

    # Drop any duplicate columns and keep all the ones we need
    final_df = df.reindex(columns=final_columns)

    return final_df

# --- Main script execution ---
# This path should point to the original, raw Excel file.
# We also specify the sheet name, which is "SocialMedia (1)" based on the file name.
file_to_clean = "Tweet.xlsx"
sheet_name_to_use = "SocialMedia (1)"

final_df = clean_twitter_data(file_to_clean, sheet_name=sheet_name_to_use)

if final_df is not None:
    output_file_name = "final_cleaned_data_from_original.csv"
    final_df.to_csv(output_file_name, index=False)
    print(f"\nSuccessfully created and saved the cleaned data to '{output_file_name}'")
