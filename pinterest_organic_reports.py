import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json

from pinterest_oauth import get_pinterest_token

load_dotenv()

PINTEREST_BASE_URL = "https://api.pinterest.com/v5"

ACCESS_TOKEN = get_pinterest_token()

def _headers():
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def get_user_account_analytics(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get high-level organic analytics at the account level.
    90-day lookback limit.
    
    Args:
        start_date: YYYY-MM-DD format
        end_date: YYYY-MM-DD format
    
    Returns:
        DataFrame with account-level organic metrics
    """
    url = f"{PINTEREST_BASE_URL}/user_account/analytics"
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "metric_types": ",".join([
            "IMPRESSION",
            "OUTBOUND_CLICK",
            "PIN_CLICK",
            "SAVE",
            "SAVE_RATE",
            "OUTBOUND_CLICK_RATE",
        ]),
        "split_field": "NO_SPLIT",  # Can be "NO_SPLIT" or other options
        "app_types": "ALL",
    }
    
    resp = requests.get(url, headers=_headers(), params=params)
    resp.raise_for_status()
    data = resp.json()
    
    # Parse the response
    records = []
    all_data = data.get("all", {})
    daily_metrics = all_data.get("daily_metrics", [])
    
    for daily in daily_metrics:
        record = {
            "date": daily.get("date"),
            "data_status": daily.get("data_status"),
        }
        
        # Extract metrics
        metrics = daily.get("metrics", {})
        for metric_name, metric_value in metrics.items():
            record[metric_name.lower()] = metric_value
        
        records.append(record)
    
    return pd.DataFrame(records)


def get_top_pins_analytics(
    start_date: str, 
    end_date: str,
    sort_by: str = "IMPRESSION",
    num_of_pins: int = 50
) -> pd.DataFrame:
    """
    Get top performing organic pins (image and video combined).
    90-day lookback limit.
    
    Args:
        start_date: YYYY-MM-DD format
        end_date: YYYY-MM-DD format
        sort_by: Metric to sort by (IMPRESSION, OUTBOUND_CLICK, PIN_CLICK, SAVE)
        num_of_pins: Number of top pins to retrieve (max 50)
    
    Returns:
        DataFrame with pin-level metrics for top pins
    """
    url = f"{PINTEREST_BASE_URL}/user_account/analytics/top_pins"
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "sort_by": sort_by,
        "metric_types": ",".join([
            "IMPRESSION",
            "OUTBOUND_CLICK",
            "PIN_CLICK",
            "SAVE",
        ]),
        "num_of_pins": num_of_pins,
    }
    
    resp = requests.get(url, headers=_headers(), params=params)
    resp.raise_for_status()
    data = resp.json()
    
    # Parse the response
    records = []
    pins = data.get("pins", [])
    
    for pin in pins:
        record = {
            "pin_id": pin.get("pin_id"),
            "data_status": pin.get("data_status"),
        }
        
        # Extract metrics
        metrics = pin.get("metrics", {})
        for metric_name, metric_value in metrics.items():
            record[metric_name.lower()] = metric_value
        
        records.append(record)
    
    return pd.DataFrame(records)


def get_top_video_pins_analytics(
    start_date: str, 
    end_date: str,
    sort_by: str = "IMPRESSION",
    num_of_pins: int = 50
) -> pd.DataFrame:
    """
    Get top performing organic video pins.
    90-day lookback limit.
    
    Args:
        start_date: YYYY-MM-DD format
        end_date: YYYY-MM-DD format
        sort_by: Metric to sort by (VIDEO_MRC_VIEW, etc.)
        num_of_pins: Number of top pins to retrieve (max 50)
    
    Returns:
        DataFrame with video pin-level metrics
    """
    url = f"{PINTEREST_BASE_URL}/user_account/analytics/top_video_pins"
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "sort_by": sort_by,
        "metric_types": ",".join([
            "IMPRESSION",
            "OUTBOUND_CLICK",
            "PIN_CLICK",
            "SAVE",
            "VIDEO_MRC_VIEW",
            "VIDEO_AVG_WATCH_TIME",
            "VIDEO_V50_WATCH_TIME",
        ]),
        "num_of_pins": num_of_pins,
    }
    
    resp = requests.get(url, headers=_headers(), params=params)
    resp.raise_for_status()
    data = resp.json()
    
    # Parse the response
    records = []
    pins = data.get("pins", [])
    
    for pin in pins:
        record = {
            "pin_id": pin.get("pin_id"),
            "data_status": pin.get("data_status"),
        }
        
        # Extract metrics
        metrics = pin.get("metrics", {})
        for metric_name, metric_value in metrics.items():
            record[metric_name.lower()] = metric_value
        
        records.append(record)
    
    return pd.DataFrame(records)


def get_pin_analytics(pin_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get detailed analytics for a single pin.
    90-day lookback limit.
    
    Args:
        pin_id: The Pinterest pin ID
        start_date: YYYY-MM-DD format
        end_date: YYYY-MM-DD format
    
    Returns:
        DataFrame with daily metrics for the pin
    """
    url = f"{PINTEREST_BASE_URL}/pins/{pin_id}/analytics"
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "metric_types": ",".join([
            "IMPRESSION",
            "OUTBOUND_CLICK",
            "PIN_CLICK",
            "SAVE",
        ]),
        "app_types": "ALL",
    }
    
    resp = requests.get(url, headers=_headers(), params=params)
    resp.raise_for_status()
    data = resp.json()
    
    # Parse the response
    records = []
    daily_metrics = data.get("daily_metrics", [])
    
    for daily in daily_metrics:
        record = {
            "pin_id": pin_id,
            "date": daily.get("date"),
            "data_status": daily.get("data_status"),
        }
        
        # Extract metrics
        metrics = daily.get("metrics", {})
        for metric_name, metric_value in metrics.items():
            record[metric_name.lower()] = metric_value
        
        records.append(record)
    
    return pd.DataFrame(records)


def get_multiple_pins_analytics(
    pin_ids: list, 
    start_date: str, 
    end_date: str
) -> pd.DataFrame:
    """
    Get analytics for multiple pins at once (up to 100 pins).
    Most performant option for bulk pin analytics.
    90-day lookback limit.
    
    Args:
        pin_ids: List of pin IDs (max 100)
        start_date: YYYY-MM-DD format
        end_date: YYYY-MM-DD format
    
    Returns:
        DataFrame with metrics for all requested pins
    """
    url = f"{PINTEREST_BASE_URL}/pins/analytics"
    
    params = {
        "pin_ids": ",".join(pin_ids[:100]),  # Max 100 pins
        "start_date": start_date,
        "end_date": end_date,
        "metric_types": ",".join([
            "IMPRESSION",
            "OUTBOUND_CLICK",
            "PIN_CLICK",
            "SAVE",
        ]),
        "app_types": "ALL",
    }
    
    resp = requests.get(url, headers=_headers(), params=params)
    resp.raise_for_status()
    data = resp.json()
    
    # Parse the response
    all_records = []
    pins_data = data.get("pins", [])
    
    for pin_data in pins_data:
        pin_id = pin_data.get("pin_id")
        daily_metrics = pin_data.get("daily_metrics", [])
        
        for daily in daily_metrics:
            record = {
                "pin_id": pin_id,
                "date": daily.get("date"),
                "data_status": daily.get("data_status"),
            }
            
            # Extract metrics
            metrics = daily.get("metrics", {})
            for metric_name, metric_value in metrics.items():
                record[metric_name.lower()] = metric_value
            
            all_records.append(record)
    
    return pd.DataFrame(all_records)


def get_date_ranges(start_date: str, end_date: str, max_days: int = 89) -> list:
    """
    Split a date range into chunks that respect Pinterest's 90-day limit.
    Using 89 days to be safe.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    ranges = []
    current_start = start
    
    while current_start < end:
        current_end = min(current_start + timedelta(days=max_days), end)
        ranges.append(
            (current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d"))
        )
        current_start = current_end + timedelta(days=1)
    
    return ranges


def append_to_historical_data(
    new_df: pd.DataFrame, 
    historical_file: str,
    key_columns: list
):
    """
    Smart merge: Replace overlapping data with fresh data, keep older historical data.
    This ensures you always have the most up-to-date metrics while preserving history.
    
    Args:
        new_df: New data to append
        historical_file: Path to historical CSV file
        key_columns: Columns that uniquely identify a row (e.g., ['date', 'pin_id'])
    """
    if not os.path.exists(historical_file):
        # First time - just save the new data
        new_df.to_csv(historical_file, index=False)
        print(f"📝 Created new historical file: {historical_file}")
        return new_df
    
    # Load existing data
    existing_df = pd.read_csv(historical_file)
    
    # Determine the date range of new data
    new_min_date = new_df['date'].min()
    new_max_date = new_df['date'].max()
    
    print(f"   New data date range: {new_min_date} to {new_max_date}")
    print(f"   Existing data date range: {existing_df['date'].min()} to {existing_df['date'].max()}")
    
    # Keep historical data older than the new data range
    historical_cutoff = pd.to_datetime(new_min_date)
    old_data = existing_df[pd.to_datetime(existing_df['date']) < historical_cutoff].copy()
    
    print(f"   Keeping {len(old_data)} rows older than {new_min_date}")
    print(f"   Replacing {len(existing_df) - len(old_data)} rows with fresh data")
    
    # Combine old historical data with new fresh data
    combined_df = pd.concat([old_data, new_df], ignore_index=True)
    
    # Remove duplicates
    combined_df = combined_df.drop_duplicates(subset=key_columns, keep='last')
    combined_df = combined_df.sort_values(by=key_columns)
    
    # Save back
    combined_df.to_csv(historical_file, index=False)
    
    net_change = len(combined_df) - len(existing_df)
    print(f"📝 Updated historical file: {historical_file}")
    print(f"   Previous total: {len(existing_df)} rows")
    print(f"   New total: {len(combined_df)} rows")
    print(f"   Net change: {net_change:+d} rows")
    print(f"   Final date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
    
    return combined_df


if __name__ == "__main__":
    try:
        # Date ranges
        today = datetime.now()
        
        # Pinterest organic analytics supports 90-day lookback
        START_DATE = (today - timedelta(days=89)).strftime("%Y-%m-%d")
        END_DATE = today.strftime("%Y-%m-%d")
        
        print("="*60)
        print("PINTEREST ORGANIC ANALYTICS")
        print("="*60)
        print(f"Date range: {START_DATE} to {END_DATE}")
        print()
        
        # ============================================================
        # 1. ACCOUNT-LEVEL ANALYTICS
        # ============================================================
        print("\n" + "="*60)
        print("1. ACCOUNT-LEVEL ANALYTICS")
        print("="*60)
        
        df_account = get_user_account_analytics(START_DATE, END_DATE)
        
        if len(df_account) > 0:
            print(f"✅ Retrieved {len(df_account)} days of account data")
            
            # Save account data
            output_file = "pinterest_organic_account_daily.csv"
            df_account.to_csv(output_file, index=False)
            print(f"💾 Saved to {output_file}")
            
            # Show summary
            if 'impression' in df_account.columns:
                total_impressions = df_account['impression'].sum()
                print(f"   Total impressions: {total_impressions:,}")
            
            if 'outbound_click' in df_account.columns:
                total_clicks = df_account['outbound_click'].sum()
                print(f"   Total outbound clicks: {total_clicks:,}")
            
            if 'save' in df_account.columns:
                total_saves = df_account['save'].sum()
                print(f"   Total saves: {total_saves:,}")
            
            print(f"\n📄 Sample rows:")
            print(df_account.head(3).to_string())
            
            # Append to historical
            print(f"\n📚 Building historical dataset...")
            append_to_historical_data(
                df_account, 
                "pinterest_organic_account_historical.csv",
                ['date']
            )
        else:
            print("⚠️ No account data retrieved")
        
        time.sleep(1)
        
        # ============================================================
        # 2. TOP PINS ANALYTICS
        # ============================================================
        print("\n" + "="*60)
        print("2. TOP PINS ANALYTICS (Top 50)")
        print("="*60)
        
        df_top_pins = get_top_pins_analytics(
            START_DATE, 
            END_DATE,
            sort_by="IMPRESSION",
            num_of_pins=50
        )
        
        if len(df_top_pins) > 0:
            print(f"✅ Retrieved {len(df_top_pins)} top pins")
            
            # Save top pins data
            output_file = "pinterest_organic_top_pins.csv"
            df_top_pins.to_csv(output_file, index=False)
            print(f"💾 Saved to {output_file}")
            
            # Show summary
            if 'impression' in df_top_pins.columns:
                print(f"   Top pin impressions: {df_top_pins['impression'].iloc[0]:,}")
                print(f"   Total impressions (top 50): {df_top_pins['impression'].sum():,}")
            
            print(f"\n📄 Top 5 pins:")
            print(df_top_pins[['pin_id', 'impression', 'outbound_click', 'save']].head(5).to_string())
            
            # Now get detailed daily metrics for these top pins
            print(f"\n📊 Fetching daily metrics for top pins...")
            pin_ids = df_top_pins['pin_id'].tolist()
            
            # Split into chunks of 100 (API limit)
            all_pin_details = []
            for i in range(0, len(pin_ids), 100):
                chunk = pin_ids[i:i+100]
                print(f"   Fetching batch {i//100 + 1} ({len(chunk)} pins)...")
                
                df_pins_chunk = get_multiple_pins_analytics(chunk, START_DATE, END_DATE)
                all_pin_details.append(df_pins_chunk)
                time.sleep(1)
            
            df_top_pins_daily = pd.concat(all_pin_details, ignore_index=True)
            
            if len(df_top_pins_daily) > 0:
                output_file = "pinterest_organic_top_pins_daily.csv"
                df_top_pins_daily.to_csv(output_file, index=False)
                print(f"💾 Saved daily metrics to {output_file}")
                print(f"   Total rows: {len(df_top_pins_daily)}")
                
                # Append to historical
                print(f"\n📚 Building historical dataset...")
                append_to_historical_data(
                    df_top_pins_daily,
                    "pinterest_organic_pins_historical.csv",
                    ['date', 'pin_id']
                )
        else:
            print("⚠️ No top pins data retrieved")
        
        time.sleep(1)
        
        # ============================================================
        # 3. TOP VIDEO PINS ANALYTICS
        # ============================================================
        print("\n" + "="*60)
        print("3. TOP VIDEO PINS ANALYTICS (Top 50)")
        print("="*60)
        
        df_top_videos = get_top_video_pins_analytics(
            START_DATE,
            END_DATE,
            sort_by="IMPRESSION",
            num_of_pins=50
        )
        
        if len(df_top_videos) > 0:
            print(f"✅ Retrieved {len(df_top_videos)} top video pins")
            
            # Save top video pins data
            output_file = "pinterest_organic_top_videos.csv"
            df_top_videos.to_csv(output_file, index=False)
            print(f"💾 Saved to {output_file}")
            
            # Show summary
            if 'video_mrc_view' in df_top_videos.columns:
                print(f"   Total video views (top 50): {df_top_videos['video_mrc_view'].sum():,}")
            
            print(f"\n📄 Top 5 video pins:")
            available_cols = [col for col in ['pin_id', 'impression', 'video_mrc_view', 'save'] if col in df_top_videos.columns]
            print(df_top_videos[available_cols].head(5).to_string())
        else:
            print("⚠️ No top video pins data retrieved (may not have video content)")
        
        print("\n" + "="*60)
        print("✅ ORGANIC ANALYTICS COMPLETE")
        print("="*60)
        print("\n💡 TIP: Run this script daily/weekly to build historical data")
        print("   beyond the 90-day API limit. Historical data is automatically")
        print("   merged and preserved in the *_historical.csv files.")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        if hasattr(e, 'response'):
            print(f"Response: {e.response.text}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()