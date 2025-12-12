import os
import time
import requests
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json

from pinterest_oauth import get_pinterest_token
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

PINTEREST_BASE_URL = "https://api.pinterest.com/v5"

ACCESS_TOKEN = get_pinterest_token()
AD_ACCOUNT_ID = os.environ["PINTEREST_AD_ACCOUNT_ID"]

def upload_df_to_google_sheet(
    df: pd.DataFrame,
    sheet_id: str,
    sheet_tab: str = "Paid Metrics By Country"
):
    """
    Upload a DataFrame to a Google Sheet tab.

    Requires:
      - GOOGLE_SERVICE_ACCOUNT_JSON in env (full service account JSON as string)
      - sheet_id: Google Sheet ID
      - sheet_tab: tab name in the spreadsheet
    """
    service_account_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not service_account_json:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON env var is not set")

    info = json.loads(service_account_json)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    # Clean DataFrame for upload (no NaNs)
    df_for_upload = df.copy()
    df_for_upload = df_for_upload.astype(object).where(pd.notnull(df_for_upload), "")

    values = [df_for_upload.columns.tolist()] + df_for_upload.values.tolist()

    # Clear existing data in the tab
    clear_range = f"{sheet_tab}!A:Z"
    sheet.values().clear(
        spreadsheetId=sheet_id,
        range=clear_range,
        body={}
    ).execute()

    # Write starting at A1
    write_range = f"{sheet_tab}!A1"
    body = {"values": values}
    sheet.values().update(
        spreadsheetId=sheet_id,
        range=write_range,
        valueInputOption="RAW",
        body=body
    ).execute()

    print(f"✅ Uploaded {len(df_for_upload)} rows to Google Sheet tab '{sheet_tab}' ({sheet_id})")

def _headers():
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def get_targeting_analytics_by_country(
    campaign_ids: list, 
    start_date: str, 
    end_date: str,
    granularity: str = "DAY"
) -> pd.DataFrame:
    """
    Get synchronous targeting analytics with country breakdown.
    This endpoint has a 90-day limit per request.
    
    Returns a DataFrame with flattened country-level metrics.
    """
    url = f"{PINTEREST_BASE_URL}/ad_accounts/{AD_ACCOUNT_ID}/campaigns/targeting_analytics"
    
    params = {
        "campaign_ids": ",".join(campaign_ids),
        "start_date": start_date,
        "end_date": end_date,
        "targeting_types": "COUNTRY",
        "granularity": granularity,
        "columns": ",".join([
            "CAMPAIGN_ID",
            "CAMPAIGN_NAME",
            "SPEND_IN_MICRO_DOLLAR",
            "IMPRESSION_1",
            "CLICKTHROUGH_1",
            "TOTAL_CHECKOUT",
            "TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR",
            "TOTAL_CONVERSIONS",
        ]),
        "click_window_days": 7,
        "engagement_window_days": 30,
        "view_window_days": 1,
        "conversion_report_time": "TIME_OF_AD_ACTION",
    }
    
    resp = requests.get(url, headers=_headers(), params=params)
    resp.raise_for_status()
    data = resp.json()
    
    # The response contains data in nested format (key is "data", not "items")
    records = data.get("data", [])
    if not records:
        return pd.DataFrame()
    
    # Flatten the nested structure
    flattened_records = []
    for record in records:
        targeting_type = record.get('targeting_type', '')
        targeting_value = record.get('targeting_value', '')  # This is the country code
        metrics = record.get('metrics', {})
        
        # If metrics is a string (JSON), parse it
        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics.replace("'", '"'))
            except:
                metrics = {}
        
        # Create flattened record
        flat_record = {
            'targeting_type': targeting_type,
            'country': targeting_value,
            **metrics  # Unpack all metrics into the record
        }
        flattened_records.append(flat_record)
    
    return pd.DataFrame(flattened_records)


def get_campaigns_list() -> list:
    """Get list of all campaigns to check what exists."""
    url = f"{PINTEREST_BASE_URL}/ad_accounts/{AD_ACCOUNT_ID}/campaigns"
    params = {"page_size": 100}
    
    resp = requests.get(url, headers=_headers(), params=params)
    resp.raise_for_status()
    data = resp.json()
    
    return data.get("items", [])


def get_date_ranges(start_date: str, end_date: str, max_days: int = 89) -> list:
    """
    Split a date range into chunks that respect Pinterest's 90-day limit
    for targeting_analytics endpoint. Using 89 days to be safe.
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


def append_to_historical_data(new_df: pd.DataFrame, historical_file: str = "pinterest_country_historical.csv"):
    """
    Smart merge: Replace last 90 days with fresh data, keep older historical data.
    This ensures you always have the most up-to-date attribution while preserving history.
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
    
    # STRATEGY: Keep historical data older than the new data range
    # This preserves data that's beyond the 90-day API limit
    historical_cutoff = pd.to_datetime(new_min_date)
    old_data = existing_df[pd.to_datetime(existing_df['date']) < historical_cutoff].copy()
    
    print(f"   Keeping {len(old_data)} rows older than {new_min_date}")
    print(f"   Replacing {len(existing_df) - len(old_data)} rows with fresh data")
    
    # Combine old historical data with new fresh data
    combined_df = pd.concat([old_data, new_df], ignore_index=True)
    
    # Remove any remaining duplicates (shouldn't happen but safe)
    key_cols = ['date', 'country', 'campaign_id']
    combined_df = combined_df.drop_duplicates(subset=key_cols, keep='last')
    combined_df = combined_df.sort_values(by=['date', 'country', 'campaign_id'])
    
    # Save back
    combined_df.to_csv(historical_file, index=False)
    
    net_change = len(combined_df) - len(existing_df)
    print(f"📝 Updated historical file: {historical_file}")
    print(f"   Previous total: {len(existing_df)} rows")
    print(f"   New total: {len(combined_df)} rows")
    print(f"   Net change: {net_change:+d} rows")
    print(f"   Final date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
    
    return combined_df

def build_daily_paid_metrics_by_country(df_historical: pd.DataFrame) -> pd.DataFrame:
    """
    From pinterest_country_historical, build a daily aggregated dataset:

      - Group by date, campaign_type, country_code
      - campaign_type: 'Awareness' if campaign_name contains 'AWR' (case-insensitive), else 'Conversion'
      - country_code: original 'country' column
      - Metrics: sum of impressions, clicks, spend, checkouts, checkout_value
    """

    df = df_historical.copy()

    # Ensure required columns exist
    required_cols = ["date", "country", "campaign_name"]
    metric_candidates = {
        "impressions": ["impressions", "IMPRESSION_1", "IMPRESSION_1_GROSS"],
        "clicks": ["clicks", "CLICKTHROUGH_1"],
        "spend": ["spend"],  # you already derive this from micro dollars
        "checkouts": ["checkouts", "TOTAL_CHECKOUT"],
        "checkout_value": ["checkout_value", "TOTAL_CHECKOUT_VALUE_IN_DOLLAR"],
    }

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Expected column '{col}' not found in df_historical")

    # Map possible metric column names to canonical names if needed
    col_map = {}
    for canonical, candidates in metric_candidates.items():
        for c in candidates:
            if c in df.columns:
                col_map[canonical] = c
                break

    missing_metrics = [m for m in metric_candidates.keys() if m not in col_map]
    if missing_metrics:
        raise ValueError(f"Missing metric columns in df_historical for: {missing_metrics}")

    # Derive campaign_type from campaign_name (ILIKE '%AWR%')
    df["campaign_type"] = df["campaign_name"].str.contains("AWR", case=False, na=False).map(
        {True: "Awareness", False: "Conversion"}
    )

    # Rename for clarity
    df["country_code"] = df["country"]

    # Group by date, campaign_type, country_code
    group_cols = ["date", "campaign_type", "country_code"]

    agg_df = df.groupby(group_cols, as_index=False).agg(
        impression=(col_map["impressions"], "sum"),
        clicks=(col_map["clicks"], "sum"),
        spend=(col_map["spend"], "sum"),
        purchase=(col_map["checkouts"], "sum"),
        revenue=(col_map["checkout_value"], "sum"),
    )

    # Optional: sort for readability
    agg_df = agg_df.sort_values(by=["date", "country_code", "campaign_type"])

    return agg_df

if __name__ == "__main__":
    try:
        # First, let's see what campaigns exist
        print("Fetching campaigns...")
        campaigns = get_campaigns_list()
        print(f"\nFound {len(campaigns)} campaigns:")
        for camp in campaigns[:5]:
            print(f"  - {camp.get('name')} (ID: {camp.get('id')}, Status: {camp.get('status')})")
        
        if len(campaigns) > 5:
            print(f"  ... and {len(campaigns) - 5} more")
        
        # Get all campaign IDs
        campaign_ids = [str(camp.get('id')) for camp in campaigns]
        
        # Date ranges
        today = datetime.now()
        YTD_START = "2025-01-01"
        YTD_END = today.strftime("%Y-%m-%d")
        
        # ============================================================
        # COUNTRY-LEVEL DATA using targeting_analytics (90-day chunks)
        # ============================================================
        print("\n" + "="*60)
        print("FETCHING COUNTRY-LEVEL DATA (Targeting Analytics)")
        print("="*60)
        
        # Targeting analytics only supports last 90 days from TODAY
        # Calculate the earliest date we can fetch
        today = datetime.now()
        earliest_allowed = (today - timedelta(days=89)).strftime("%Y-%m-%d")
        
        # Adjust start date if it's too far back
        actual_start = max(YTD_START, earliest_allowed)
        
        if YTD_START < earliest_allowed:
            print(f"\n⚠️  WARNING: Targeting analytics only supports last 90 days")
            print(f"   Requested start: {YTD_START}")
            print(f"   Earliest allowed: {earliest_allowed}")
            print(f"   Using: {actual_start}")
        
        # Split into 90-day chunks for targeting_analytics
        date_ranges_country = get_date_ranges(actual_start, YTD_END, max_days=89)
        print(f"\nSplitting date range into {len(date_ranges_country)} chunks (90-day limit):")
        for start, end in date_ranges_country:
            print(f"  - {start} to {end}")
        
        all_country_data = []
        for i, (start, end) in enumerate(date_ranges_country, 1):
            print(f"\n[{i}/{len(date_ranges_country)}] Fetching country data from {start} to {end}...")
            try:
                df_country_chunk = get_targeting_analytics_by_country(
                    campaign_ids=campaign_ids,
                    start_date=start,
                    end_date=end,
                    granularity="DAY"
                )
                print(f"  Retrieved {len(df_country_chunk)} rows")
                
                if len(df_country_chunk) > 0:
                    # Add date range info for tracking
                    df_country_chunk['date_range_start'] = start
                    df_country_chunk['date_range_end'] = end
                    all_country_data.append(df_country_chunk)
                    
                    # Show sample of what we got
                    if 'country' in df_country_chunk.columns:
                        countries = df_country_chunk['country'].unique()
                        print(f"  Countries in this chunk: {len(countries)}")
                        print(f"  Sample countries: {list(countries[:5])}")
                
                # Be nice to Pinterest's API
                time.sleep(1)
                
            except requests.exceptions.HTTPError as e:
                print(f"  ⚠️ Error: {e}")
                if hasattr(e, 'response'):
                    print(f"  Response: {e.response.text}")
                continue
            except Exception as e:
                print(f"  ⚠️ Unexpected error: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Combine country data
        if all_country_data:
            df_country = pd.concat(all_country_data, ignore_index=True)
            print(f"\n✅ Country data: {len(df_country)} total rows")
            
            # Convert micro dollars to dollars
            if 'SPEND_IN_MICRO_DOLLAR' in df_country.columns:
                df_country['SPEND_IN_DOLLAR'] = df_country['SPEND_IN_MICRO_DOLLAR'] / 1_000_000
            
            if 'TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR' in df_country.columns:
                df_country['TOTAL_CHECKOUT_VALUE_IN_DOLLAR'] = df_country['TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR'] / 1_000_000
            
            # Clean up column names for better readability
            column_renames = {
                'CAMPAIGN_ID': 'campaign_id',
                'CAMPAIGN_NAME': 'campaign_name',
                'DATE': 'date',
                'IMPRESSION_1_GROSS': 'impressions',
                'CLICKTHROUGH_1': 'clicks',
                'SPEND_IN_MICRO_DOLLAR': 'spend_micro',
                'SPEND_IN_DOLLAR': 'spend',
                'TOTAL_CONVERSIONS': 'conversions',
                'TOTAL_CHECKOUT': 'checkouts',
                'TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR': 'checkout_value_micro',
                'TOTAL_CHECKOUT_VALUE_IN_DOLLAR': 'checkout_value'
            }
            df_country.rename(columns=column_renames, inplace=True)
            
            # Reorder columns for better readability
            base_cols = ['date', 'country', 'campaign_id']
            metric_cols = [col for col in df_country.columns if col not in base_cols + ['date_range_start', 'date_range_end', 'targeting_type']]
            tracking_cols = ['date_range_start', 'date_range_end']
            
            final_cols = base_cols + sorted(metric_cols) + tracking_cols
            final_cols = [col for col in final_cols if col in df_country.columns]
            df_country = df_country[final_cols]
            
            # Save country-level data
            output_file_country = "pinterest_ytd_country_data.csv"
            df_country.to_csv(output_file_country, index=False)
            print(f"💾 Saved to {output_file_country}")
            
            # Display column info
            print(f"\n📋 Columns in output:")
            print(f"  {list(df_country.columns)}")
            
            # Show country breakdown summary
            if 'country' in df_country.columns:
                print(f"\n📊 Country Analysis:")
                print(f"Total unique countries: {df_country['country'].nunique()}")
                print(f"Countries: {sorted(df_country['country'].unique())}")
                
                if 'spend' in df_country.columns:
                    country_spend = df_country.groupby('country')['spend'].sum().sort_values(ascending=False)
                    print(f"\n💰 Top countries by spend:")
                    for country, spend in country_spend.head(10).items():
                        print(f"  {country}: ${spend:,.2f}")
                
                if 'impressions' in df_country.columns:
                    country_impressions = df_country.groupby('country')['impressions'].sum().sort_values(ascending=False)
                    print(f"\n👁️  Top countries by impressions:")
                    for country, impressions in country_impressions.head(10).items():
                        print(f"  {country}: {impressions:,}")
                
                if 'conversions' in df_country.columns:
                    country_conversions = df_country.groupby('country')['conversions'].sum().sort_values(ascending=False)
                    print(f"\n🎯 Top countries by conversions:")
                    for country, conversions in country_conversions.head(10).items():
                        print(f"  {country}: {int(conversions):,}")
                
                # Show date range
                if 'date' in df_country.columns:
                    print(f"\n📅 Date range in data:")
                    print(f"  From: {df_country['date'].min()}")
                    print(f"  To: {df_country['date'].max()}")
                
                # Show sample rows
                print(f"\n📄 Sample rows (first 3):")
                print(df_country.head(3).to_string())
                
                # Append to historical data file
                print(f"\n📚 Building historical dataset...")
                df_historical = append_to_historical_data(df_country)
                print(f"   Historical date range: {df_historical['date'].min()} to {df_historical['date'].max()}")

                # ================================
                # Upload historical data to Google Sheets
                # ================================
                google_sheet_id = os.environ.get("GOOGLE_SHEET_ID")
                google_sheet_tab = os.environ.get("GOOGLE_SHEET_TAB", "Paid Metrics By Country")

                if google_sheet_id:
                    print(f"\n📤 Uploading pinterest_country_historical to Google Sheet...")
                    upload_df_to_google_sheet(df_historical, google_sheet_id, google_sheet_tab)
                else:
                    print("\n⚠️ GOOGLE_SHEET_ID not set – skipping Google Sheets upload.")
                # ================================
                # Build daily aggregated metrics by country
                # ================================
                if google_sheet_id:
                    print(f"\n📊 Building Daily Paid Metrics By Country...")
                    df_daily = build_daily_paid_metrics_by_country(df_historical)
                    print(f"   Daily rows: {len(df_daily)}")

                    # Upload aggregated daily metrics to a separate tab
                    daily_tab_name = "Daily Paid Metrics By Country"
                    print(f"\n📤 Uploading daily metrics to Google Sheet tab '{daily_tab_name}'...")
                    upload_df_to_google_sheet(df_daily, google_sheet_id, daily_tab_name)
                else:
                    print("\n⚠️ GOOGLE_SHEET_ID not set – skipping Daily Paid Metrics By Country upload.")
            else:
                print(f"\n⚠️ No country column found")
                print(f"Available columns: {list(df_country.columns)}")
        else:
            print("\n⚠️ No country-level data retrieved")
            print("\n💡 TIP: The targeting_analytics endpoint only supports the last 90 days.")
            print("   To build historical data, run this script regularly (daily/weekly)")
            print("   and it will automatically append new data to pinterest_country_historical.csv")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        if hasattr(e, 'response'):
            print(f"Response: {e.response.text}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
