import requests
import json
import pandas as pd
import datetime
import re
import time

# authentication token on Piwik PRO API ---------------------------------


def piwik_token(domain, url, id, secret):
    creds = {
        "grant_type": "client_credentials",
        "client_id": id,
        "client_secret": secret
    }
    token_data = requests.post(url, data=creds, headers={
                               'Accept': 'application/json'}, json={"key": "value"}).json()

    token = token_data['access_token']
    token_headers = {"Authorization": "Bearer " +
                     token,  "Accept-Encoding": "gzip"}

    return(token_headers)


piwik_domain = ''
token_url = f'https://{piwik_domain}.piwik.pro/auth/token'
client_id = ""
client_secret = ""

piwik_headers = piwik_token(token_url, token_url, client_id, client_secret)
print("token generated")

headers_timestamp = datetime.datetime.now()

website_id = ""

query_url = f'https://{piwik_domain}.piwik.pro/api/analytics/v1/query'
session_query_url = f'https://{piwik_domain}.piwik.pro/api/analytics/v1/sessions/'
event_query_url = f'https://{piwik_domain}.piwik.pro/api/analytics/v1/events/'


def piwik_query(query, url_query):
    piwik_response = requests.post(
        url_query, headers=piwik_headers, data=json.dumps(query))

    if piwik_response.status_code == 200:
        piwik_data = pd.DataFrame(piwik_response.json()['data'])
    else:
        print(f'session response code is: {piwik_response.status_code}')
        piwik_data = piwik_response
    return piwik_data


now = datetime.datetime.now() - datetime.timedelta(minutes=1)
mins_ago = now - datetime.timedelta(minutes=30)


token_age = (datetime.datetime.now()-headers_timestamp).total_seconds()
print(f'token age is {token_age} seconds')


# ---------------------------  Data API Query Loop--------------------------

while True & bool(token_age <= 1300):

    # --------------------------- RAW SESSION DATA -----------------------------

    raw_session_query = {
        "relative_date": "today",
        "website_id": website_id,
        "columns": [
            {
                "column_id": "source"
            },
            {
                "column_id": "medium"
            },
            {
                "column_id": "campaign_name"
            },
            {
                "column_id": "session_total_ecommerce_conversions"
            },

        ],
        "filters": {
            "operator": "and",
            "conditions": []
        },
        "offset": 0,
        "limit": 100000,
        "format": "json"
    }

    try:

        session_data = piwik_query(raw_session_query, session_query_url)

        session_query_columns = ['session_id',  'visitor_id', 'timestamp']
        for i in raw_session_query["columns"]:
            for v in i.values():
                session_query_columns.append(v)

        session_data.columns = session_query_columns

        session_data["timestamp"] = pd.to_datetime(session_data["timestamp"])

        session_data = session_data.sort_values('timestamp', ascending=False)

        # ------------------- Get today's total orders, sales, sessions

        today_orders = round(
            session_data["session_total_ecommerce_conversions"].sum())

        today_sessions = session_data["session_id"].nunique()

        # ------------------- LIVE session data from timeframe (30 mins)

        # loc function to retrieve last 30 minutes raw sessions
        df_live = session_data.loc[session_data["timestamp"].between(
            mins_ago, now)]

        # Total timeframe sessions metric
        live_sessions = df_live["session_id"].nunique()

        # Total timeframe Session Order Conversions
        live_orders = df_live["session_total_ecommerce_conversions"].sum()

        df_live_source = df_live.groupby(["source", "medium", "campaign_name"]).agg(
            {"session_id": "count", "session_total_ecommerce_conversions": "sum"}).sort_values("session_id", ascending=False).reset_index()

        df_live_source.rename(columns={'session_id': 'sessions'}, inplace=True)

        # dataframe to plot sessions per minute
        df_live_minutes = df_live[['timestamp', 'session_id']]

        # group the data by minute
        df_live_minutes = df_live_minutes.groupby(pd.Grouper(
            key="timestamp", freq="1Min")).agg({"session_id": "count"}).reset_index()

        # format the "Time" column to show HH:MM
        df_live_minutes['timestamp'] = df_live_minutes["timestamp"].dt.strftime(
            '%H:%M')

        # rename the "session_id" column to "Sessions"
        df_live_minutes = df_live_minutes.rename(
            columns={"session_id": "Sessions"})

    except:
        today_orders = 0
        today_sessions = 0
        live_sessions = 0
        live_orders = 0
        df_live = None
        df_live_minutes = None
        df_live_source = None

    # ------------------ EVENT DATA ---------------------------------

    raw_event_query = {
        "relative_date": "today",
        "website_id": website_id,
        "columns": [
            {
                "column_id": "event_type"
            },
            {
                "column_id": "event_url"
            },
            {
                "column_id": "search_keyword"
            },
            {
                "column_id": "revenue"
            },
            {
                "column_id": "order_id"
            },

        ],
        "order_by": [
            [
                4,
                "desc"
            ]
        ],
        "filters": {
            "operator": "and",
            "conditions": [
                {
                        "operator": "or",
                        "conditions": [
                            {
                                "column_id": "event_type",
                                "condition": {
                                    "operator": "eq",
                                    "value": 4  # Search
                                }
                            },
                            {
                                "column_id": "event_type",
                                "condition": {
                                    "operator": "eq",
                                    "value": 1  # Page View
                                }
                            },
                            {
                                "column_id": "event_type",
                                "condition": {
                                    "operator": "eq",
                                    "value": 9  # Ecommerce Conversion
                                }
                            }
                        ]
                }
            ]
        },
        "offset": 0,
        "limit": 100000,
        "format": "json",
        "column_format": "name"
    }

    try:

        event_data = piwik_query(raw_event_query, event_query_url)

        event_query_columns = ['session_id',
                               'event_id', 'visitor_id', 'timestamp']
        for i in raw_event_query["columns"]:
            for v in i.values():
                event_query_columns.append(v)

        event_data.columns = event_query_columns

        event_data["timestamp"] = pd.to_datetime(event_data["timestamp"])

        # event_type has a list type column. this splits it into two different columns and drops the original afterwards
        event_data[['event_type_id', 'event_type_name']
                   ] = event_data['event_type'].apply(lambda x: pd.Series([x[0], x[1]]))

        event_data.drop(columns=['event_type'], inplace=True)

        # # Total searches today
        df_searches = event_data[event_data['event_type_id'] == 4]

        total_searches = df_searches['visitor_id'].nunique()

        # # Total pageviews today
        df_pageviews = event_data[event_data['event_type_id'] == 1]

        total_pageviews = df_pageviews['visitor_id'].nunique()

        # # Total Ecommerce Sales from today
        total_sales = event_data['revenue'].sum()

        # live events from 30 minutes ago
        df_live_events = event_data.loc[event_data["timestamp"].between(
            mins_ago, now)]

        # live searches
        df_live_searches = df_live_events[df_live_events['event_type_id'] == 4].groupby(
            'search_keyword')['visitor_id'].nunique().sort_values(ascending=False).reset_index()

        # live searches table
        df_live_searches.rename(columns={"visitor_id": "unique_searches"})

        # total live searches
        df_total_live_searches = df_live_searches['visitor_id'].nunique()

        # live pageviews

        # live pageviews table
        df_live_pageviews = df_live_events[df_live_events['event_type_id'] == 1].groupby(
            'event_url')['visitor_id'].nunique().sort_values(ascending=False).reset_index()

        df_live_pageviews.rename(
            columns={"event_url": "url", "visitor_id": "pageviews"})

        # total live pageviews
        df_total_live_pageviews = df_live_pageviews['visitor_id'].nunique()

    except:
        df_pageviews = None
        df_searches = None
        total_pageviews = None
        total_sales = 0
        total_searches = 0
        df_live_searches = None
        df_total_live_searches = 0
        df_live_pageviews = None
        df_total_live_pageviews = 0

    # prints for debugging

    print("session data")
    print(session_data)
    print(f'today orders: {today_orders}')
    print(f'today sessions {today_sessions}')
    print(f'live sessions {live_sessions}')
    print(f'live orders {live_orders}')
    print("df_live_source")
    print(df_live_source)
    print("df_live_minutes")
    print(df_live_minutes)

    print("event data")
    print(event_data)
    print("df searches")
    print(df_searches)
    print("df_pageviews")
    print(df_pageviews)
    print(f'total pageviews {total_pageviews}')
    print(f'total sales {total_sales}')
    print(f'total searches {total_searches}')
    print("df_total_live_searches")
    print(df_total_live_searches)
    print("df_live_searches")
    print(df_live_searches)
    print("df_live_pageviews")
    print(df_live_pageviews)

    print("data refreshes in 30 secs")
    time.sleep(30)

    token_age = (datetime.datetime.now()-headers_timestamp).total_seconds()
    print(f'token age is {token_age} seconds')


print("while loop ended")
