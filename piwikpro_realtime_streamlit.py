import requests
import json
import pandas as pd
import datetime
import re
import time
import streamlit as st
from streamlit_javascript import st_javascript

# STREAMLIT data visualization ----------------------

st.set_page_config(layout="centered",
                   page_title="Piwik Pro (near)Realtime Analytics")


now = datetime.datetime.now() - datetime.timedelta(minutes=1)
today_pt = now.strftime("%d-%m-%Y")

st.title(f"Piwik Pro (near)Realtime Analytics")

st.sidebar.subheader(
    "Please insert your Piwik Pro website/app details to generate near realtime data to the App")


@st.cache_data
def get_user_input():
    return {
        "piwik_domain": "",
        "website_id": "",
        "client_id": "",
        "client_secret": "",
    }


user_input = get_user_input()

user_input["piwik_domain"] = st.sidebar.text_input(
    "Piwik Domain (from domain.piwik.pro)", user_input["piwik_domain"])
user_input["website_id"] = st.sidebar.text_input(
    "Website id", user_input["website_id"])
user_input["client_id"] = st.sidebar.text_input(
    "Client ID", user_input["client_id"])
user_input["client_secret"] = st.sidebar.text_input(
    "Client Secret", user_input["client_secret"], type="password")


st.sidebar.text("Created by João Valente")
st.sidebar.markdown(
    "[Linkedin](https://www.linkedin.com/in/joao-valente-analytics/)", unsafe_allow_html=True)
st.sidebar.markdown(
    "[Medium](https://medium.com/@jvanalytics)", unsafe_allow_html=True)

st.sidebar.text(f"Version 1.0")


# st.header(f'Domain: {piwik_domain}.piwik.pro | Website id: {website_id}')


# markdown to change the font size for the metrics
st.markdown(
    """
<style>

h1 {
    text-align: center;
    display: flex;
    align-items: center;
    justify-content: center;
    width: auto;
}

h2 {
    text-align: center;
    display: flex;
    align-items: center;
    justify-content: center;
    width: auto;
}

[data-testid="metric-container"] {
  text-align: center;
  margin: auto;
}

</style>
""",
    unsafe_allow_html=True,
)

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


# Add this code to display a message when input values are not provided
if not user_input["piwik_domain"] or not user_input["client_id"] or not user_input["client_secret"] or not user_input["website_id"]:
    st.warning("Please provide the required input values in the sidebar.")


else:
    piwik_domain = user_input["piwik_domain"]
    token_url = f'https://{piwik_domain}.piwik.pro/auth/token'
    client_id = user_input["client_id"]
    client_secret = user_input["client_secret"]

    piwik_headers = piwik_token(token_url, token_url, client_id, client_secret)

    print("token generated")

    headers_timestamp = datetime.datetime.now()

    # website id from user input
    website_id = user_input["website_id"]

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

    now = datetime.datetime.now()
    mins_ago = now - datetime.timedelta(minutes=30)

    token_age = (datetime.datetime.now()-headers_timestamp).total_seconds()
    print(f'token age is {token_age} seconds')

    # --------------------------- Streamlit Data (empty placeholders to be filled later  by on the query api)

    update_time = st.sidebar.empty()

    tab1, tab2, tab3 = st.tabs(
        ["Traffic and Ecommerce", "Pageviews", "Searches"])

    with tab1:

        st.header("Traffic and Ecommerce")

        col1, col2, col3 = st.columns(3)

        st_total_sessions = col1.empty()
        st_live_sessions = col1.empty()

        st_total_orders = col2.empty()
        st_live_orders = col2.empty()

        st_total_sales = col3.empty()
        st_live_sales = col3.empty()

        st_live_minutes = st.empty()

        st_live_source = st.empty()

        st_live_filters = st.empty()
        st_selected_source_options = st.empty()
        st_selected_medium_options = st.empty()
        st_selected_campaign_options = st.empty()

    with tab2:

        st.header("Pageviews")

        col1, col2 = st.columns(2)

        st_total_pageviews = col1.empty()
        st_total_live_pageviews = col2.empty()

        st_live_pageviews = st.empty()

    with tab3:

        st.header("Searches")

        col1, col2 = st.columns(2)

        st_total_searches = col1.empty()
        st_total_live_searches = col2.empty()

        st_live_searches = st.empty()

    st_spinner = st.empty()

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
        with st.spinner("Data is loading. Event Data may take longer depending on volume 🥵"):

            try:

                session_data = piwik_query(
                    raw_session_query, session_query_url)

                session_query_columns = [
                    'session_id',  'visitor_id', 'timestamp']
                for i in raw_session_query["columns"]:
                    for v in i.values():
                        session_query_columns.append(v)

                session_data.columns = session_query_columns

                session_data["timestamp"] = pd.to_datetime(
                    session_data["timestamp"])

                session_data = session_data.sort_values(
                    'timestamp', ascending=False)

                # Define multiselect options
                st_live_filters.subheader(
                    f"You can filter the raw session data by source. It will be applied in the data update")

                source_options = session_data['source'].unique().tolist()
                medium_options = session_data['medium'].unique().tolist()
                campaign_options = session_data['campaign_name'].unique(
                ).tolist()

                # Create multiselect filter
                def get_selected_options():
                    selected_source_options = st_selected_source_options.multiselect(
                        'Filter Session data by Source.', source_options)
                    selected_medium_options = st_selected_medium_options.multiselect(
                        'Filter Session data by Medium', medium_options)
                    selected_campaign_options = st_selected_campaign_options.multiselect(
                        'Filter Session data by Campaign', campaign_options)
                    return (selected_source_options, selected_medium_options, selected_campaign_options)

                # Get the selected filter options
                selected_source_options, selected_medium_options, selected_campaign_options = get_selected_options()

                # Filter session data by selected options
                if selected_source_options:
                    session_data = session_data[session_data['source'].isin(
                        selected_source_options)]
                if selected_medium_options:
                    session_data = session_data[session_data['medium'].isin(
                        selected_medium_options)]
                if selected_campaign_options:
                    session_data = session_data[session_data['campaign_name'].isin(
                        selected_campaign_options)]

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
                live_orders = df_live["session_total_ecommerce_conversions"].sum(
                )

                df_live_source = df_live.groupby(["source", "medium", "campaign_name"]).agg(
                    {"session_id": "count", "session_total_ecommerce_conversions": "sum"}).sort_values("session_id", ascending=False).reset_index()

                df_live_source.rename(columns={
                    'session_id': 'sessions', 'session_total_ecommerce_conversions': 'orders'}, inplace=True)

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
                    columns={"timestamp": "Time", "session_id": "Sessions"})

            except Exception as e:
                print(f"Session Data Error: {e}")

                today_orders = 0
                today_sessions = 0
                live_sessions = 0
                live_orders = 0
                df_live = None
                df_live_minutes = None
                df_live_source = None

            # streamlit metrics amd visualizations

            st_total_orders.metric("Today's Total Orders", today_orders)
            st_total_sessions.metric("Today's Total Sessions", today_sessions)
            st_live_sessions.metric(
                "Live Sessions (last 30 mins)", live_sessions)
            st_live_orders.metric("Live Orders (last 30 mins)", live_orders)
            st_live_minutes.bar_chart(df_live_minutes, x='Time')
            st_live_source.dataframe(
                df_live_source, use_container_width=True)

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

                event_data["timestamp"] = pd.to_datetime(
                    event_data["timestamp"])

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
                total_sales = round(event_data['revenue'].sum(), 2)

                # live events from 30 minutes ago
                df_live_events = event_data.loc[event_data["timestamp"].between(
                    mins_ago, now)]

                # live sales from 30 minutes ago
                df_total_live_sales = round(df_live_events['revenue'].sum(), 2)

                # live searches
                df_live_searches = df_live_events[df_live_events['event_type_id'] == 4].groupby(
                    'search_keyword')['visitor_id'].nunique().sort_values(ascending=False).reset_index()

                # live searches table
                df_live_searches = df_live_searches.rename(
                    columns={"visitor_id": "unique_searches"})

                # total live searches
                df_total_live_searches = df_live_searches['unique_searches'].sum(
                )

                # live pageviews

                # live pageviews table
                df_live_pageviews = df_live_events[df_live_events['event_type_id'] == 1].groupby(
                    'event_url')['visitor_id'].nunique().sort_values(ascending=False).reset_index()

                df_live_pageviews = df_live_pageviews.rename(
                    columns={"event_url": "url", "visitor_id": "pageviews"})

                # total live pageviews
                df_total_live_pageviews = df_live_pageviews['pageviews'].sum()

            except Exception as e:
                print(f"Event Data Error: {e}")
                df_pageviews = None
                df_searches = None
                total_pageviews = None
                total_sales = 0
                total_searches = 0
                df_live_searches = None
                df_total_live_searches = 0
                df_live_pageviews = None
                df_total_live_pageviews = 0
                df_total_live_sales = 0

        # streamlit events metrics and visualizations
        st_total_sales.metric("Total Sales (from raw event data)", total_sales)
        st_live_sales.metric(
            "Total Live Sales (from raw event data)", df_total_live_sales)

        st_total_pageviews.metric("Total Pageviews", total_pageviews)
        st_total_searches.metric("Total Searches", total_searches)

        st_total_live_pageviews.metric(
            "Total Live Pageviews", df_total_live_pageviews)
        st_live_pageviews.dataframe(
            df_live_pageviews, use_container_width=True)

        st_total_live_searches.metric(
            "Total Live Searches", df_total_live_searches)
        st_live_searches.dataframe(df_live_searches, use_container_width=True)

        now_update = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sleep = 60

        st_spinner.success(
            f"Data Loaded at {now_update}! Refreshes in {sleep} seconds", icon="✅")

        time.sleep(sleep)
        print(f"data refreshes in {sleep} secs")

        token_age = (datetime.datetime.now()-headers_timestamp).total_seconds()
        print(f'token age is {token_age} seconds')

    print("token expired. while loop ended")

    # https://pypi.org/project/streamlit-javascript/
    # https://stackoverflow.com/a/32913581/16129184
    # automated script to refresh page 600000 ms = 1 min after the token expires and the while loop ends

    st_javascript("""window.setTimeout( function() {
    window.location.reload();
    }, 60000);""")
