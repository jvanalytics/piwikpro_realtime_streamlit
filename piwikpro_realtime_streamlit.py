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
                   page_title="PiwikPro Realtime Analytics")


now = datetime.datetime.now()
today_pt = now.strftime("%d-%m-%Y")

st.title(f"PiwikPro Realtime Analytics")

st.sidebar.subheader(
    "Please insert your Piwik Pro website/app and API key details and data requests.")

# user input with cache functionality to not lose credentials. it is reset in 3 hours to not break streamlit memory
@st.cache_resource(ttl=3600*12)
def get_user_input():
    return {
        "piwik_domain": "",
        "website_id": "",
        "client_id": "",
        "client_secret": "",
        "total_sessions":"",
        "total_pageviews":"",
        "total_searches":""
    }


user_input = get_user_input()


user_input["total_sessions"]=st.sidebar.checkbox("Total Daily Sessions?")
user_input["total_pageviews"]=st.sidebar.checkbox("Total Daily Pageviews?")
user_input["total_searches"]=st.sidebar.checkbox("Total Daily Searches?")


user_input["piwik_domain"] = st.sidebar.text_input(
    "Piwik Domain (from domain.piwik.pro)", user_input["piwik_domain"])
user_input["website_id"] = st.sidebar.text_input(
    "Website id", user_input["website_id"])
user_input["client_id"] = st.sidebar.text_input(
    "Client ID", user_input["client_id"])

user_input["client_secret"] = st.sidebar.text_input(
    "Client Secret", user_input["client_secret"], type="password")

st.sidebar.markdown(
    "Piwik Pro's API allow us to access its raw session and event metrics in near real time. Official PiwikPro instructions for getting [Website Id](https://help.piwik.pro/support/questions/find-website-id/) and [Client Id / Secret](https://developers.piwik.pro/en/latest/platform/getting_started.html). Cache, refresh rate and options are limited in order to not break Streamlit's memory.", unsafe_allow_html=True)

st.sidebar.text("Created by João Valente. Enjoy!")
st.sidebar.markdown(
    "[Linkedin](https://www.linkedin.com/in/joao-valente-analytics/)", unsafe_allow_html=True)
st.sidebar.markdown(
    "[Medium](https://medium.com/@jvanalytics)", unsafe_allow_html=True)
st.sidebar.markdown(
    "[Github](https://github.com/jvanalytics)", unsafe_allow_html=True)

st.sidebar.text(f"Version 1.0")



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


    # --------------------------- Streamlit Data (empty placeholders to be filled later  by on the query api)

    update_time = st.sidebar.empty()

    tab1, tab2, tab3 = st.tabs(
        ["Traffic and Ecommerce", "Pageviews", "Searches"])

    with tab1:

        st.header("Live Traffic and Ecommerce")

        col1, col2, col3 = st.columns(3)

        st_live_sessions = col1.metric(label="Live Sessions (last 30 mins)",value=0)
        st_live_orders = col2.metric(label="Live Orders (last 30 mins)",value=0)
        st_live_revenue = col3.metric(label="Live Revenue (from raw event data)",value=0)

        st_live_minutes = st.empty()

        st_live_source = st.dataframe()

        if user_input["total_sessions"]: 
            st.header("Today's Total Traffic and Ecommerce")

            col4, col5, col6 = st.columns(3)

            st_total_sessions = col4.metric(label="Today's Total Sessions",value=0)
            st_total_orders = col5.metric(label="Today's Total Orders",value=0)
            st_total_revenue = col6.metric(label="Total Revenue (from raw event data)",value=0)

            st_total_sessions_source = st.dataframe()

    with tab2:

        st.header("Live Pageviews")

        st_total_live_pageviews = st.metric(label="Total Live Pageviews (last 30 mins)", value=0)
        st_live_pageviews = st.empty()

        if user_input["total_pageviews"]:

            st.header("Today's Total Pageviews")

            st_total_pageviews = st.metric("Today's Total Pageviews",value=0)
            st_table_total_pageviews = st.empty()
    
    with tab3:

        st.header("Live Searches")

        st_total_live_searches = st.metric(
            "Total Live Searches (last 30 mins)",value=0)
        st_live_searches = st.empty()

        if user_input["total_searches"]:
            st.header("Today's Total Searches")
            st_total_searches = st.metric("Today's Total Searches", value=0)
            st_table_total_searches=st.empty()

    st_spinner = st.empty()

    # ---------------------------  Data API Query --------------------------

    # --------------------------- RAW SESSION DATA -----------------------------

    # initialize df_live_minutes here
    with st.spinner("Data is loading. Event Data may take longer depending on volume 🥵"):

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



            # ------------------- LIVE session data from timeframe (30 mins)

            # loc function to retrieve last 30 minutes raw sessions
            df_live = session_data.loc[session_data["timestamp"].between(
                mins_ago, now)]
            

            # Total timeframe sessions metric
            live_sessions = df_live["session_id"].nunique()
            st_live_sessions.metric(
                "Live Sessions (last 30 mins)", live_sessions)            

            # Total timeframe Session Order Conversions
            live_orders = df_live["session_total_ecommerce_conversions"].sum(
            )
            st_live_orders.metric("Live Orders (last 30 mins)", live_orders)


            df_live_source = df_live.groupby(["source", "medium", "campaign_name"]).agg(
                {"session_id": "count", "session_total_ecommerce_conversions": "sum"}).sort_values("session_id", ascending=False).reset_index()

            df_live_source.rename(columns={
                'session_id': 'sessions', 'session_total_ecommerce_conversions': 'orders'}, inplace=True)
            
            
            df_live_source[r"% conversion rate"]=df_live_source["orders"]/df_live_source["sessions"]*100



            # dataframe to plot sessions per minute
            df_live_minutes = df_live[['timestamp', 'session_id']]

            # group the data by minute
            df_live_minutes = df_live_minutes.groupby(pd.Grouper(
                key="timestamp", freq="1Min")).agg({"session_id": "count"}).reset_index()

            # format the "Time" column to show HH:MM
            df_live_minutes['timestamp'] = df_live_minutes["timestamp"].dt.strftime(
                '%H:%M')

            # # rename the "session_id" column to "Sessions"
            df_live_minutes = df_live_minutes.rename(
                columns={"timestamp": "Time", "session_id": "Sessions"})
                
            st_live_minutes.bar_chart(df_live_minutes, x='Time')
            st_live_source.dataframe(df_live_source, use_container_width=True)    


            
           # ------------------- Get today's total orders, revenue, sessions

            if user_input["total_sessions"]:
                today_orders = round(
                    session_data["session_total_ecommerce_conversions"].sum())
                
                st_total_orders.metric("Today's Total Orders", today_orders)

                today_sessions = session_data["session_id"].nunique()
                st_total_sessions.metric("Today's Total Sessions", today_sessions)

                df_total_source = session_data.groupby(["source", "medium", "campaign_name"]).agg(
                    {"session_id": "count", "session_total_ecommerce_conversions": "sum"}).sort_values("session_id", ascending=False).reset_index()

                df_total_source.rename(columns={
                    'session_id': 'sessions', 'session_total_ecommerce_conversions': 'orders'}, inplace=True)
                
                df_total_source[r"% conversion rate"]=df_total_source["orders"]/df_total_source["sessions"]*100

                st_total_sessions_source.dataframe(df_total_source,use_container_width=True)


        except Exception as e:
            print(f"Session Data Error: {e}")
            st.error(f"Session Data Error:{e}. Please verify your PiwikPro credentials input.")


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


            if user_input["total_sessions"]:
                # Total Ecommerce Revenue from today
                total_revenue = round(event_data['revenue'].sum(), 2)
                st_total_revenue.metric("Total Revenue €,$...", total_revenue)            

            # ------------- live events from 30 minutes ago
            df_live_events = event_data.loc[event_data["timestamp"].between(
                mins_ago, now)]




            if user_input["total_pageviews"]:
                # Total pageviews today
                df_pageviews = event_data[event_data['event_type_id'] == 1]

                total_pageviews = df_pageviews['visitor_id'].nunique()
                st_total_pageviews.metric("Today's Total Pageviews", total_pageviews)

                # total daily table pageviews
                df_pageviews = df_pageviews.groupby(
                    'event_url')['visitor_id'].nunique().sort_values(ascending=False).reset_index()
                df_pageviews = df_pageviews.rename(
                    columns={"event_url": "url", "visitor_id": "pageviews"})
                st_table_total_pageviews.dataframe(df_pageviews,use_container_width=True)



            # Total searches today
            if user_input["total_searches"]:

                df_searches = event_data[event_data['event_type_id'] == 4]

                total_searches = df_searches['visitor_id'].nunique()
                st_total_searches.metric("Today's Total Searches", total_searches)

                # total daily table searches
                df_searches = df_searches.groupby(
                    'search_keyword')['visitor_id'].nunique().sort_values(ascending=False).reset_index()
                df_searches = df_searches.rename(
                    columns={"visitor_id": "unique_searches"})


                st_table_total_searches.dataframe(df_searches,use_container_width=True)




            # live revenue from 30 minutes ago
            df_total_live_revenue = round(df_live_events['revenue'].sum(), 2)
            st_live_revenue.metric(
            "Live Revenue (last 30 mins) €,$...", df_total_live_revenue)

            # live searches
            df_live_searches = df_live_events[df_live_events['event_type_id'] == 4].groupby(
                'search_keyword')['visitor_id'].nunique().sort_values(ascending=False).reset_index()

            # live searches table
            df_live_searches = df_live_searches.rename(
                columns={"visitor_id": "unique_searches"})

            # total live searches
            df_total_live_searches = df_live_searches['unique_searches'].sum()
            st_total_live_searches.metric(
            "Total Live Searches (last 30 mins)", df_total_live_searches)
    
            st_live_searches.dataframe(df_live_searches, use_container_width=True)
    
    
            # live pageviews table
            df_live_pageviews = df_live_events[df_live_events['event_type_id'] == 1].groupby(
                'event_url')['visitor_id'].nunique().sort_values(ascending=False).reset_index()

            df_live_pageviews = df_live_pageviews.rename(
                columns={"event_url": "url", "visitor_id": "pageviews"})

            # total live pageviews
            df_total_live_pageviews = df_live_pageviews['pageviews'].sum()
            st_total_live_pageviews.metric(
            "Total Live Pageviews (last 30 mins)", df_total_live_pageviews)

            st_live_pageviews.dataframe(df_live_pageviews, use_container_width=True)


        except Exception as e:
            print(f"Event Data Error: {e}")
            st.error(f"Event Data Error:{e}. Please verify your PiwikPro credentials input.")


    now_update = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st_spinner.success(
        f"Data Loaded at {now_update}! It will automatically refresh in 10 minutes.", icon="✅")
    
    sleep = 600

    time.sleep(sleep)
    print(f"data refreshes in {sleep} secs")


    # automated javascript function to refresh page after the token expires and the while loop ends
    # 60000 ms = 1 min 

    st_javascript("""window.setTimeout( function() {
    window.location.reload();
    }, 60000);""")
