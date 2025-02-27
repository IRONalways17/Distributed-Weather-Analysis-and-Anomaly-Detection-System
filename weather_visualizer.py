import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
import datetime
import numpy as np
import folium
from streamlit_folium import folium_static
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from datetime import timedelta
import json

# Set page configuration
st.set_page_config(
    page_title="Global Weather Analysis Dashboard",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize BigQuery client
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client()

client = get_bigquery_client()

# Define functions to fetch data
@st.cache_data(ttl=3600)
def get_recent_weather_data(hours=24):
    """Fetch recent weather data from BigQuery"""
    query = f"""
    SELECT
        city,
        country,
        latitude,
        longitude,
        timestamp,
        temperature,
        temperature_f,
        humidity,
        pressure,
        wind_speed,
        weather_main,
        weather_description,
        rain_1h,
        snow_1h,
        is_anomaly,
        anomaly_reason
    FROM
        `your-gcp-project-id.weather_dataset.weather_data`
    WHERE
        timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        AND data_type = 'current_weather'
    ORDER BY
        timestamp DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_anomalies(days=7):
    """Fetch recent weather anomalies from BigQuery"""
    query = f"""
    SELECT
        city,
        country,
        latitude,
        longitude,
        timestamp,
        temperature,
        weather_main,
        anomaly_reason,
        anomaly_z_score,
        anomaly_type,
        detected_at
    FROM
        `your-gcp-project-id.weather_dataset.weather_anomalies`
    WHERE
        detected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
    ORDER BY
        anomaly_z_score DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_temperature_trends(days=30):
    """Fetch temperature trends by city from BigQuery"""
    query = f"""
    WITH daily_temps AS (
        SELECT
            city,
            DATE(timestamp) as date,
            AVG(temperature) as avg_temp
        FROM
            `your-gcp-project-id.weather_dataset.weather_data`
        WHERE
            timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
            AND data_type = 'current_weather'
        GROUP BY
            city, DATE(timestamp)
    )
    SELECT
        city,
        date,
        avg_temp,
        AVG(avg_temp) OVER (
            PARTITION BY city
            ORDER BY date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as moving_avg
    FROM
        daily_temps
    ORDER BY
        city, date
    """
    return client.query(query).to_dataframe()

# Set up the sidebar for filtering
st.sidebar.title("Weather Analysis Dashboard")
st.sidebar.info(f"Last updated: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
st.sidebar.info(f"Current user: {st.session_state.get('user', 'IRONalways17')}")

# Time range filter
time_range = st.sidebar.selectbox(
    "Select Time Range",
    ["Last 24 Hours", "Last 48 Hours", "Last 72 Hours", "Last Week"],
    index=0
)

if time_range == "Last 24 Hours":
    hours = 24
elif time_range == "Last 48 Hours":
    hours = 48
elif time_range == "Last 72 Hours":
    hours = 72
else:
    hours = 168  # Last Week

# Fetch data based on filters
df_weather = get_recent_weather_data(hours)
df_anomalies = get_anomalies(7)  # Always fetch last 7 days of anomalies
df_trends = get_temperature_trends(30)  # Always fetch 30 days of trends

# Display header
st.title("🌦️ Global Weather Analysis Dashboard")
st.write(f"Displaying weather data for the selected time period: {time_range}")

# Layout of the dashboard
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Current Global Weather Map")
    
    # Get the most recent data point for each city
    latest_by_city = df_weather.sort_values('timestamp').groupby('city').last().reset_index()
    
    # Create the map
    m = folium.Map(location=[20, 0], zoom_start=2, tiles="CartoDB positron")
    
    # Add markers for each city
    for _, row in latest_by_city.iterrows():
        color = "green"
        if row.get('is_anomaly'):
            color = "red"
        
        # Create popup content
        popup_content = f"""
        <b>{row['city']}, {row['country']}</b><br>
        Temperature: {row['temperature']:.1f}°C ({row['temperature_f']:.1f}°F)<br>
        Weather: {row['weather_main']} ({row['weather_description']})<br>
        Humidity: {row['humidity']}%<br>
        Wind Speed: {row['wind_speed']} m/s<br>
        """
        
        if row.get('is_anomaly'):
            popup_content += f"<b>ANOMALY: {row['anomaly_reason']}</b>"
        
        # Create tooltip content (simpler than popup)
        tooltip = f"{row['city']}: {row['temperature']:.1f}°C - {row['weather_main']}"
        
        # Create marker
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=8,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.6,
            popup=folium.Popup(popup_content, max_width=300),
            tooltip=tooltip
        ).add_to(m)
    
    # Display the map
    folium_static(m)

with col2:
    st.subheader("Weather Anomalies")
    if not df_anomalies.empty:
        for _, row in df_anomalies.head(5).iterrows():
            with st.container():
                st.markdown(f"""
                **{row['city']}, {row['country']}** - {row['timestamp'].strftime('%Y-%m-%d %H:%M')}  
                {row['anomaly_reason']}  
                Weather: {row['weather_main']} | Temp: {row['temperature']:.1f}°C  
                Anomaly Score: {row['anomaly_z_score']:.2f}
                """)
                st.divider()
    else:
        st.info("No anomalies detected in the selected time period.")

# Temperature trends
st.subheader("Temperature Trends by City")

# Get list of cities
cities = df_trends['city'].unique()

# Allow user to select cities
selected_cities = st.multiselect("Select Cities", cities, default=cities[:5])

if selected_cities:
    # Filter data
    city_trends = df_trends[df_trends['city'].isin(selected_cities)]
    
    # Plot
    fig = px.line(
        city_trends,
        x='date',
        y='moving_avg',
        color='city',
        labels={'moving_avg': 'Temperature (°C)', 'date': 'Date'},
        title='3-Day Moving Average Temperature by City'
    )
    
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Please select at least one city to view temperature trends.")

# Weather distribution
st.subheader("Weather Conditions Distribution")
col1, col2 = st.columns(2)

with col1:
    # Weather type distribution by city
    weather_counts = df_weather.groupby(['city', 'weather_main']).size().reset_index(name='count')
    pivot_df = weather_counts.pivot(index='city', columns='weather_main', values='count').fillna(0)
    
    fig = px.imshow(
        pivot_df,
        labels=dict(x="Weather Condition", y="City", color="Count"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale="Viridis",
        aspect="auto"
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Temperature distribution
    fig = px.box(
        df_weather,
        x='city',
        y='temperature',
        labels={'temperature': 'Temperature (°C)', 'city': 'City'},
        title='Temperature Distribution by City'
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

# Detailed data table
st.subheader("Recent Weather Data")
st.dataframe(
    df_weather[['city', 'country', 'timestamp', 'temperature', 'humidity', 
               'pressure', 'wind_speed', 'weather_main', 'weather_description']]
    .sort_values(by='timestamp', ascending=False),
    use_container_width=True
)

# Footer
st.markdown("""---
*Data source: OpenWeatherMap API | Last updated: {}*

Dashboard created by: IRONalways17  
Current date: 2025-02-27 18:21:29 UTC
""".format(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')))