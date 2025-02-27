    def publish_to_topic(self, data_path, data_type):
        """Publish a message to Pub/Sub with the path to the saved data"""
        message = {
            'data_path': data_path,
            'data_type': data_type,
            'collected_at': datetime.datetime.utcnow().isoformat(),
            'record_count': len(data_path),
            'user': 'IRONalways17',  # Using the provided login
            'timestamp': '2025-02-27 18:19:45'  # Using the provided timestamp
        }
        
        # Convert message to JSON string and then to bytes
        message_json = json.dumps(message)
        message_bytes = message_json.encode('utf-8')
        
        # Publish message
        future = self.publisher.publish(self.topic_path, data=message_bytes)
        message_id = future.result()
        
        print(f"Published message {message_id} to {self.topic_path}")
        return message_id
    
    def run_collection_cycle(self):
        """Run a complete data collection cycle"""
        print(f"Starting weather data collection at {datetime.datetime.utcnow().isoformat()}")
        
        # Collect current weather data
        weather_data = self.collect_openweathermap_data()
        if weather_data:
            weather_path = self.save_to_gcs(weather_data, 'current_weather')
            self.publish_to_topic(weather_path, 'current_weather')
        
        # Collect forecast data
        forecast_data = self.collect_forecast_data()
        if forecast_data:
            forecast_path = self.save_to_gcs(forecast_data, 'forecast')
            self.publish_to_topic(forecast_path, 'forecast')
        
        print(f"Completed weather data collection at {datetime.datetime.utcnow().isoformat()}")

def main():
    # Configuration
    api_key = os.environ.get('OPENWEATHERMAP_API_KEY')
    gcp_project_id = os.environ.get('GCP_PROJECT_ID')
    bucket_name = os.environ.get('GCS_BUCKET_NAME')
    topic_name = os.environ.get('PUBSUB_TOPIC_NAME')
    
    # Sample locations (city, country, latitude, longitude)
    locations = [
        ('New York', 'US', 40.7128, -74.0060),
        ('London', 'GB', 51.5074, -0.1278),
        ('Tokyo', 'JP', 35.6762, 139.6503),
        ('Sydney', 'AU', -33.8688, 151.2093),
        ('Rio de Janeiro', 'BR', -22.9068, -43.1729),
        ('Cape Town', 'ZA', -33.9249, 18.4241),
        ('Moscow', 'RU', 55.7558, 37.6173),
        ('Dubai', 'AE', 25.2048, 55.2708),
        ('Mumbai', 'IN', 19.0760, 72.8777),
        ('Beijing', 'CN', 39.9042, 116.4074)
    ]
    
    # Create and run collector
    collector = WeatherDataCollector(
        api_key=api_key,
        locations=locations,
        gcp_project_id=gcp_project_id,
        bucket_name=bucket_name,
        topic_name=topic_name
    )
    
    # Run collection once
    collector.run_collection_cycle()
    
    # For continuous collection, uncomment the following:
    # while True:
    #     collector.run_collection_cycle()
    #     # Sleep for 30 minutes
    #     time.sleep(30 * 60)

if __name__ == "__main__":
    main()