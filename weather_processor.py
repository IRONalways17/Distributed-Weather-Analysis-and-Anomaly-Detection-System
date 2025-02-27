    # Define anomalies schema (subset of weather data with anomaly info)
    anomalies_schema = {
        'fields': [
            {'name': 'city', 'type': 'STRING'},
            {'name': 'country', 'type': 'STRING'},
            {'name': 'latitude', 'type': 'FLOAT'},
            {'name': 'longitude', 'type': 'FLOAT'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'},
            {'name': 'processing_timestamp', 'type': 'TIMESTAMP'},
            {'name': 'temperature', 'type': 'FLOAT'},
            {'name': 'weather_main', 'type': 'STRING'},
            {'name': 'weather_description', 'type': 'STRING'},
            {'name': 'is_anomaly', 'type': 'BOOLEAN'},
            {'name': 'anomaly_reason', 'type': 'STRING'},
            {'name': 'anomaly_z_score', 'type': 'FLOAT'},
            {'name': 'anomaly_type', 'type': 'STRING'},
            {'name': 'detected_at', 'type': 'TIMESTAMP'}
        ]
    }
    
    # Create pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Read messages from Pub/Sub
        messages = (
            pipeline
            | 'Read from Pub/Sub' >> ReadFromPubSub(
                subscription=f'projects/your-gcp-project-id/subscriptions/weather-data-sub')
        )
        
        # Parse and process weather data
        processed_data = (
            messages
            | 'Parse JSON' >> beam.ParDo(ParseJsonDoFn())
            | 'Enrich Weather Data' >> beam.ParDo(EnrichWeatherDataFn())
            | 'Detect Anomalies' >> beam.ParDo(DetectAnomaliesFn(), historical_stats=historical_stats)
        )
        
        # Write all processed data to BigQuery
        processed_data | 'Write to BigQuery' >> WriteToBigQuery(
            table='your-gcp-project-id:weather_dataset.weather_data',
            schema=weather_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        
        # Filter and write anomalies to a separate table
        anomalies = (
            processed_data
            | 'Filter Anomalies' >> beam.Filter(lambda record: record.get('is_anomaly', False) == True)
            | 'Add Detection Time' >> beam.Map(lambda record: {
                **{k: record[k] for k in [
                    'city', 'country', 'latitude', 'longitude', 'timestamp',
                    'processing_timestamp', 'temperature', 'weather_main',
                    'weather_description', 'is_anomaly', 'anomaly_reason',
                    'anomaly_z_score', 'anomaly_type'
                ] if k in record},
                'detected_at': datetime.datetime.utcnow().isoformat()
            })
        )
        
        anomalies | 'Write Anomalies to BigQuery' >> WriteToBigQuery(
            table='your-gcp-project-id:weather_dataset.weather_anomalies',
            schema=anomalies_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        
        # Also publish anomalies to a Pub/Sub topic for alerting
        anomalies | 'Format Anomaly Alerts' >> beam.Map(
            lambda record: json.dumps(record).encode('utf-8')
        ) | 'Publish Anomalies' >> beam.io.WriteToPubSub(
            topic='projects/your-gcp-project-id/topics/weather-anomalies'
        )

if __name__ == '__main__':
    run_pipeline()