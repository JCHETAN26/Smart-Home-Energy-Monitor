import json
import base64
import datetime
import boto3
import os
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')


DYNAMODB_READINGS_TABLE_NAME = os.environ.get('DYNAMODB_READINGS_TABLE_NAME', 'SmartHomeReadings')
S3_PROCESSED_BUCKET_NAME = os.environ.get('S3_PROCESSED_BUCKET_NAME', 'your-smart-home-processed-data-bucket')
S3_RAW_BUCKET_NAME = os.environ.get('S3_RAW_BUCKET_NAME', 'your-smart-home-raw-data-bucket')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', 'arn:aws:sns:REGION:ACCOUNT_ID:smart-home-anomaly-alerts')


COST_PER_KWH_USD = Decimal('0.12') # $0.12 per kilowatt-hour


def detect_anomaly(reading):
    """
    Implements basic rule-based anomaly detection.
    """
    anomaly_detected = False
    anomaly_message = ""
    
    device_id = reading.get('device_id')
    consumption_kwh = reading.get('consumption_kwh') 
    status = reading.get('status')
    timestamp_str = reading.get('timestamp')
    outside_temp_f = reading.get('simulated_outside_temp_f')
    
    try:
        if status == "ANOMALY_SPIKE":
            anomaly_detected = True
            anomaly_message = f"Injected anomaly: Device {device_id} had an unusual spike in consumption."
        
        elif device_id == 'HVAC_001' and consumption_kwh > 2.5 and outside_temp_f is not None and 55 <= outside_temp_f <= 75:
            anomaly_detected = True
            anomaly_message = "HVAC running high during mild weather."
            
        elif "Lights" in device_id and status == "ON" and consumption_kwh < 0.005:
            anomaly_detected = True
            anomaly_message = "Very low light consumption while status is ON (possible malfunction)."
            
        elif consumption_kwh > 0.1 and status == "OFF" and status != "ANOMALY_SPIKE": 
            anomaly_detected = True
            anomaly_message = f"Device {device_id} consuming energy while reporting OFF."
            
        elif device_id == 'WaterHeater_Basement' and consumption_kwh > 1.5 and 0 <= datetime.datetime.fromisoformat(timestamp_str).hour < 5:
            anomaly_detected = True
            anomaly_message = "Water heater high consumption in early morning hours."

    except Exception as e:
        print(f"Error in anomaly detection for reading {reading}: {e}")
            
    return anomaly_detected, anomaly_message

def lambda_handler(event, context):
    dynamodb_readings_table = dynamodb.Table(DYNAMODB_READINGS_TABLE_NAME)
    
    dynamodb_batch_items = []
    s3_processed_records = []
    s3_raw_payloads = []

    print(f"Received {len(event['Records'])} records from Kinesis.")

    for record in event['Records']:
        raw_payload = record['kinesis']['data']
        decoded_payload = base64.b64decode(raw_payload).decode('utf-8')
        s3_raw_payloads.append(decoded_payload)

        try:
            reading = json.loads(decoded_payload)
            
          
            if not all(k in reading for k in ['timestamp', 'device_id', 'location', 'consumption_kwh', 'status']):
                print(f"Skipping malformed record (missing keys): {reading}")
                continue
            
            try:
                current_consumption_float = float(reading['consumption_kwh']) 
                reading['consumption_kwh'] = current_consumption_float 
            except ValueError:
                print(f"Skipping malformed record (invalid consumption_kwh): {reading}")
                continue

            try:
                dt_object = datetime.datetime.fromisoformat(reading['timestamp'])
                reading['timestamp'] = dt_object.isoformat() 
            except ValueError:
                print(f"Skipping malformed record (invalid timestamp format): {reading}")
                continue

         
            cost_usd = Decimal(str(reading['consumption_kwh'])) * COST_PER_KWH_USD
            reading['cost_usd'] = float(cost_usd) # Add to the reading dict for S3

            
            anomaly_detected, anomaly_message = detect_anomaly(reading)
            
            reading['anomaly_detected'] = anomaly_detected
            reading['anomaly_message'] = anomaly_message

           
            dynamodb_item = {
                'device_timestamp_id': f"{reading['device_id']}#{reading['timestamp']}",
                'timestamp': reading['timestamp'],
                'device_id': reading['device_id'],
                'location': reading['location'],
                'consumption_kwh': Decimal(str(reading['consumption_kwh'])),
                'cost_usd': Decimal(str(cost_usd)), # NEW: Add cost to DynamoDB item
                'status': reading['status'],
                'anomaly_detected': anomaly_detected,
                'anomaly_message': anomaly_message
            }
            if 'simulated_outside_temp_f' in reading and reading['simulated_outside_temp_f'] is not None:
                dynamodb_item['simulated_outside_temp_f'] = Decimal(str(reading['simulated_outside_temp_f']))
            if 'simulated_season' in reading and reading['simulated_season'] is not None:
                dynamodb_item['simulated_season'] = reading['simulated_season']

            dynamodb_batch_items.append(dynamodb_item)

            
            s3_processed_records.append(reading) 

            if anomaly_detected:
                try:
                    sns_client.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Message=f"ANOMALY ALERT: {anomaly_message} - Device: {reading['device_id']}, Consumption: {reading['consumption_kwh']} kWh, Time: {reading['timestamp']}",
                        Subject="Smart Home Energy Anomaly Detected!"
                    )
                    print(f"Sent SNS alert for device {reading['device_id']}: {anomaly_message}")
                except Exception as e:
                    print(f"Error sending SNS notification: {e}")

        except json.JSONDecodeError as e:
            print(f"Skipping malformed JSON record: {decoded_payload}. Error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred processing record: {decoded_payload}. Error: {e}")
            
    if dynamodb_batch_items:
        try:
            with dynamodb_readings_table.batch_writer() as batch:
                for item in dynamodb_batch_items:
                    batch.put_item(Item=item)
            print(f"Successfully batch-wrote {len(dynamodb_batch_items)} items to SmartHomeReadings.")
        except Exception as e:
            print(f"Error during SmartHomeReadings batch write: {e}")

    if s3_processed_records:
        current_date_path = datetime.datetime.now(datetime.timezone.utc).strftime('year=%Y/month=%m/day=%d')
        import uuid
        s3_key_processed = f"processed-energy-readings/{current_date_path}/{uuid.uuid4()}.jsonl"
        processed_data_body = "\n".join(json.dumps(record) for record in s3_processed_records)
        try:
            s3_client.put_object(
                Bucket=S3_PROCESSED_BUCKET_NAME,
                Key=s3_key_processed,
                Body=processed_data_body
            )
            print(f"Uploaded {len(s3_processed_records)} processed records to S3: s3://{S3_PROCESSED_BUCKET_NAME}/{s3_key_processed}")
        except Exception as e:
            print(f"Error uploading processed data to S3: {e}")

    if s3_raw_payloads:
        current_date_path = datetime.datetime.now(datetime.timezone.utc).strftime('year=%Y/month=%m/day=%d')
        import uuid
        s3_key_raw = f"raw-kinesis-payloads/{current_date_path}/{uuid.uuid4()}.jsonl"
        raw_data_body = "\n".join(s3_raw_payloads)
        try:
            s3_client.put_object(
                Bucket=S3_RAW_BUCKET_NAME,
                Key=s3_key_raw,
                Body=raw_data_body
            )
            print(f"Uploaded {len(s3_raw_payloads)} raw payloads to S3: s3://{S3_RAW_BUCKET_NAME}/{s3_key_raw}")
        except Exception as e:
            print(f"Error uploading raw data to S3: {e}")

    return {'statusCode': 200, 'body': f'Processed {len(event["Records"])} records from Kinesis.'}
