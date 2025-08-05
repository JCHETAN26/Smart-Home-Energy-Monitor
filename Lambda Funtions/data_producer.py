import json
import datetime
import random
import boto3
import os

# Initialize Kinesis client outside the handler for better performance (Lambda reuses connections)
kinesis_client = boto3.client('kinesis')

# --- Configuration (can be set as Lambda Environment Variables later) ---
STREAM_NAME = os.environ.get('KINESIS_STREAM_NAME', 'smart-home-energy-stream-cj-250722') 
NUM_RECORDS_PER_INVOCATION = int(os.environ.get('NUM_RECORDS_PER_INVOCATION', '10')) 

# --- Anomaly Injection Configuration ---
ANOMALY_INJECTION_PROBABILITY = 0.03 # 3% chance for a record to be an artificial anomaly

# --- MODIFIED FUNCTION DEFINITION ---
def generate_device_reading(device_id, location, timestamp, hour_of_day, outside_temp_f=None, season=None):
    """Generates a single simulated energy reading for a device."""
    
    consumption_kwh = 0.0 
    status = "OFF"
    
    # --- Existing consumption logic for various devices (now using hour_of_day) ---
    if "HVAC" in device_id:
        if outside_temp_f is not None:
            if outside_temp_f > 85: # Hot day
                consumption_kwh = round(random.uniform(1.5, 5.0), 2)
                status = "COOLING"
            elif outside_temp_f < 40: # Cold day
                consumption_kwh = round(random.uniform(1.5, 5.0), 2)
                status = "HEATING"
            else: # Mild day
                consumption_kwh = round(random.uniform(0.1, 1.0), 2)
                status = "FAN_ONLY" if consumption_kwh > 0.1 else "OFF"
        else: # Fallback if no temp (shouldn't happen with simulated_outside_temp_f)
             if 6 <= hour_of_day <= 9 or 17 <= hour_of_day <= 22: # Morning/Evening use
                consumption_kwh = round(random.uniform(0.5, 3.0), 2)
                status = "ACTIVE" if consumption_kwh > 0.5 else "STANDBY"
             else:
                consumption_kwh = round(random.uniform(0.01, 0.2), 2)
                status = "STANDBY"
                
    elif "Lights" in device_id:
        if 18 <= hour_of_day < 23: # Evening peak
            consumption_kwh = round(random.uniform(0.1, 0.5), 2)
            status = "ON"
        elif 23 <= hour_of_day or hour_of_day < 6: # Late night / early morning
            consumption_kwh = round(random.uniform(0.01, 0.1), 2)
            status = "DIM" if consumption_kwh > 0.01 else "OFF"
        else:
            consumption_kwh = round(random.uniform(0.001, 0.005), 2) # Minimal idle
            status = "OFF"

    elif "Fridge" in device_id:
        consumption_kwh = round(random.uniform(0.05, 0.2), 2) # Constant low draw
        status = "ON"

    elif "WaterHeater" in device_id:
        if (6 <= hour_of_day <= 9 and random.random() < 0.6) or \
           (18 <= hour_of_day <= 21 and random.random() < 0.4): # Higher chance of use
            consumption_kwh = round(random.uniform(0.8, 3.0), 2)
            status = "HEATING"
        else:
            consumption_kwh = round(random.uniform(0.02, 0.1), 2) # Idle draw
            status = "STANDBY"
            
    else: # Generic Appliance (random on/off with varied consumption)
        if random.random() < 0.4: # 40% chance of being active
            consumption_kwh = round(random.uniform(0.05, 1.5), 2)
            status = "ACTIVE"
        else:
            consumption_kwh = round(random.uniform(0.001, 0.01), 2) # Idle draw
            status = "OFF"
    
    # --- NEW: Inject an anomaly randomly ---
    if random.random() < ANOMALY_INJECTION_PROBABILITY:
        consumption_kwh = round(random.uniform(5.0, 15.0), 2) # Very high KWH
        status = "ANOMALY_SPIKE" 
        print(f"ANOMALY INJECTED: Device {device_id} at {timestamp.isoformat()} with {consumption_kwh} kWh") 

    reading = {
        "timestamp": timestamp.isoformat(),
        "device_id": device_id,
        "location": location,
        "consumption_kwh": consumption_kwh,
        "status": status,
        "simulated_outside_temp_f": outside_temp_f,
        "simulated_season": season
    }
    return reading

def lambda_handler(event, context):
    readings = []
    current_time = datetime.datetime.now(datetime.timezone.utc) 

    # --- Pass current_hour to the generator ---
    current_hour = current_time.hour # Get hour here

    # Simulate outside temperature based on "season" (basic simulation)
    season_map = {
        (12,1,2): "Winter", (3,4,5): "Spring", (6,7,8): "Summer", (9,10,11): "Fall"
    }
    current_month = current_time.month
    simulated_season = "Unknown"
    for months, season_name in season_map.items():
        if current_month in months:
            simulated_season = season_name
            break
            
    simulated_outside_temp_f = 0
    if simulated_season == "Summer":
        simulated_outside_temp_f = random.uniform(75, 105)
    elif simulated_season == "Winter":
        simulated_outside_temp_f = random.uniform(20, 50)
    elif simulated_season == "Spring" or simulated_season == "Fall":
        simulated_outside_temp_f = random.uniform(50, 80)
    simulated_outside_temp_f = round(simulated_outside_temp_f, 1)

    # Define a set of typical devices
    devices = [
        {"id": "HVAC_001", "loc": "MainHouse"},
        {"id": "Lights_LivingRoom", "loc": "LivingRoom"},
        {"id": "Lights_Kitchen", "loc": "Kitchen"},
        {"id": "Fridge_Main", "loc": "Kitchen"},
        {"id": "WaterHeater_Basement", "loc": "Basement"},
        {"id": "TV_LivingRoom", "loc": "LivingRoom"},
        {"id": "Computer_Office", "loc": "Office"},
        {"id": "Dishwasher_Kitchen", "loc": "Kitchen"}
    ]

    for _ in range(NUM_RECORDS_PER_INVOCATION):
        for device in devices:
            # --- MODIFIED FUNCTION CALL ---
            readings.append(generate_device_reading(
                device["id"], device["loc"], current_time, current_hour, # Pass current_hour here
                simulated_outside_temp_f, simulated_season
            ))
            current_time += datetime.timedelta(seconds=random.uniform(0.1, 0.5)) 
    
    kinesis_records = []
    for reading in readings:
        kinesis_records.append({
            'Data': json.dumps(reading),
            'PartitionKey': reading['device_id'] 
        })

    try:
        response = kinesis_client.put_records(
            StreamName=STREAM_NAME,
            Records=kinesis_records
        )
        failed_record_count = response.get('FailedRecordCount', 0)
        if failed_record_count > 0:
            print(f"WARNING: Failed to put {failed_record_count} records to Kinesis.")
            for record_response in response['Records']:
                if 'ErrorCode' in record_response:
                    print(f"  Error: {record_response.get('ErrorCode')}, Message: {record_response.get('ErrorMessage')}")
        
        print(f"Published {len(readings) - failed_record_count}/{len(readings)} records to Kinesis stream '{STREAM_NAME}'.")
    except Exception as e:
        print(f"ERROR: Could not publish records to Kinesis: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully attempted to publish {len(readings)} records to Kinesis.')
    }