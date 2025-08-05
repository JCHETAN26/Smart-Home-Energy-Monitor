import json
import boto3
import os
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
DYNAMODB_READINGS_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'SmartHomeReadings')

def decimal_to_float(obj):
    """Helper to convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def get_recent_readings(table):
    """Fetches recent readings for display on the dashboard."""
    # Fetch 24 hours of data for more robust comparison logic in frontend
    twenty_four_hours_ago = datetime.utcnow() - timedelta(hours=24) 
    response = table.scan(
        FilterExpression=Key('timestamp').gte(twenty_four_hours_ago.isoformat()),
        Limit=5000 # Increased limit
    )
    items = response.get('Items', [])
    items.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
    return items

def get_daily_summary(table):
    """
    Calculates daily total consumption and cost, plus peak device usage.
    Returns data for the last 7 days.
    """
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    
    response = table.scan(
        FilterExpression=Attr('timestamp').gte(seven_days_ago.isoformat())
    )
    all_readings = response.get('Items', [])

    daily_totals_kwh = {}
    daily_totals_cost = {}
    device_daily_totals = {}
    
    for item in all_readings:
        timestamp_dt = datetime.fromisoformat(item['timestamp'])
        date_str = timestamp_dt.strftime('%Y-%m-%d')
        device_id = item['device_id']
        consumption = float(item['consumption_kwh'])
        cost = float(item.get('cost_usd', 0))

        daily_totals_kwh[date_str] = daily_totals_kwh.get(date_str, 0) + consumption
        daily_totals_cost[date_str] = daily_totals_cost.get(date_str, 0) + cost
        
        if date_str not in device_daily_totals:
            device_daily_totals[date_str] = {}
        device_daily_totals[date_str][device_id] = device_daily_totals[date_str].get(device_id, 0) + consumption
    
    summaries = []
    for date_str in sorted(daily_totals_kwh.keys()):
        peak_device = None
        peak_consumption = 0
        if date_str in device_daily_totals:
            for dev, total in device_daily_totals[date_str].items():
                if total > peak_consumption:
                    peak_consumption = total
                    peak_device = dev

        summaries.append({
            'date': date_str,
            'total_consumption_kwh': round(daily_totals_kwh[date_str], 3),
            'total_cost_usd': round(daily_totals_cost[date_str], 3),
            'peak_device_daily': peak_device,
            'peak_device_consumption_daily': round(peak_consumption, 3)
        })
    return summaries

def get_smart_suggestions(recent_readings, daily_summaries):
    suggestions = []
    night_consumption = 0
    for reading in recent_readings:
        timestamp_dt = datetime.fromisoformat(reading['timestamp'])
        if 23 <= timestamp_dt.hour or timestamp_dt.hour < 6:
            if "Lights" in reading['device_id']:
                 night_consumption += float(reading['consumption_kwh'])
            elif reading['device_id'] == 'TV_LivingRoom' and reading['status'] == 'ON':
                 suggestions.append("Consider turning off the living room TV late at night.")
    if night_consumption > 1.0:
        suggestions.append("High light consumption detected during night hours. Remember to turn off unused lights!")
    hvac_recent_high_mild = False
    for reading in recent_readings:
        if reading['device_id'] == 'HVAC_001' and reading.get('anomaly_detected') and "mild weather" in reading.get('anomaly_message', ''):
            hvac_recent_high_mild = True
            break
    if hvac_recent_high_mild:
        suggestions.append("HVAC is consuming more than expected during mild weather. Check insulation or consider smart thermostat settings.")
    for summary in daily_summaries:
        if summary.get('total_consumption_kwh', 0) > 15.0 and summary['date'] == datetime.utcnow().strftime('%Y-%m-%d'):
            if "Fridge" in summary.get('peak_device_daily', ''):
                suggestions.append("Your refrigerator seems to be a consistent high consumer. Check its seals and temperature settings.")
                break
    if not suggestions:
        suggestions.append("No immediate issues detected. Keep monitoring your energy use!")
        suggestions.append("Unplug electronics when not in use to reduce 'vampire' energy draw.")
    return suggestions

def lambda_handler(event, context):
    table = dynamodb.Table(DYNAMODB_READINGS_TABLE_NAME)
    
    path = event.get('path', '/')
    query_params = event.get('queryStringParameters', {})
    
    response_body = {}
    status_code = 200

    try:
        if path == '/data':
            recent_readings = get_recent_readings(table)
            daily_summaries = get_daily_summary(table)
            smart_suggestions = get_smart_suggestions(recent_readings, daily_summaries)

            # NEW: Compute consumption by device for the last 24 hours
            consumption_by_device = {}
            for reading in recent_readings:
                device_id = reading['device_id']
                consumption_by_device[device_id] = (consumption_by_device.get(device_id, 0) + float(reading['consumption_kwh']))
            
            response_body = {
                'recentReadings': recent_readings,
                'dailySummaries': daily_summaries,
                'anomalies': [r for r in recent_readings if r.get('anomaly_detected')],
                'smartSuggestions': smart_suggestions,
                'consumptionByDevice': consumption_by_device
            }

        else:
            status_code = 404
            response_body = {"error": "Not Found", "message": "Invalid API endpoint."}

    except Exception as e:
        print(f"Error processing API request for path {path}: {e}")
        status_code = 500
        response_body = {"error": str(e), "message": "Failed to retrieve or process data for API."}

    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET,OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'
        },
        'body': json.dumps(response_body, default=decimal_to_float)
    }