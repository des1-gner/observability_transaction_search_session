import boto3
from datetime import datetime, timedelta
import time
import json

SESSION_ID = "<your-session-id>"

logs = boto3.client('logs', region_name='us-east-1')

# Query all fields
response = logs.start_query(
    logGroupName='aws/spans',
    startTime=int((datetime.now() - timedelta(hours=24)).timestamp()),
    endTime=int(datetime.now().timestamp()),
    queryString=f"""fields @timestamp, @message
| filter attributes.session.id = "{SESSION_ID}"
| sort @timestamp asc
| limit 10000"""
)

query_id = response['queryId']
print(f"Query started: {query_id}\n")

# Wait for results
while True:
    result = logs.get_query_results(queryId=query_id)
    
    if result['status'] == 'Complete':
        print(f"Found {len(result['results'])} spans\n")
        print("="*120)
        
        for i, span in enumerate(result['results'], 1):
            data = {f['field']: f['value'] for f in span}
            
            # Parse the @message field which contains full span data
            try:
                span_data = json.loads(data.get('@message', '{}'))
                
                print(f"\n{'='*120}")
                print(f"SPAN {i}")
                print(f"{'='*120}")
                print(f"Timestamp: {data.get('@timestamp')}")
                print(f"Trace ID: {span_data.get('traceId')}")
                print(f"Span ID: {span_data.get('spanId')}")
                print(f"Name: {span_data.get('name')}")
                print(f"Kind: {span_data.get('kind')}")
                
                # Duration
                duration_nano = span_data.get('durationNano')
                if duration_nano:
                    print(f"Duration: {float(duration_nano)/1_000_000:.2f} ms")
                
                # Status
                status = span_data.get('status', {})
                print(f"Status: {status.get('code', 'UNSET')}")
                
                # Attributes
                attributes = span_data.get('attributes', {})
                if attributes:
                    print(f"\nAttributes:")
                    for key, value in attributes.items():
                        str_val = str(value)
                        if len(str_val) > 200:
                            str_val = str_val[:200] + "..."
                        print(f"  • {key}: {str_val}")
                
                # Resource
                resource = span_data.get('resource', {})
                if resource:
                    print(f"\nResource:")
                    for key, value in resource.items():
                        print(f"  • {key}: {value}")
                
                # Events
                events = span_data.get('events', [])
                if events:
                    print(f"\nEvents ({len(events)}):")
                    for event in events:
                        print(f"  • {event.get('name')}")
                        event_attrs = event.get('attributes', {})
                        if event_attrs:
                            for k, v in event_attrs.items():
                                str_v = str(v)
                                if len(str_v) > 150:
                                    str_v = str_v[:150] + "..."
                                print(f"      {k}: {str_v}")
                
                # Links
                links = span_data.get('links', [])
                if links:
                    print(f"\nLinks ({len(links)}):")
                    for link in links:
                        print(f"  • Trace: {link.get('traceId')}, Span: {link.get('spanId')}")
                
                print()
                
            except json.JSONDecodeError:
                print(f"\nSpan {i}: Unable to parse span data")
                print(f"Raw: {data.get('@message', '')[:500]}")
        
        print("="*120)
        
        # Export to JSON
        output = {
            'session_id': SESSION_ID,
            'query_time': datetime.now().isoformat(),
            'span_count': len(result['results']),
            'spans': []
        }
        
        for span in result['results']:
            data = {f['field']: f['value'] for f in span}
            try:
                span_data = json.loads(data.get('@message', '{}'))
                output['spans'].append(span_data)
            except:
                pass
        
        filename = f"session_{SESSION_ID}_full.json"
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"\nExported full details to: {filename}")
        
        break
    elif result['status'] == 'Failed':
        print(f"Query failed: {result}")
        break
    
    time.sleep(1)
