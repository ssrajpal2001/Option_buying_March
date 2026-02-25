
import asyncio
from datetime import datetime, timedelta
import pandas as pd
import pytz

class MockAggregator:
    def __init__(self, interval_minutes):
        self.interval_minutes = interval_minutes
        self.history = {}

async def test_priming_logic():
    kolkata = pytz.timezone('Asia/Kolkata')
    instrument_key = "TEST_INST"
    interval = 1
    
    # Simulate existing history
    last_ts = kolkata.localize(datetime(2026, 2, 20, 10, 0))
    aggregator = MockAggregator(interval)
    aggregator.history[instrument_key] = [
        {'timestamp': last_ts - timedelta(minutes=i)} for i in range(5, -1, -1)
    ]
    
    # current_timestamp
    timestamp = kolkata.localize(datetime(2026, 2, 20, 10, 1, 30))
    
    # Logic from DataManager
    if instrument_key in aggregator.history and len(aggregator.history[instrument_key]) >= 5:
        history_last_ts = aggregator.history[instrument_key][-1]['timestamp']
        if history_last_ts.tzinfo is None:
            history_last_ts = kolkata.localize(history_last_ts)
        else:
            history_last_ts = history_last_ts.astimezone(kolkata)
            
        diff = (timestamp - history_last_ts).total_seconds()
        print(f"Diff: {diff}s, Limit: {interval * 120}s")
        if diff <= (interval * 120):
            print("SKIP PRIMING")
        else:
            print("PROCEED PRIMING")

if __name__ == "__main__":
    asyncio.run(test_priming_logic())
