import pandas as pd
from datetime import datetime

def convert_timestamp(ts):
    if isinstance(ts, pd.Timestamp):
        return int(ts.timestamp())
    elif isinstance(ts, datetime):
        return int(ts.timestamp())
    elif isinstance(ts, str):
        return int(datetime.fromisoformat(ts).timestamp())
    else:
        raise ValueError(f"Unknown timestamp type: {type(ts)}")