#####################################################################
# db_utils/helpers.py
#
# This file contains helper functions for managing database operations
# and processing user event data.
#####################################################################

from .crud import get_or_create_user, get_or_create_session, create_event
from datetime import datetime
import pandas as pd
import uuid


def convert_timestamp(ts):
    """
    Convert various timestamp formats to a UNIX timestamp.
    """
    if isinstance(ts, pd.Timestamp):
        return int(ts.timestamp())
    elif isinstance(ts, datetime):
        return int(ts.timestamp())
    elif isinstance(ts, str):
        return int(datetime.fromisoformat(ts).timestamp())
    else:
        raise ValueError(f"Unknown timestamp type: {type(ts)}")


def get_valid_session_id(session_id):
    """
    This function is needed because all data is generated via a LLaMA ML model,
    and sometimes we receive session IDs in an incorrect format.
    To prevent the application from crashing, this function checks the validity
    of a session ID. If the session ID is invalid, the entire session and
    all associated data are skipped.
    """
    try:
        return uuid.UUID(session_id)
    except (ValueError, TypeError):
        return None


def extract_event_info(record):
    """
    function to extract event data
    """
    timestamp = convert_timestamp(record['timestamp'])
    event_type = record['event_type']
    browser = record.get('browser')
    device = record.get('device')

    return timestamp, event_type, browser, device


def process_records(db, files_data):
    """
    Process a list of user event records from multiple files.

    for each record, the function:
    - Validates the session ID and skips invalid sessions.
    - Converts timestamps to a standard format.
    - Retrieves or creates the corresponding User.
    - Retrieves or creates the corresponding Session.
    - Creates an Event linked to the session.

    All valid events are committed to the database in a single transaction.
    Skipped sessions are logged and returned.
    """
    users_cache = {}
    sessions_cache = {}
    skipped_sessions = []

    for file in files_data:
        for record in file['data']:
            user_id = record['user_id']

            # Validate session ID
            session_id = get_valid_session_id(record.get('session_id'))
            if session_id is None:
                skipped_sessions.append(record.get('session_id'))
                print(f"Skipping invalid session and all its events: {record.get('session_id')}")
                continue

            # Extract event info
            timestamp, event_type, browser, device = extract_event_info(record)

            # Get or create user
            user = get_or_create_user(db, user_id, users_cache)

            # Get or create session
            session = get_or_create_session(db, session_id, sessions_cache, user, browser, device)

            # Create event
            create_event(db, session, timestamp, event_type)

    db.commit()

    if skipped_sessions:
        print(f"Skipped {len(skipped_sessions)} invalid sessions: {skipped_sessions}")

    return skipped_sessions