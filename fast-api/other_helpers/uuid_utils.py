import uuid

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