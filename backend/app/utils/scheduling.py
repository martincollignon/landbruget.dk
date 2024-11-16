from datetime import datetime, timedelta

def get_next_monday_timestamp() -> str:
    """Calculate the timestamp for next Monday 2 AM UTC"""
    now = datetime.utcnow()
    days_ahead = 7 - now.weekday()  # 0 = Monday, so this gets us to next Monday
    if days_ahead <= 0:  # If today is Monday, get next Monday
        days_ahead += 7
    next_monday = now + timedelta(days=days_ahead)
    next_update = next_monday.replace(hour=2, minute=0, second=0, microsecond=0)
    return next_update.isoformat()

def get_data_freshness(timestamp: datetime) -> str:
    """Return a human-readable description of data freshness"""
    now = datetime.utcnow()
    age = now - timestamp
    if age.days < 7:
        return "current"
    elif age.days < 14:
        return "last week"
    else:
        return f"{age.days // 7} weeks old" 