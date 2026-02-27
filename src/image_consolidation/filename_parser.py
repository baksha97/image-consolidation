"""Module for parsing dates from filenames when EXIF data is missing."""

import re
from datetime import datetime, timezone
from pathlib import Path

# Timezone handling with fallback - for environments without tzdata
def _get_tz(name: str):
    """Get a timezone by name, falling back to UTC if unavailable."""
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception:
        return timezone.utc

LOCAL_TZ = _get_tz("America/New_York")

def parse_filename_date(path_str: str) -> str | None:
    """
    Attempt to extract a date from the filename using known patterns.
    Returns an ISO8601 string (naive, in local time) or None.
    """
    filename = Path(path_str).name
    
    # 1. ISO UTC: 2016-02-21_06-19-23_UTC.mp4
    # Pattern: YYYY-MM-DD_HH-MM-SS_UTC
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})_UTC", filename)
    if m:
        try:
            dt_utc = datetime(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5)), int(m.group(6)),
                tzinfo=_get_tz("UTC")
            )
            # Convert to local and then make naive to match EXIF storage convention
            return dt_utc.astimezone(LOCAL_TZ).replace(tzinfo=None).isoformat()
        except (ValueError, OverflowError):
            pass

    # 2. macOS Screenshot: Screenshot 2022-04-10 at 8.50.59 AM.jpeg
    # Handle thin space (\u202f) or regular space before AM/PM
    m = re.search(r"Screenshot (\d{4})-(\d{2})-(\d{2}) at (\d{1,2})\.(\d{2})\.(\d{2})\s?([AP]M)", filename, re.IGNORECASE)
    if m:
        try:
            year, month, day, hour_str, minute, second, ampm = m.groups()
            hour = int(hour_str)
            if ampm.upper() == "PM" and hour < 12:
                hour += 12
            elif ampm.upper() == "AM" and hour == 12:
                hour = 0
            dt = datetime(int(year), int(month), int(day), hour, int(minute), int(second))
            return dt.isoformat()
        except (ValueError, OverflowError):
            pass

    # 3. Android/Other Screenshot: Screenshot_2016-04-10-00-07-13.png
    m = re.search(r"Screenshot_(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})", filename)
    if m:
        try:
            dt = datetime(*map(int, m.groups()))
            return dt.isoformat()
        except (ValueError, OverflowError):
            pass

    # 4. Clipboard: clipboard_2025-12-24_13-36.png
    m = re.search(r"clipboard_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})", filename)
    if m:
        try:
            dt = datetime(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5))
            )
            return dt.isoformat()
        except (ValueError, OverflowError):
            pass

    # 5. Dell Webcam: Dell Webcam Center - Photo Capture - 20100406 -.jpg
    m = re.search(r"Dell Webcam Center - .* - (\d{4})(\d{2})(\d{2})", filename)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return dt.isoformat()
        except (ValueError, OverflowError):
            pass

    # 6. RPReplay: RPReplay_Final1607278603.mp4 (Unix timestamp)
    m = re.search(r"RPReplay_Final(\d{10})", filename)
    if m:
        try:
            ts = int(m.group(1))
            dt_utc = datetime.fromtimestamp(ts, tz=_get_tz("UTC"))
            return dt_utc.astimezone(LOCAL_TZ).replace(tzinfo=None).isoformat()
        except (ValueError, OverflowError, OSError):
            pass

    # 7. Month-name: "Sep 21 2009", "Sep 14 2009" (local time)
    _MONTHS = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})\s+(\d{4})", filename, re.IGNORECASE)
    if m:
        try:
            month = _MONTHS[m.group(1).lower()]
            dt = datetime(int(m.group(3)), month, int(m.group(2)))
            return dt.isoformat()
        except (ValueError, OverflowError):
            pass

    # 8. Simple YYYY-MM-DD pattern often found in various apps
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", filename)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return dt.isoformat()
        except (ValueError, OverflowError):
            pass

    return None
