import pytest
from image_consolidation.filename_parser import parse_filename_date

def test_iso_utc_standard_time():
    # Standard time (Jan) - America/New_York is UTC-5
    # 2016-01-21 12:00:00 UTC -> 2016-01-21 07:00:00 EST
    filename = "2016-01-21_12-00-00_UTC.mp4"
    assert parse_filename_date(filename) == "2016-01-21T07:00:00"

def test_iso_utc_daylight_time():
    # Daylight time (Jun) - America/New_York is UTC-4
    # 2016-06-21 12:00:00 UTC -> 2016-06-21 08:00:00 EDT
    filename = "2016-06-21_12-00-00_UTC.mp4"
    assert parse_filename_date(filename) == "2016-06-21T08:00:00"

def test_macos_screenshot():
    # Screenshot 2022-04-10 at 8.50.59 AM.jpeg
    filename = "Screenshot 2022-04-10 at 8.50.59 AM.jpeg"
    assert parse_filename_date(filename) == "2022-04-10T08:50:59"

    filename = "Screenshot 2022-04-10 at 8.50.59 PM.jpeg"
    assert parse_filename_date(filename) == "2022-04-10T20:50:59"

    # With thin space (\u202f)
    filename = "Screenshot 2024-09-03 at 2.46.21\u202fPM.png"
    assert parse_filename_date(filename) == "2024-09-03T14:46:21"

def test_android_screenshot():
    # Screenshot_2016-04-10-00-07-13.png
    filename = "Screenshot_2016-04-10-00-07-13.png"
    assert parse_filename_date(filename) == "2016-04-10T00:07:13"

def test_clipboard():
    # clipboard_2025-12-24_13-36.png
    filename = "clipboard_2025-12-24_13-36.png"
    assert parse_filename_date(filename) == "2025-12-24T13:36:00"

def test_dell_webcam():
    # Dell Webcam Center - Photo Capture - 20100406 -.jpg
    filename = "Dell Webcam Center - Photo Capture - 20100406 -.jpg"
    assert parse_filename_date(filename) == "2010-04-06T00:00:00"

def test_rpreplay():
    # RPReplay_Final1607278603.mp4
    # 1607278603 is 2020-12-06 18:16:43 UTC
    # Dec 6 is Standard Time (EST, -5) -> 13:16:43
    filename = "RPReplay_Final1607278603.mp4"
    assert parse_filename_date(filename) == "2020-12-06T13:16:43"

    # RPReplay_Final1620576310.mp4
    # 1620576310 is 2021-05-09 16:05:10 UTC
    # May 9 is Daylight Time (EDT, -4) -> 12:05:10
    filename = "RPReplay_Final1620576310.mp4"
    assert parse_filename_date(filename) == "2021-05-09T12:05:10"

def test_simple_date_pattern():
    # IMG_2023-01-01.jpg
    filename = "IMG_2023-01-01.jpg"
    assert parse_filename_date(filename) == "2023-01-01T00:00:00"

def test_invalid_dates():
    assert parse_filename_date("IMG_001.jpg") is None
    assert parse_filename_date("2023-13-40.jpg") is None # Invalid month/day
