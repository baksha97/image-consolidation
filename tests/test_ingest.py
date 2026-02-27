import json
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime
from image_consolidation.ingest import _video_metadata_ffprobe, _extract_metadata
from image_consolidation.config import Config

@patch("shutil.which")
@patch("subprocess.check_output")
def test_video_metadata_ffprobe(mock_check_output, mock_which):
    mock_which.return_value = "/usr/bin/ffprobe"
    
    # Mock output for a 1080p video with duration 10.5s and creation_time
    mock_data = {
        "format": {
            "duration": "10.5",
            "tags": {
                "creation_time": "2021-05-10T12:34:56.000000Z"
            }
        },
        "streams": [
            {
                "codec_type": "video",
                "width": 1920,
                "height": 1080
            }
        ]
    }
    mock_check_output.return_value = json.dumps(mock_data).encode("utf-8")

    result = _video_metadata_ffprobe(Path("test.mp4"))
    
    assert result["duration_sec"] == 10.5
    assert result["width"] == 1920
    assert result["height"] == 1080
    # UTC 12:34:56 on May 10 -> EDT -4 -> 08:34:56
    assert result["exif_date"] == "2021-05-10T08:34:56"

@patch("shutil.which")
@patch("subprocess.check_output")
def test_video_metadata_ffprobe_with_rotation(mock_check_output, mock_which):
    mock_which.return_value = "/usr/bin/ffprobe"
    
    # Video with 90 degree rotation
    mock_data = {
        "format": {"duration": "5.0"},
        "streams": [
            {
                "codec_type": "video",
                "width": 1920,
                "height": 1080,
                "tags": {"rotate": "90"}
            }
        ]
    }
    mock_check_output.return_value = json.dumps(mock_data).encode("utf-8")

    result = _video_metadata_ffprobe(Path("rotated.mp4"))
    
    # Width and height should be swapped
    assert result["width"] == 1080
    assert result["height"] == 1920

@patch("image_consolidation.ingest._video_metadata_ffprobe")
@patch("pathlib.Path.stat")
def test_extract_metadata_video_fallback(mock_stat, mock_ffprobe):
    # Mock stat for file
    mock_stat_res = MagicMock()
    mock_stat_res.st_size = 1000
    mock_stat_res.st_mtime = 123456789.0
    mock_stat.return_value = mock_stat_res

    # Mock ffprobe returning NO date
    mock_ffprobe.return_value = {
        "duration_sec": 10.0,
        "width": 1280,
        "height": 720
    }

    cfg = Config()
    
    # Test file with date in name
    path = Path("2016-02-21_06-19-23_UTC.mp4")
    
    # We need to mock _find_source too as it uses cfg.sources.paths which might be empty
    with patch("image_consolidation.ingest._find_source") as mock_find_source:
        mock_find_source.return_value = "/src"
        rec = _extract_metadata(path, cfg)

    assert rec.is_video is True
    assert rec.duration_sec == 10.0
    assert rec.width == 1280
    # UTC 06:19:23 on Feb 21 -> EST -5 -> 01:19:23
    assert rec.exif_date == "2016-02-21T01:19:23"

@patch("image_consolidation.ingest._exif_from_pillow")
@patch("pathlib.Path.stat")
def test_extract_metadata_image_fallback(mock_stat, mock_pillow):
    # Mock stat
    mock_stat_res = MagicMock()
    mock_stat_res.st_size = 500
    mock_stat_res.st_mtime = 123456789.0
    mock_stat.return_value = mock_stat_res

    # Pillow returns dimensions but NO date
    mock_pillow.return_value = {
        "width": 800,
        "height": 600
    }

    cfg = Config()
    
    # Test screenshot
    path = Path("Screenshot 2022-04-10 at 8.50.59 AM.jpeg")
    
    with patch("image_consolidation.ingest._find_source") as mock_find_source:
        mock_find_source.return_value = "/src"
        rec = _extract_metadata(path, cfg)

    assert rec.is_video is False
    assert rec.width == 800
    assert rec.exif_date == "2022-04-10T08:50:59"
