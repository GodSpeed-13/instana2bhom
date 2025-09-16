import configparser
import json
import logging
import os
import inspect
import requests
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

def validate_path(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path does not exist: {path}")
    return True

def read_json_cfg(path):
    with open(path, 'r') as f:
        return json.load(f)

def read_config_ini(path):
    config = configparser.ConfigParser()
    config.read(path)
    result = {}
    for section in config.sections():
        result[section] = {}
        for key, val in config[section].items():
            try:
                result[section][key] = json.loads(val)
            except:
                result[section][key] = val
    return result

def setup_logger(name=None, log_dir='logs', level=logging.INFO):
    if name is None:
        # Get the name of the calling script (without extension)
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        name = os.path.splitext(os.path.basename(module.__file__))[0] if module and module.__file__ else 'default'

    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent adding duplicate handlers
    if logger.hasHandlers():
        return logger

    log_file_base = os.path.join(log_dir, f"{name}.log")

    # Rotate logs at midnight with date-based suffix
    rotating_handler = TimedRotatingFileHandler(
        log_file_base,
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8',
        utc=False
    )
    rotating_handler.suffix = "%Y-%m-%d"

    formatter = logging.Formatter(
        '%(asctime)s :: %(levelname)s :: %(filename)s :: %(funcName)s :: %(lineno)d :: %(message)s'
    )
    rotating_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(rotating_handler)
    logger.addHandler(stream_handler)

    return logger

def get_valid_refresh_token(json_path, refresh_api_url):
    try:
        # Step 1: Check if file exists and is non-empty
        if not os.path.exists(json_path) or os.stat(json_path).st_size == 0:
            return _create_new_refresh_token(json_path, refresh_api_url)

        # Step 2: Try reading the token
        with open(json_path, 'r') as f:
            data = json.load(f)

        last_time = datetime.strptime(data.get('time', ""), "%Y-%m-%d %H:%M:%S")
        current_time = datetime.now()
        diff_minutes = (current_time - last_time).total_seconds() / 60

        # Step 3: Check if token is older than 15 mins
        if diff_minutes > 10:
            return _create_new_refresh_token(json_path, refresh_api_url)

        return data['json_web_token']

    except (json.JSONDecodeError, ValueError, KeyError):
        # If file is malformed, regenerate
        return _create_new_refresh_token(json_path, refresh_api_url)
    except Exception as e:
        raise RuntimeError(f"Error handling refresh token: {e}")

def _create_new_refresh_token(json_path, refresh_api_url):
    try:
        config = read_config_ini('cfg/config.ini')
        payload = json.dumps({
        "access_key": config["bhom"]["access_key"],
        "access_secret_key": config["bhom"]["access_secret_key"],
        })
        headers = {
        'Content-Type': 'application/json'
        }

        response = requests.request("POST", refresh_api_url, headers=headers, data=payload)
        response.raise_for_status()
        new_token = response.json().get("json_web_token")

        if not new_token:
            raise Exception("No 'refresh_token' found in API response")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = {
            "time": current_time,
            "refresh_token": new_token
        }

        # Ensure directory exists before writing
        os.makedirs(os.path.dirname(json_path), exist_ok=True)

        with open(json_path, 'w') as f:
            json.dump(data, f, indent=4)

        return new_token

    except Exception as e:
        raise RuntimeError(f"Error refreshing and writing token: {e}")
