from botasaurus.request import request, Request
from botasaurus_requests.response import Response
import time
import os
import random
from dotenv import load_dotenv
from datetime import datetime, timedelta
import atexit
from .logger_config import setup_logging

# Load environment variables
load_dotenv()

# Setup logging
logger = setup_logging(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 1.13  # seconds between retries
UNPROXIED_COOLDOWN = 1  # seconds between unproxied requests

# Time windows for unproxied requests (in hours)
# Hours before forcing proxy
UNPROXIED_MAX_WINDOW = float(os.getenv("UNPROXIED_MAX_WINDOW", "2"))
# Hours before allowing unproxied again
UNPROXIED_LOCKOUT_WINDOW = float(os.getenv("UNPROXIED_LOCKOUT_WINDOW", "1"))

# Tracking variables
last_unproxied_request = 0
unproxied_start_time = None
unproxied_lockout_until = None

# Request counters
request_stats = {"proxied": 0, "unproxied": 0, "start_time": None}


def print_request_stats():
    if request_stats["start_time"]:
        duration = datetime.now() - request_stats["start_time"]
        hours = duration.total_seconds() / 3600
        print("\n=== Request Statistics ===")
        print(f"Duration: {hours:.2f} hours")
        print(f"Proxied Requests: {request_stats['proxied']}")
        print(f"Unproxied Requests: {request_stats['unproxied']}")
        total = request_stats["proxied"] + request_stats["unproxied"]
        if total > 0:
            proxy_percentage = (request_stats["proxied"] / total) * 100
            print(f"Actual Proxy Usage: {proxy_percentage:.1f}%")
        print("=====================")


# Register the stats printing function to run at exit
atexit.register(print_request_stats)

# Optional proxy configuration
PROXY_ENDPOINT = os.getenv("PROXY_ENDPOINT")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")
PROXY_PERCENTAGE = float(
    os.getenv("PROXY_PERCENTAGE", "100")
)  # Default to 100% if not set


@request(output=None, create_error_logs=False)
def botasaurus_get(req: Request, url: str) -> Response:
    """General purpose "get" function that uses Botasaurus.

    Parameters
    ----------
    req : botasaurus.request.Request
        The request object provided by the botasaurus decorator
    url : str
        The URL to request

    Returns
    -------
    botasaurus_requests.response.Response
        The response from the request
    """
    if not isinstance(url, str):
        raise TypeError("`url` must be a string.")

    # Initialize start time if not set
    if request_stats["start_time"] is None:
        request_stats["start_time"] = datetime.now()

    headers = {
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
        "Connection": "keep-alive",
    }

    global last_unproxied_request, unproxied_start_time, unproxied_lockout_until
    current_time = datetime.now()

    # Check if we're in a lockout period
    if unproxied_lockout_until and current_time < unproxied_lockout_until:
        use_proxy = True
    else:
        # If lockout period is over, reset tracking
        if unproxied_lockout_until and current_time >= unproxied_lockout_until:
            unproxied_lockout_until = None
            unproxied_start_time = None

        # Determine whether to use proxy based on PROXY_PERCENTAGE
        use_proxy = PROXY_ENDPOINT and (random.random() * 100 < PROXY_PERCENTAGE)

        # If not using proxy, check time windows
        if not use_proxy:
            if not unproxied_start_time:
                unproxied_start_time = current_time

            # Check if we've exceeded the maximum unproxied window
            time_since_start = (
                current_time - unproxied_start_time
            ).total_seconds() / 3600
            if time_since_start >= UNPROXIED_MAX_WINDOW:
                # Set lockout period
                unproxied_lockout_until = current_time + timedelta(
                    hours=UNPROXIED_LOCKOUT_WINDOW
                )
                use_proxy = True
                logger.info(
                    f"Maximum unproxied window ({UNPROXIED_MAX_WINDOW}h) reached. Enforcing proxy usage for {UNPROXIED_LOCKOUT_WINDOW}h"
                )

    if not use_proxy:
        current_time_secs = time.time()
        time_since_last = current_time_secs - last_unproxied_request

        if time_since_last < UNPROXIED_COOLDOWN:
            time.sleep(UNPROXIED_COOLDOWN - time_since_last)

        logger.debug(f"Requesting (Unproxied): {url}")
        last_unproxied_request = time.time()
        request_stats["unproxied"] += 1
        for attempt in range(MAX_RETRIES):
            try:
                resp = req.get(url, headers=headers)
                if resp.status_code == 404:
                    logger.info(f"Resource not found (404) for URL: {url}")
                    return resp
                if resp.status_code >= 200 and resp.status_code < 300:
                    return resp
                logger.info(f"Request failed with status code: {resp.status_code}")
            except Exception as e:
                logger.info(f"Request failed with error: {str(e)}")
                if "botasaurus_requests.exceptions.ClientException" in str(e):
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        raise Exception(f"Failed to get {url} after {MAX_RETRIES} attempts")

    # Build proxy URL based on whether authentication is needed
    if PROXY_USERNAME and PROXY_PASSWORD:
        proxy = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_ENDPOINT}"
    else:
        proxy = f"http://{PROXY_ENDPOINT}"

    logger.info(f"Requesting (Proxied): {url}")
    request_stats["proxied"] += 1
    for attempt in range(MAX_RETRIES):
        try:
            resp = req.get(
                url,
                proxies={"http": proxy, "https": proxy},
                headers=headers,
                verify=False,
            )
            if resp.status_code == 404:
                logger.info(f"Resource not found (404) for URL: {url}")
                return resp
            if resp.status_code >= 200 and resp.status_code < 300:
                return resp
            logger.info(f"Request failed with status code: {resp.status_code}")
        except Exception as e:
            logger.info(f"Request failed with error: {str(e)}")

        if attempt < MAX_RETRIES - 1:  # Don't sleep after the last attempt
            time.sleep(RETRY_DELAY)

    # If we get here, all retries failed
    raise Exception(f"Failed to get {url} after {MAX_RETRIES} attempts")
