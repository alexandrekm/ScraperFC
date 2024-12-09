from botasaurus.request import request, Request
from botasaurus_requests.response import Response
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MAX_RETRIES = 3
RETRY_DELAY = 1.13  # seconds between retries

# Optional proxy configuration
PROXY_ENDPOINT = os.getenv('PROXY_ENDPOINT')
PROXY_USERNAME = os.getenv('PROXY_USERNAME')
PROXY_PASSWORD = os.getenv('PROXY_PASSWORD')

@request(output=None, create_error_logs=False)
def botasaurus_get(req: Request, url: str, use_proxy: bool = False) -> Response:
    """ General purpose "get" function that uses Botasaurus.
    
    Parameters
    ----------
    req : botasaurus.request.Request
        The request object provided by the botasaurus decorator
    url : str
        The URL to request
    use_proxy : bool, optional
        If True, uses proxy for the request if configured. Defaults to False.
        
    Returns
    -------
    botasaurus_requests.response.Response
        The response from the request
    """
    time.sleep(2)
    if not isinstance(url, str):
        raise TypeError('`url` must be a string.')
    
    print(f"Requesting: {url}")
    
    # If proxy is not requested or endpoint not configured, make direct request
    if not use_proxy or not PROXY_ENDPOINT:
        return req.get(url)
    
    # Build proxy URL based on whether authentication is needed
    if PROXY_USERNAME and PROXY_PASSWORD:
        proxy_url = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_ENDPOINT}"
    else:
        proxy_url = f"http://{PROXY_ENDPOINT}"
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Attempt {attempt + 1}/{MAX_RETRIES}")
            resp = req.get(url, proxies={'http': proxy_url, 'https': proxy_url}, verify=False)
            if resp.status_code >= 200 and resp.status_code < 300:
                return resp
            print(f"Request failed with status code: {resp.status_code}")
        except Exception as e:
            print(f"Request failed with error: {str(e)}")
        
        if attempt < MAX_RETRIES - 1:  # Don't sleep after the last attempt
            time.sleep(RETRY_DELAY)
    
    # If we get here, all retries failed
    raise Exception(f"Failed to get {url} after {MAX_RETRIES} attempts")