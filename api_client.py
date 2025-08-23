# api_client.py
import requests
import base64
import time
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class ETAApiClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api.invoicing.eta.gov.eg"
        self.auth_url = "https://id.eta.gov.eg/connect/token"
        self.access_token = None
        self.token_expiry_time = 0
        self.last_api_call_time = 0
        self.min_request_interval = 0.6 

    def _enforce_rate_limit(self):
        now = time.monotonic()
        elapsed = now - self.last_api_call_time
        if elapsed < self.min_request_interval:
            wait_time = self.min_request_interval - elapsed
            print(f"Rate limit: waiting for {wait_time:.2f} seconds...")
            time.sleep(wait_time)
        self.last_api_call_time = time.monotonic()

    def test_authentication(self):
        # ... (This method can remain the same as the previous version)
        self.access_token = None 
        auth_header = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        headers = {'Authorization': f'Basic {auth_header}', 'Content-Type': 'application/x-www-form-urlencoded'}
        payload = {'grant_type': 'client_credentials'}
        try:
            self._enforce_rate_limit()
            response = requests.post(self.auth_url, headers=headers, data=payload, timeout=10)
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data['access_token']
            self.token_expiry_time = time.time() + (token_data.get('expires_in', 3600) - 300)
            return (True, "Authentication Successful!")
        except requests.exceptions.RequestException as e:
            return (False, f"Connection Error: {e}")

    def _get_access_token(self):
        if self.access_token and time.time() < self.token_expiry_time:
            return self.access_token
        print("Token expired or missing. Re-authenticating...")
        success, message = self.test_authentication()
        return self.access_token if success else None

    def _make_request(self, method, url, **kwargs):
        """A centralized and resilient request handler with retries."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._enforce_rate_limit()
                response = requests.request(method, url, **kwargs)
                if response.status_code == 429: # Specifically handle rate limit error
                    wait_time = 5 * (attempt + 1) # Wait longer each time
                    print(f"Rate limit hit (429). Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue # Go to next attempt
                
                response.raise_for_status() # Raise HTTPError for other bad responses (4xx or 5xx)
                return response.json()

            except requests.exceptions.ReadTimeout as e:
                print(f"Read timeout. Retrying... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(3 * (attempt + 1)) # Wait and try again
            except requests.exceptions.RequestException as e:
                print(f"A network error occurred: {e}. Retrying... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(3 * (attempt + 1))
        
        print(f"Request failed after {max_retries} attempts.")
        return None # Return None if all retries fail

    def search_documents(self, start_date, end_date, page_size=100, continuation_token=None, direction=None):
        """Searches for documents, now with support for a 'direction' filter."""
        token = self._get_access_token()
        if not token: return None
        
        headers = {'Authorization': f'Bearer {token}'}
        params = {
            'submissionDateFrom': start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'submissionDateTo': end_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'pageSize': page_size
        }
        if continuation_token:
            params['continuationToken'] = continuation_token
        
        # --- NEW: Add the direction parameter if provided ---
        if direction:
            params['direction'] = direction
        
        url = f"{self.base_url}/api/v1.0/documents/search"
        return self._make_request('GET', url, headers=headers, params=params, timeout=20)
        
    def get_document_details(self, uuid):
        """
        Retrieves the full details for a single document. This version trusts the API response
        and passes it on directly, correctly handling different document structures like 'Cancelled'.
        """
        token = self._get_access_token()
        if not token:
            return None
        
        headers = {'Authorization': f'Bearer {token}'}
        url = f"{self.base_url}/api/v1.0/documents/{uuid}/details"
        
        # The resilient _make_request handler already ensures we get a valid JSON or None.
        # We no longer need the flawed check for the 'document' key.
        response_json = self._make_request('GET', url, headers=headers, timeout=20)

        # If we got any valid JSON response, we return it.
        return response_json

    # The discovery functions remain the same as the last version
    def find_newest_invoice_date(self):
        # ... (Keep the last working version of this function)
        print("Searching for the newest invoice...")
        now = datetime.utcnow()
        for i in range(3):
            end_period = now - timedelta(days=i * 30)
            start_period = end_period - timedelta(days=30)
            print(f"Probing for newest: {start_period.date()} to {end_period.date()}")
            data = self.search_documents(start_period, end_period, page_size=1)
            if data and data.get('result'):
                date_str = data['result'][0]['dateTimeReceived']
                print(f"Found newest invoice date: {date_str}")
                if '.' in date_str and len(date_str.split('.')[1]) > 7:
                    date_str = date_str[:26] + "Z"
                for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                    try:
                        return datetime.strptime(date_str, fmt)
                    except ValueError:
                        continue
                print(f"Warning: Could not parse date format for newest invoice: {date_str}")
                return None
        print("Could not find any recent invoices in the last 90 days.")
        return None

    def find_oldest_invoice_date(self):
        # ... (Keep the last working version of this function)
        print("Searching for the oldest invoice (this may take a moment)...")
        probe_end_date = datetime.utcnow()
        last_found_date_str = None
        for i in range(60):
            probe_start_date = probe_end_date - timedelta(days=30)
            print(f"Probing for oldest: {probe_start_date.date()} to {probe_end_date.date()}...")
            data = self.search_documents(probe_start_date, probe_end_date, page_size=1)
            if data and data.get('result'):
                last_found_date_str = data['result'][0]['dateTimeReceived']
            else:
                if last_found_date_str:
                    print(f"Found oldest invoice date: {last_found_date_str}")
                    if '.' in last_found_date_str and len(last_found_date_str.split('.')[1]) > 7:
                         last_found_date_str = last_found_date_str[:26] + "Z"
                    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                        try:
                            return datetime.strptime(last_found_date_str, fmt)
                        except ValueError:
                            continue
                    print(f"Warning: Could not parse date format for oldest invoice: {last_found_date_str}")
                return None
            probe_end_date = probe_start_date
        print("Could not find any invoices after probing back 5 years.")
        return None