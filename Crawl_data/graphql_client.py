"""
GraphQL client with rate limiting and key rotation - Fixed FORBIDDEN errors
"""
import requests
import time
import random
from typing import Dict, Any, Optional, List
import logging
from config import *
from utils import rate_limit_wait

logger = logging.getLogger(__name__)

class GitHubGraphQLClient:
    """GitHub GraphQL API client with key rotation and better error handling"""
    
    def __init__(self, api_keys: List[str]):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.rate_limits = {}
        self.session = requests.Session()
        # Add connection pooling and timeout settings
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        self.session.mount('https://', adapter)
        
    @property
    def current_key(self) -> str:
        """Get current API key"""
        return self.api_keys[self.current_key_index]
    
    @property
    def headers(self) -> Dict:
        """Get request headers with current key"""
        return {
            'Authorization': f'Bearer {self.current_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.github.v4+json'
        }
    
    def check_rate_limit(self) -> Dict:
        """Check rate limit for current key"""
        query = """
        query {
            rateLimit {
                limit
                cost
                remaining
                resetAt
            }
        }
        """
        
        try:
            response = self.session.post(
                GITHUB_GRAPHQL_URL,
                json={'query': query},
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'rateLimit' in data['data']:
                    rate_limit = data['data']['rateLimit']
                    logger.info(f"Rate limit - Remaining: {rate_limit['remaining']}/{rate_limit['limit']}")
                    return rate_limit
        except Exception as e:
            logger.warning(f"Failed to check rate limit: {e}")
            
        return {'remaining': 0}
    
    def rotate_key(self) -> bool:
        """Rotate to next API key"""
        if len(self.api_keys) <= 1:
            logger.error("No more API keys available")
            return False
            
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        logger.info(f"Rotated to API key index: {self.current_key_index}")
        time.sleep(1)  # Brief pause after rotation
        return True
    
    def execute_query(self, query: str, variables: Dict = None, max_retries: int = 5) -> Optional[Dict]:
        """Execute GraphQL query with improved error handling"""
        
        retry_count = 0
        backoff_base = 2
        
        while retry_count < max_retries:
            try:
                # Check rate limit before query
                if retry_count == 0:  # Only check on first attempt
                    rate_limit = self.check_rate_limit()
                    
                    if rate_limit.get('remaining', 0) < RATE_LIMIT_THRESHOLD:
                        logger.warning(f"Rate limit low: {rate_limit.get('remaining', 0)}")
                        if not self.rotate_key():
                            # No more keys, wait for reset
                            wait_time = min(300, backoff_base ** retry_count)
                            rate_limit_wait(wait_time)
                            retry_count += 1
                            continue
                
                # Prepare request
                payload = {'query': query}
                if variables:
                    payload['variables'] = variables
                
                # Execute request with timeout
                response = self.session.post(
                    GITHUB_GRAPHQL_URL,
                    json=payload,
                    headers=self.headers,
                    timeout=30
                )
                
                # Handle successful response
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'errors' in data:
                        error_msg = str(data['errors'])
                        
                        # Check for specific error types
                        has_forbidden = any('FORBIDDEN' in str(err.get('type', '')) for err in data['errors'])
                        has_permission_error = any('permission' in str(err.get('message', '')).lower() for err in data['errors'])
                        
                        if has_forbidden or has_permission_error:
                            # Log but still return the partial data if available
                            logger.debug(f"GraphQL permission errors (expected): {data['errors']}")
                            if 'data' in data:
                                return data  # Return partial data
                            else:
                                return None  # No data available
                        
                        # Handle other errors
                        logger.error(f"GraphQL errors: {error_msg}")
                        
                        if 'rate limit' in error_msg.lower():
                            self.rotate_key()
                            retry_count += 1
                            continue
                        elif 'timeout' in error_msg.lower():
                            retry_count += 1
                            time.sleep(backoff_base ** retry_count)
                            continue
                    
                    return data
                
                # Handle 4xx errors
                elif response.status_code == 401:
                    logger.error(f"Authentication failed for key index {self.current_key_index}")
                    if self.rotate_key():
                        retry_count += 1
                        continue
                    else:
                        return None
                    
                elif response.status_code == 403:
                    logger.error("Rate limit exceeded")
                    if self.rotate_key():
                        retry_count += 1
                        continue
                    else:
                        wait_time = min(300, backoff_base ** retry_count)
                        rate_limit_wait(wait_time)
                        retry_count += 1
                        continue
                
                # Handle 5xx errors (server errors)
                elif response.status_code >= 500:
                    wait_time = min(60, backoff_base ** retry_count + random.randint(0, 10))
                    logger.warning(f"Server error {response.status_code}. Retrying in {wait_time}s... (Attempt {retry_count + 1}/{max_retries})")
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                    
                else:
                    logger.error(f"Request failed: {response.status_code}")
                    retry_count += 1
                    time.sleep(backoff_base ** retry_count)
                    
            except requests.exceptions.Timeout:
                wait_time = min(30, backoff_base ** retry_count)
                logger.warning(f"Request timeout. Retrying in {wait_time}s... (Attempt {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                retry_count += 1
                
            except requests.exceptions.ConnectionError as e:
                wait_time = min(30, backoff_base ** retry_count)
                logger.warning(f"Connection error: {e}. Retrying in {wait_time}s... (Attempt {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                retry_count += 1
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                retry_count += 1
                time.sleep(backoff_base ** retry_count)
        
        logger.error(f"Failed after {max_retries} retries")
        return None
    
    def paginate_query(self, query_template: str, variables: Dict, 
                      page_info_path: List[str], max_pages: int = None) -> List[Dict]:
        """Handle pagination for GraphQL queries with better error handling"""
        all_results = []
        has_next = True
        cursor = None
        page_count = 0
        consecutive_failures = 0
        max_consecutive_failures = 3
        
        while has_next and (max_pages is None or page_count < max_pages):
            # Update cursor in variables
            if cursor:
                variables['after'] = cursor
            
            result = self.execute_query(query_template, variables)
            
            if not result or 'data' not in result:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"Too many consecutive failures. Stopping pagination.")
                    break
                    
                # Wait and retry
                wait_time = 2 ** consecutive_failures
                logger.warning(f"Pagination failed. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            
            # Reset failure counter on success
            consecutive_failures = 0
            all_results.append(result)
            
            # Navigate to pageInfo
            page_info_data = result['data']
            for key in page_info_path:
                if key in page_info_data:
                    page_info_data = page_info_data[key]
                else:
                    has_next = False
                    break
            
            if has_next and 'pageInfo' in page_info_data:
                has_next = page_info_data['pageInfo'].get('hasNextPage', False)
                cursor = page_info_data['pageInfo'].get('endCursor')
            else:
                has_next = False
            
            page_count += 1
            logger.info(f"Fetched page {page_count}")
            
            # Small delay between pages to avoid overwhelming the API
            time.sleep(0.5)
        
        return all_results

class GitHubRESTClient:
    """GitHub REST API client for operations not available in GraphQL"""
    
    def __init__(self, api_keys: List[str]):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.session = requests.Session()
        # Add connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        self.session.mount('https://', adapter)
    
    @property
    def current_key(self) -> str:
        return self.api_keys[self.current_key_index]
    
    @property
    def headers(self) -> Dict:
        return {
            'Authorization': f'token {self.current_key}',
            'Accept': 'application/vnd.github.v3+json'
        }
    
    def rotate_key(self) -> bool:
        """Rotate to next API key"""
        if len(self.api_keys) <= 1:
            return False
            
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        logger.info(f"REST client rotated to key index: {self.current_key_index}")
        return True
    
    def get(self, endpoint: str, params: Dict = None, max_retries: int = 3) -> Optional[Any]:
        """Make GET request to REST API with retry logic"""
        url = f"{GITHUB_REST_URL}/{endpoint}"
        
        for retry in range(max_retries):
            try:
                response = self.session.get(
                    url, 
                    headers=self.headers, 
                    params=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401:
                    logger.error(f"REST API authentication failed")
                    self.rotate_key()
                elif response.status_code == 403:
                    # Check if it's rate limit or permission issue
                    if 'rate limit' in response.text.lower():
                        logger.warning("REST API rate limit exceeded")
                        self.rotate_key()
                    else:
                        logger.debug(f"REST API permission denied for {endpoint}")
                        return None  # Permission denied, don't retry
                elif response.status_code == 404:
                    logger.debug(f"REST API resource not found: {endpoint}")
                    return None  # Resource not found, don't retry
                elif response.status_code >= 500:
                    wait_time = 2 ** retry
                    logger.warning(f"REST API server error. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"REST API error: {response.status_code} - {response.text}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"REST API timeout. Retry {retry + 1}/{max_retries}")
                time.sleep(2 ** retry)
            except Exception as e:
                logger.error(f"REST API exception: {e}")
                time.sleep(2 ** retry)
        
        return None