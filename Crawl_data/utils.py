"""
Utility functions for the crawler - Enhanced version
"""
import json
import csv
import os
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CheckpointManager:
    """Manage checkpoints for resumable crawling"""
    
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file
        self.checkpoint_data = self.load_checkpoint()
    
    def load_checkpoint(self) -> Dict:
        """Load checkpoint from file"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading checkpoint: {e}")
                return {}
        return {}
    
    def save_checkpoint(self, data: Dict):
        """Save checkpoint to file"""
        self.checkpoint_data.update(data)
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.checkpoint_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Checkpoint saved: {self.checkpoint_file}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
    
    def get(self, key: str, default=None):
        """Get checkpoint value"""
        return self.checkpoint_data.get(key, default)
    
    def set(self, key: str, value):
        """Set checkpoint value"""
        self.checkpoint_data[key] = value
        self.save_checkpoint(self.checkpoint_data)

class ProcessedDataTracker:
    """Track processed items to avoid duplicates"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.processed_items = self.load_processed()
    
    def load_processed(self) -> set:
        """Load processed items from file"""
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    return set(json.load(f))
            except Exception as e:
                logger.error(f"Error loading processed items: {e}")
                return set()
        return set()
    
    def save_processed(self):
        """Save processed items to file"""
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(list(self.processed_items), f)
        except Exception as e:
            logger.error(f"Error saving processed items: {e}")
    
    def is_processed(self, item_id: str) -> bool:
        """Check if item is already processed"""
        return item_id in self.processed_items
    
    def mark_processed(self, item_id: str):
        """Mark item as processed"""
        if item_id:  # Only add non-empty IDs
            self.processed_items.add(item_id)
            self.save_processed()

class CSVWriter:
    """Handle CSV writing with headers and None value handling"""
    
    def __init__(self, file_path: str, headers: List[str]):
        self.file_path = file_path
        self.headers = headers
        self.file_exists = os.path.exists(file_path)
        
        if not self.file_exists:
            self.write_headers()
    
    def write_headers(self):
        """Write CSV headers"""
        try:
            with open(self.file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()
        except Exception as e:
            logger.error(f"Error writing CSV headers: {e}")
    
    def sanitize_value(self, value: Any) -> str:
        """Sanitize value for CSV writing"""
        if value is None:
            return ''
        elif isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        else:
            return str(value)
    
    def write_row(self, data: Dict):
        """Write a single row to CSV with None handling"""
        try:
            # Sanitize all values
            sanitized_data = {}
            for key in self.headers:
                value = data.get(key)
                sanitized_data[key] = self.sanitize_value(value)
            
            with open(self.file_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writerow(sanitized_data)
        except Exception as e:
            logger.error(f"Error writing row to CSV: {e}")
            logger.debug(f"Problem data: {data}")
    
    def write_rows(self, data_list: List[Dict]):
        """Write multiple rows to CSV"""
        for data in data_list:
            self.write_row(data)

def rate_limit_wait(seconds: int = 60):
    """Wait with progress bar"""
    logger.warning(f"Rate limit reached. Waiting {seconds} seconds...")
    for _ in tqdm(range(seconds), desc="Waiting"):
        time.sleep(1)

def parse_datetime(date_str: str) -> str:
    """Parse and format datetime string"""
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return date_str