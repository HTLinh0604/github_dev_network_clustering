"""
Configuration file for GitHub GraphQL crawler
"""
import os
from typing import List

# GitHub API Keys - Add your keys here
GITHUB_API_KEYS = [
    "KEY_1",
    "KEY_2",
    "KEY_3"
    
]

# API Configuration
GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
GITHUB_REST_URL = "https://api.github.com"
RATE_LIMIT_THRESHOLD = 100  # Switch key when remaining < 100

# Data directories
DATA_DIR = "data"
CHECKPOINT_DIR = "checkpoints"
CSV_DIR = "csv_output"

# Create directories if not exist
for dir_path in [DATA_DIR, CHECKPOINT_DIR, CSV_DIR]:
    os.makedirs(dir_path, exist_ok=True)

# File paths
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, "checkpoint.json")
PROCESSED_USERS_FILE = os.path.join(CHECKPOINT_DIR, "processed_users.json")
PROCESSED_REPOS_FILE = os.path.join(CHECKPOINT_DIR, "processed_repos.json")

# CSV file paths
USERS_CSV = os.path.join(CSV_DIR, "users.csv")
REPOS_CSV = os.path.join(CSV_DIR, "repos.csv")
COLLABORATION_CSV = os.path.join(CSV_DIR, "collaboration_edges.csv")
SOCIAL_GRAPH_CSV = os.path.join(CSV_DIR, "social_graph_edges.csv")
ACTIVITY_CSV = os.path.join(CSV_DIR, "activity.csv")
USER_REPO_CONTRIB_CSV = os.path.join(CSV_DIR, "user_repo_contributions.csv")
STAR_EVENTS_CSV = os.path.join(CSV_DIR, "star_events.csv")

# Crawl parameters
MIN_CONTRIBUTORS = 5
MIN_COMMITS = 1
TOP_REPOS_COUNT = 100
SEARCH_TOPIC = "tensorflow"