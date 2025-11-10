"""
Crawl detailed user information - Fixed NoneType errors
"""
import logging
from typing import List, Dict, Optional
from tqdm import tqdm
from config import *
from utils import CheckpointManager, ProcessedDataTracker, CSVWriter, parse_datetime
from graphql_client import GitHubGraphQLClient, GitHubRESTClient

logger = logging.getLogger(__name__)

class UserCrawler:
    """Crawl detailed user information - NO LIMITS"""
    
    def __init__(self, graphql_client: GitHubGraphQLClient, rest_client: GitHubRESTClient):
        self.graphql_client = graphql_client
        self.rest_client = rest_client
        self.checkpoint = CheckpointManager(CHECKPOINT_FILE)
        self.processed_users = ProcessedDataTracker(PROCESSED_USERS_FILE)
        
    def get_user_details_query(self) -> str:
        """Query to get detailed user information"""
        return """
        query GetUserDetails($login: String!) {
            user(login: $login) {
                id
                login
                name
                bio
                email
                company
                location
                websiteUrl
                twitterUsername
                avatarUrl
                url
                createdAt
                updatedAt
                repositories(first: 100, privacy: PUBLIC) {
                    totalCount
                    nodes {
                        id
                        name
                        stargazerCount
                        forkCount
                        primaryLanguage {
                            name
                        }
                    }
                }
                gists(first: 100, privacy: PUBLIC) {
                    totalCount
                }
                followers(first: 1) {
                    totalCount
                }
                following(first: 1) {
                    totalCount
                }
                organizations(first: 100) {
                    nodes {
                        id
                        login
                        name
                    }
                }
                contributionsCollection {
                    totalCommitContributions
                    totalPullRequestContributions
                    totalIssueContributions
                    totalPullRequestReviewContributions
                    contributionCalendar {
                        totalContributions
                    }
                }
                starredRepositories(first: 1) {
                    totalCount
                }
            }
        }
        """
    
    def get_user_followers_query(self) -> str:
        """Query to get user followers"""
        return """
        query GetFollowers($login: String!, $first: Int!, $after: String) {
            user(login: $login) {
                id
                followers(first: $first, after: $after) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    nodes {
                        id
                        login
                    }
                }
            }
        }
        """
    
    def get_user_following_query(self) -> str:
        """Query to get user following"""
        return """
        query GetFollowing($login: String!, $first: Int!, $after: String) {
            user(login: $login) {
                id
                following(first: $first, after: $after) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    nodes {
                        id
                        login
                    }
                }
            }
        }
        """
    
    def get_user_starred_repos_query(self) -> str:
        """Query to get user starred repositories"""
        return """
        query GetStarredRepos($login: String!, $first: Int!, $after: String) {
            user(login: $login) {
                starredRepositories(first: $first, after: $after) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    edges {
                        starredAt
                        node {
                            id
                            nameWithOwner
                        }
                    }
                }
            }
        }
        """
    
    def safe_get(self, data: Optional[Dict], key: str, default=None):
        """Safely get value from dictionary"""
        if data is None:
            return default
        return data.get(key, default)
    
    def safe_get_nested(self, data: Optional[Dict], *keys, default=None):
        """Safely get nested dictionary values"""
        if data is None:
            return default
        
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
                if current is None:
                    return default
            else:
                return default
        
        return current if current is not None else default
    
    def get_user_details(self, login: str) -> Optional[Dict]:
        """Get detailed information for a user"""
        
        # Check if already processed
        if self.processed_users.is_processed(login):
            logger.info(f"User {login} already processed, skipping")
            return None
        
        logger.info(f"Fetching details for user: {login}")
        
        try:
            result = self.graphql_client.execute_query(
                self.get_user_details_query(),
                {'login': login}
            )
            
            if not result or 'data' not in result:
                logger.error(f"No data returned for user {login}")
                return None
            
            user_data = result.get('data', {}).get('user')
            
            if not user_data:
                logger.error(f"User {login} not found or no access")
                return None
            
            # Get paginated data with error handling
            try:
                user_data['all_followers'] = self.get_user_followers(login)
            except Exception as e:
                logger.warning(f"Failed to get followers for {login}: {e}")
                user_data['all_followers'] = []
            
            try:
                user_data['all_following'] = self.get_user_following(login)
            except Exception as e:
                logger.warning(f"Failed to get following for {login}: {e}")
                user_data['all_following'] = []
            
            try:
                user_data['starred_repos'] = self.get_user_starred_repos(login)
            except Exception as e:
                logger.warning(f"Failed to get starred repos for {login}: {e}")
                user_data['starred_repos'] = []
            
            return user_data
            
        except Exception as e:
            logger.error(f"Error getting details for user {login}: {e}")
            return None
    
    def get_user_followers(self, login: str) -> List[Dict]:
        """Get ALL followers for a user"""
        logger.info(f"Getting ALL followers for {login}")
        
        all_followers = []
        variables = {'login': login, 'first': 100, 'after': None}
        
        try:
            # NO LIMIT - get all pages
            pages = self.graphql_client.paginate_query(
                self.get_user_followers_query(),
                variables,
                ['user', 'followers'],
                max_pages=None  # NO LIMIT
            )
            
            for page in pages:
                if page and 'data' in page and 'user' in page['data']:
                    user_data = page['data']['user']
                    if user_data and 'followers' in user_data:
                        followers = user_data['followers'].get('nodes', [])
                        if followers:
                            all_followers.extend(followers)
            
            logger.info(f"Found {len(all_followers)} followers")
            
        except Exception as e:
            logger.error(f"Error getting followers for {login}: {e}")
        
        return all_followers
    
    def get_user_following(self, login: str) -> List[Dict]:
        """Get ALL following for a user"""
        logger.info(f"Getting ALL following for {login}")
        
        all_following = []
        variables = {'login': login, 'first': 100, 'after': None}
        
        try:
            # NO LIMIT - get all pages
            pages = self.graphql_client.paginate_query(
                self.get_user_following_query(),
                variables,
                ['user', 'following'],
                max_pages=None  # NO LIMIT
            )
            
            for page in pages:
                if page and 'data' in page and 'user' in page['data']:
                    user_data = page['data']['user']
                    if user_data and 'following' in user_data:
                        following = user_data['following'].get('nodes', [])
                        if following:
                            all_following.extend(following)
            
            logger.info(f"Found {len(all_following)} following")
            
        except Exception as e:
            logger.error(f"Error getting following for {login}: {e}")
        
        return all_following
    
    def get_user_starred_repos(self, login: str) -> List[Dict]:
        """Get ALL starred repositories for a user"""
        logger.info(f"Getting ALL starred repos for {login}")
        
        all_starred = []
        variables = {'login': login, 'first': 100, 'after': None}
        
        try:
            # NO LIMIT - get all pages
            pages = self.graphql_client.paginate_query(
                self.get_user_starred_repos_query(),
                variables,
                ['user', 'starredRepositories'],
                max_pages=None  # NO LIMIT
            )
            
            for page in pages:
                if page and 'data' in page and 'user' in page['data']:
                    user_data = page['data']['user']
                    if user_data and 'starredRepositories' in user_data:
                        starred = user_data['starredRepositories'].get('edges', [])
                        if starred:
                            all_starred.extend(starred)
            
            logger.info(f"Found {len(all_starred)} starred repos")
            
        except Exception as e:
            logger.error(f"Error getting starred repos for {login}: {e}")
        
        return all_starred
    
    def save_user_data(self, user_data: Dict):
        """Save user data to various CSV files"""
        if not user_data:
            return
        
        try:
            # Save to users.csv
            self.save_user_info(user_data)
            
            # Save social graph edges
            self.save_social_graph(user_data)
            
            # Save activity data
            self.save_activity_data(user_data)
            
            # Save star events
            self.save_star_events(user_data)
            
            # Mark as processed
            login = user_data.get('login')
            if login:
                self.processed_users.mark_processed(login)
                
        except Exception as e:
            logger.error(f"Error saving user data: {e}")
    
    def save_user_info(self, user_data: Dict):
        """Save basic user information to CSV"""
        if not user_data:
            return
            
        headers = ['user_id', 'login', 'name', 'bio', 'company', 'location',
                  'created_at', 'updated_at', 'public_repos', 'followers_count',
                  'following_count', 'organizations']
        
        csv_writer = CSVWriter(USERS_CSV, headers)
        
        try:
            # Safely extract organizations
            orgs_list = []
            orgs_data = self.safe_get(user_data, 'organizations', {})
            if orgs_data and 'nodes' in orgs_data:
                for org in orgs_data['nodes']:
                    if org and 'login' in org:
                        orgs_list.append(org['login'])
            orgs = ','.join(orgs_list)
            
            csv_writer.write_row({
                'user_id': self.safe_get(user_data, 'id', ''),
                'login': self.safe_get(user_data, 'login', ''),
                'name': self.safe_get(user_data, 'name', ''),
                'bio': self.safe_get(user_data, 'bio', ''),
                'company': self.safe_get(user_data, 'company', ''),
                'location': self.safe_get(user_data, 'location', ''),
                'created_at': parse_datetime(self.safe_get(user_data, 'createdAt', '')),
                'updated_at': parse_datetime(self.safe_get(user_data, 'updatedAt', '')),
                'public_repos': self.safe_get_nested(user_data, 'repositories', 'totalCount', default=0),
                'followers_count': self.safe_get_nested(user_data, 'followers', 'totalCount', default=0),
                'following_count': self.safe_get_nested(user_data, 'following', 'totalCount', default=0),
                'organizations': orgs
            })
        except Exception as e:
            logger.error(f"Error saving user info: {e}")
    
    def save_social_graph(self, user_data: Dict):
        """Save social graph edges to CSV"""
        if not user_data:
            return
            
        headers = ['source_user_id', 'target_user_id']
        csv_writer = CSVWriter(SOCIAL_GRAPH_CSV, headers)
        
        try:
            user_id = self.safe_get(user_data, 'id')
            if not user_id:
                return
            
            # Save followers edges
            followers = self.safe_get(user_data, 'all_followers', [])
            for follower in followers:
                if follower and 'id' in follower:
                    csv_writer.write_row({
                        'source_user_id': follower['id'],
                        'target_user_id': user_id
                    })
            
            # Save following edges
            following = self.safe_get(user_data, 'all_following', [])
            for follow in following:
                if follow and 'id' in follow:
                    csv_writer.write_row({
                        'source_user_id': user_id,
                        'target_user_id': follow['id']
                    })
        except Exception as e:
            logger.error(f"Error saving social graph: {e}")
    
    def save_activity_data(self, user_data: Dict):
        """Save user activity data to CSV"""
        if not user_data:
            return
            
        headers = ['user_id', 'total_commits', 'repo_count', 'avg_commits_per_repo',
                  'active_years', 'language_distribution', 'stars_received']
        
        csv_writer = CSVWriter(ACTIVITY_CSV, headers)
        
        try:
            # Calculate language distribution
            languages = {}
            total_stars = 0
            repos_data = self.safe_get(user_data, 'repositories', {})
            repos = repos_data.get('nodes', []) if repos_data else []
            
            for repo in repos:
                if repo:
                    lang_data = self.safe_get(repo, 'primaryLanguage')
                    if lang_data:
                        lang = lang_data.get('name')
                        if lang:
                            languages[lang] = languages.get(lang, 0) + 1
                    total_stars += self.safe_get(repo, 'stargazerCount', 0)
            
            contributions = self.safe_get(user_data, 'contributionsCollection', {})
            total_commits = self.safe_get(contributions, 'totalCommitContributions', 0)
            repo_count = len(repos)
            
            # Calculate active years
            created_at = self.safe_get(user_data, 'createdAt', '')
            if created_at:
                try:
                    from datetime import datetime
                    created_year = datetime.fromisoformat(created_at.replace('Z', '+00:00')).year
                    current_year = datetime.now().year
                    active_years = current_year - created_year + 1
                except:
                    active_years = 0
            else:
                active_years = 0
            
            csv_writer.write_row({
                'user_id': self.safe_get(user_data, 'id', ''),
                'total_commits': total_commits,
                'repo_count': repo_count,
                'avg_commits_per_repo': total_commits / repo_count if repo_count > 0 else 0,
                'active_years': active_years,
                'language_distribution': str(languages),
                'stars_received': total_stars
            })
        except Exception as e:
            logger.error(f"Error saving activity data: {e}")
    
    def save_star_events(self, user_data: Dict):
        """Save star events to CSV"""
        if not user_data:
            return
            
        headers = ['user_id', 'repo_id', 'starred_at']
        csv_writer = CSVWriter(STAR_EVENTS_CSV, headers)
        
        try:
            user_id = self.safe_get(user_data, 'id')
            if not user_id:
                return
            
            starred_repos = self.safe_get(user_data, 'starred_repos', [])
            for star in starred_repos:
                if star and 'node' in star:
                    node = star['node']
                    if node and 'id' in node:
                        csv_writer.write_row({
                            'user_id': user_id,
                            'repo_id': node['id'],
                            'starred_at': parse_datetime(star.get('starredAt', ''))
                        })
        except Exception as e:
            logger.error(f"Error saving star events: {e}")