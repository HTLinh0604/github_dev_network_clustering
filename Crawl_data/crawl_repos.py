"""
Crawl top repositories by stars and forks - Better checkpoint recovery
"""
import logging
import time
from typing import List, Dict, Set, Optional
from tqdm import tqdm
from config import *
from utils import CheckpointManager, ProcessedDataTracker, CSVWriter
from graphql_client import GitHubGraphQLClient, GitHubRESTClient

logger = logging.getLogger(__name__)

class RepositoryCrawler:
    """Crawl GitHub repositories with better checkpoint recovery"""
    
    def __init__(self, client: GitHubGraphQLClient):
        self.client = client
        self.rest_client = GitHubRESTClient(client.api_keys)
        self.checkpoint = CheckpointManager(CHECKPOINT_FILE)
        self.processed_repos = ProcessedDataTracker(PROCESSED_REPOS_FILE)
        
    def search_repos_query_simple(self) -> str:
        """Simplified GraphQL query to reduce load"""
        return """
        query SearchRepos($query: String!, $first: Int!, $after: String) {
            search(query: $query, type: REPOSITORY, first: $first, after: $after) {
                repositoryCount
                pageInfo {
                    hasNextPage
                    endCursor
                }
                edges {
                    node {
                        ... on Repository {
                            id
                            nameWithOwner
                            name
                            owner {
                                id
                                login
                                __typename
                            }
                            description
                            primaryLanguage {
                                name
                            }
                            stargazerCount
                            forkCount
                            isPrivate
                            createdAt
                            updatedAt
                            pushedAt
                        }
                    }
                }
            }
        }
        """
    
    def get_repo_details_query(self) -> str:
        """Query for detailed repo information without collaborators field"""
        return """
        query GetRepoDetails($owner: String!, $name: String!) {
            repository(owner: $owner, name: $name) {
                id
                isFork
                parent {
                    nameWithOwner
                }
                repositoryTopics(first: 10) {
                    nodes {
                        topic {
                            name
                        }
                    }
                }
                watchers {
                    totalCount
                }
                issues(states: OPEN) {
                    totalCount
                }
                pullRequests {
                    totalCount
                }
                defaultBranchRef {
                    target {
                        ... on Commit {
                            history(first: 1) {
                                totalCount
                            }
                        }
                    }
                }
            }
        }
        """
    
    def reconstruct_repos_from_contributors(self) -> List[Dict]:
        """Reconstruct repository list from contributors data"""
        logger.info("Attempting to reconstruct repos from contributors data...")
        
        contributors = self.checkpoint.get('contributors', [])
        if not contributors:
            return []
        
        # Extract unique repos from contributors
        repos_map = {}
        for contributor in contributors:
            repo_id = contributor.get('repo_id')
            if repo_id and repo_id not in repos_map:
                # We have limited info, but it's enough to continue
                repos_map[repo_id] = {
                    'id': repo_id,
                    'nameWithOwner': 'unknown/unknown',  # Will be filled later
                    'contributors_count': 1
                }
            elif repo_id:
                repos_map[repo_id]['contributors_count'] += 1
        
        logger.info(f"Reconstructed {len(repos_map)} repos from contributors data")
        return list(repos_map.values())
    
    def get_contributors_count(self, owner: str, name: str) -> int:
        """Get contributors count using REST API"""
        try:
            result = self.rest_client.get(
                f"repos/{owner}/{name}/contributors",
                params={'per_page': MIN_CONTRIBUTORS + 1, 'anon': 'false'}
            )
            
            if result and len(result) > MIN_CONTRIBUTORS:
                return len(result)
                    
            return 0
            
        except Exception as e:
            logger.debug(f"Failed to get contributors count for {owner}/{name}: {e}")
            return 0
    
    def get_top_repos(self, sort_by: str, limit: int = 100) -> List[Dict]:
        """Get top repositories with checkpoint support"""
        
        # Try multiple checkpoint keys
        checkpoint_keys = [
            f'{sort_by}_repos_with_details',
            f'{sort_by}_repos_detailed', 
            f'{sort_by}_repos'
        ]
        
        for key in checkpoint_keys:
            cached_repos = self.checkpoint.get(key)
            if cached_repos and isinstance(cached_repos, list) and len(cached_repos) > 0:
                logger.info(f"✓ Loaded {len(cached_repos)} {sort_by} repos from checkpoint (key: {key})")
                return cached_repos
        
        logger.info(f"Fetching top {limit} repos sorted by {sort_by}")
        
        query = f"topic:{SEARCH_TOPIC} sort:{sort_by}-desc is:public"
        
        # Start with smaller batch size
        batch_size = 20
        variables = {
            'query': query,
            'first': min(batch_size, limit),
            'after': None
        }
        
        repos = []
        remaining = limit
        
        while remaining > 0:
            try:
                result = self.client.execute_query(
                    self.search_repos_query_simple(),
                    variables
                )
                
                if not result or 'data' not in result:
                    logger.warning(f"Failed to fetch repos. Retrying with smaller batch...")
                    batch_size = max(5, batch_size // 2)
                    variables['first'] = min(batch_size, remaining)
                    time.sleep(5)
                    continue
                
                search_data = result['data']['search']
                
                for edge in search_data['edges']:
                    repo = edge['node']
                    
                    # Skip private repos
                    if repo.get('isPrivate', True):
                        continue
                    
                    repos.append(repo)
                    remaining -= 1
                    
                    if remaining <= 0:
                        break
                
                # Check for next page
                if not search_data['pageInfo']['hasNextPage'] or remaining <= 0:
                    break
                
                variables['after'] = search_data['pageInfo']['endCursor']
                variables['first'] = min(batch_size, remaining)
                
                # Small delay between requests
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error fetching repos: {e}")
                time.sleep(5)
        
        # Save basic repos list
        self.checkpoint.set(f'{sort_by}_repos', repos)
        
        # Now get detailed information for each repo
        logger.info(f"Fetching detailed information for {len(repos)} repos")
        detailed_repos = []
        
        for repo in tqdm(repos, desc=f"Getting {sort_by} repo details"):
            owner, name = repo['nameWithOwner'].split('/')
            
            try:
                # Get basic details from GraphQL
                details_result = self.client.execute_query(
                    self.get_repo_details_query(),
                    {'owner': owner, 'name': name}
                )
                
                if details_result and 'data' in details_result and details_result['data']['repository']:
                    details = details_result['data']['repository']
                    # Merge data
                    repo.update(details)
                
                # Check contributors count using REST API
                contributors_count = self.get_contributors_count(owner, name)
                
                if contributors_count < MIN_CONTRIBUTORS:
                    logger.debug(f"Skipping {repo['nameWithOwner']} - only {contributors_count} contributors")
                    continue
                
                repo['contributors_count'] = contributors_count
                detailed_repos.append(repo)
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Failed to get details for {repo['nameWithOwner']}: {e}")
                # Still include the repo with basic info
                repo['contributors_count'] = 0
                detailed_repos.append(repo)
        
        # Save detailed repos to checkpoint
        self.checkpoint.set(f'{sort_by}_repos_with_details', detailed_repos)
        
        logger.info(f"Found {len(detailed_repos)} repos sorted by {sort_by} with >= {MIN_CONTRIBUTORS} contributors")
        return detailed_repos
    
    def get_union_repos(self) -> List[Dict]:
        """Get union of top starred and forked repos with better recovery"""
        logger.info("Getting union of top starred and forked repositories")
        
        # Log checkpoint status
        logger.info(f"Checking checkpoint file: {CHECKPOINT_FILE}")
        checkpoint_data = self.checkpoint.checkpoint_data
        logger.info(f"Checkpoint keys available: {list(checkpoint_data.keys())[:10]}...")  # Show first 10 keys
        
        # Try to load from various checkpoint keys
        checkpoint_attempts = [
            ('union_repos', 'union'),
            ('starred_repos_with_details', 'starred with details'),
            ('forked_repos_with_details', 'forked with details'),
            ('starred_repos', 'starred'),
            ('forked_repos', 'forked')
        ]
        
        union_repos = None
        starred_repos = None
        forked_repos = None
        
        # Try to load union_repos first
        cached_union = self.checkpoint.get('union_repos')
        if cached_union and isinstance(cached_union, list) and len(cached_union) > 0:
            logger.info(f"✓ Loaded {len(cached_union)} union repos from checkpoint")
            return cached_union
        
        # Try to reconstruct from existing data
        if 'contributors' in checkpoint_data:
            logger.info("Found contributors data, attempting to reconstruct repos...")
            
            # Get all unique repo IDs from processed repos
            repo_ids = set()
            for key in checkpoint_data.keys():
                if key.startswith('repo_processed_'):
                    repo_id = key.replace('repo_processed_', '')
                    repo_ids.add(repo_id)
            
            if repo_ids:
                logger.info(f"Found {len(repo_ids)} processed repo IDs in checkpoint")
                
                # Create minimal repo objects
                union_repos = []
                for repo_id in repo_ids:
                    union_repos.append({
                        'id': repo_id,
                        'nameWithOwner': 'recovered/repo',
                        'contributors_count': MIN_CONTRIBUTORS  # Assume it met criteria
                    })
                
                # Save and return
                self.checkpoint.set('union_repos', union_repos)
                logger.info(f"✓ Recovered {len(union_repos)} repos from checkpoint data")
                return union_repos
        
        # If we can't recover, fetch fresh data
        logger.info("No recoverable data found, fetching fresh...")
        
        # Get top starred repos
        starred_repos = self.get_top_repos('stars', TOP_REPOS_COUNT)
        
        # Save intermediate checkpoint
        self.checkpoint.set('starred_repos_with_details', starred_repos)
        logger.info(f"Saved {len(starred_repos)} starred repos to checkpoint")
        
        # Get top forked repos  
        forked_repos = self.get_top_repos('forks', TOP_REPOS_COUNT)
        
        # Save intermediate checkpoint
        self.checkpoint.set('forked_repos_with_details', forked_repos)
        logger.info(f"Saved {len(forked_repos)} forked repos to checkpoint")
        
        # Create union (remove duplicates)
        repo_dict = {}
        for repo in starred_repos + forked_repos:
            repo_id = repo['id']
            if repo_id not in repo_dict:
                repo_dict[repo_id] = repo
        
        union_repos = list(repo_dict.values())
        logger.info(f"Total unique repos: {len(union_repos)}")
        
        # Save to checkpoint
        self.checkpoint.set('union_repos', union_repos)
        logger.info(f"Saved {len(union_repos)} union repos to checkpoint")
        
        return union_repos
    
    def get_repo_contributors(self, repo: Dict) -> List[Dict]:
        """Get contributors for a repository using REST API"""
        
        # Handle recovered repos with minimal info
        if repo.get('nameWithOwner') == 'recovered/repo':
            logger.debug(f"Skipping recovered repo {repo['id']}")
            return []
        
        owner, name = repo['nameWithOwner'].split('/')
        
        # Check if we already processed this repo's contributors
        contrib_checkpoint_key = f"contributors_{repo['id']}"
        cached_contributors = self.checkpoint.get(contrib_checkpoint_key)
        
        if cached_contributors and isinstance(cached_contributors, list):
            logger.info(f"Loaded {len(cached_contributors)} contributors for {repo['nameWithOwner']} from checkpoint")
            return cached_contributors
        
        logger.info(f"Getting contributors for {repo['nameWithOwner']}")
        
        contributors = []
        page = 1
        per_page = 100
        
        while True:
            try:
                result = self.rest_client.get(
                    f"repos/{owner}/{name}/contributors",
                    params={'page': page, 'per_page': per_page, 'anon': 'false'}
                )
                
                if not result:
                    break
                
                for contributor in result:
                    if contributor.get('contributions', 0) > MIN_COMMITS:
                        # Get user details from GraphQL for ID
                        user_result = self.client.execute_query(
                            """
                            query GetUser($login: String!) {
                                user(login: $login) {
                                    id
                                    login
                                    name
                                }
                            }
                            """,
                            {'login': contributor['login']}
                        )
                        
                        if user_result and 'data' in user_result and user_result['data']['user']:
                            user_data = user_result['data']['user']
                            contributors.append({
                                'user_id': user_data['id'],
                                'login': user_data['login'],
                                'name': user_data.get('name', ''),
                                'repo_id': repo['id'],
                                'commit_count': contributor['contributions']
                            })
                        else:
                            # Fallback if GraphQL fails
                            contributors.append({
                                'user_id': str(contributor.get('id', '')),
                                'login': contributor['login'],
                                'name': contributor.get('name', ''),
                                'repo_id': repo['id'],
                                'commit_count': contributor['contributions']
                            })
                
                if len(result) < per_page:
                    break
                    
                page += 1
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error getting contributors for {repo['nameWithOwner']}: {e}")
                break
        
        # Save to checkpoint
        if contributors:
            self.checkpoint.set(contrib_checkpoint_key, contributors)
        
        logger.info(f"Found {len(contributors)} contributors with > {MIN_COMMITS} commits")
        return contributors
    
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
    
    def save_repos_to_csv(self, repos: List[Dict]):
        """Save repositories to CSV with better null handling"""
        
        # Skip if repos are recovered minimal info
        valid_repos = [r for r in repos if r.get('nameWithOwner') != 'recovered/repo']
        
        if not valid_repos:
            logger.warning("No valid repos to save to CSV")
            return
        
        headers = ['repo_id', 'name', 'owner_id', 'owner_type', 'description', 
                  'language', 'stars', 'forks', 'topics', 'watchers', 
                  'issues_count', 'pr_count', 'commits_count', 
                  'created_at', 'updated_at', 'pushed_at', 'owner']
        
        csv_writer = CSVWriter(REPOS_CSV, headers)
        
        for repo in tqdm(valid_repos, desc="Saving repos to CSV"):
            try:
                # Skip if already saved
                if self.processed_repos.is_processed(repo.get('id', '')):
                    continue
                
                # Safely extract topics
                topics_list = []
                repo_topics = repo.get('repositoryTopics', {})
                if repo_topics and 'nodes' in repo_topics:
                    for topic_node in repo_topics['nodes']:
                        if topic_node and 'topic' in topic_node:
                            topic_name = topic_node['topic'].get('name')
                            if topic_name:
                                topics_list.append(topic_name)
                topics = ','.join(topics_list)
                
                # Safely extract commits count
                commits_count = 0
                default_branch = repo.get('defaultBranchRef')
                if default_branch and 'target' in default_branch:
                    target = default_branch['target']
                    if target and 'history' in target:
                        commits_count = target['history'].get('totalCount', 0)
                
                # Safely extract language
                language = ''
                primary_lang = repo.get('primaryLanguage')
                if primary_lang and isinstance(primary_lang, dict):
                    language = primary_lang.get('name', '')
                
                # Safely extract owner info
                owner_info = repo.get('owner', {})
                owner_id = owner_info.get('id', '') if owner_info else ''
                owner_type = owner_info.get('__typename', 'User') if owner_info else 'User'
                owner_login = owner_info.get('login', '') if owner_info else ''
                
                csv_writer.write_row({
                    'repo_id': repo.get('id', ''),
                    'name': repo.get('name', ''),
                    'owner_id': owner_id,
                    'owner_type': owner_type,
                    'description': repo.get('description', '') or '',
                    'language': language,
                    'stars': repo.get('stargazerCount', 0),
                    'forks': repo.get('forkCount', 0),
                    'topics': topics,
                    'watchers': self.safe_get_nested(repo, 'watchers', 'totalCount', default=0),
                    'issues_count': self.safe_get_nested(repo, 'issues', 'totalCount', default=0),
                    'pr_count': self.safe_get_nested(repo, 'pullRequests', 'totalCount', default=0),
                    'commits_count': commits_count,
                    'created_at': repo.get('createdAt', ''),
                    'updated_at': repo.get('updatedAt', ''),
                    'pushed_at': repo.get('pushedAt', ''),
                    'owner': owner_login
                })
                
                self.processed_repos.mark_processed(repo.get('id', ''))
                
            except Exception as e:
                logger.error(f"Error saving repo {repo.get('nameWithOwner', 'unknown')}: {e}")
                continue