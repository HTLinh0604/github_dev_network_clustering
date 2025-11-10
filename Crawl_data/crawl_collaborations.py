"""
Crawl collaboration data between users - FULL DATA VERSION
"""
import logging
from typing import List, Dict, Tuple
from collections import defaultdict
from tqdm import tqdm
from config import *
from utils import CSVWriter
from graphql_client import GitHubGraphQLClient, GitHubRESTClient

logger = logging.getLogger(__name__)

class CollaborationCrawler:
    """Crawl collaboration data between users - NO LIMITS"""
    
    def __init__(self, client: GitHubGraphQLClient):
        self.client = client
        self.rest_client = GitHubRESTClient(client.api_keys)
        
    def get_repo_contributors_with_commits_query(self) -> str:
        """Query to get contributors with commit details"""
        return """
        query GetRepoContributors($owner: String!, $name: String!, $first: Int!, $after: String) {
            repository(owner: $owner, name: $name) {
                id
                defaultBranchRef {
                    target {
                        ... on Commit {
                            history(first: $first, after: $after) {
                                pageInfo {
                                    hasNextPage
                                    endCursor
                                }
                                totalCount
                                nodes {
                                    author {
                                        user {
                                            id
                                            login
                                        }
                                    }
                                    committedDate
                                }
                            }
                        }
                    }
                }
            }
        }
        """
    
    def get_user_repo_contributions_query(self) -> str:
        """Query to get user contributions to a repository - Fixed version"""
        return """
        query GetUserRepoContributions($owner: String!, $name: String!, $login: String!) {
            repository(owner: $owner, name: $name) {
                id
                name
                issues(first: 1, filterBy: {createdBy: $login}) {
                    totalCount
                }
                pullRequests(first: 1, states: [OPEN, CLOSED, MERGED]) {
                    totalCount
                }
            }
            user(login: $login) {
                id
                login
                contributionsCollection(organizationID: null) {
                    totalCommitContributions
                    totalPullRequestContributions
                    totalIssueContributions
                    totalPullRequestReviewContributions
                }
            }
        }
        """
    
    def get_repo_collaborations(self, repo_full_name: str) -> Dict[Tuple[str, str], Dict]:
        """Get ALL collaboration data for a repository"""
        owner, name = repo_full_name.split('/')
        
        logger.info(f"Getting ALL collaborations for {repo_full_name}")
        
        commits_by_user = defaultdict(int)
        variables = {
            'owner': owner,
            'name': name,
            'first': 100,
            'after': None
        }
        
        # NO LIMIT - Get ALL commits
        page_count = 0
        total_commits = 0
        
        # First query to get total count
        first_result = self.client.execute_query(
            self.get_repo_contributors_with_commits_query(),
            variables
        )
        
        if first_result and 'data' in first_result:
            repo_data = first_result.get('data', {}).get('repository')
            if repo_data and repo_data.get('defaultBranchRef'):
                total_commits = repo_data['defaultBranchRef']['target']['history'].get('totalCount', 0)
                logger.info(f"Repository has {total_commits} total commits")
        
        # Process ALL pages
        has_next = True
        commits_processed = 0
        
        with tqdm(total=total_commits, desc=f"Processing commits for {name}") as pbar:
            while has_next:
                try:
                    result = self.client.execute_query(
                        self.get_repo_contributors_with_commits_query(),
                        variables
                    )
                    
                    if not result or 'data' not in result:
                        break
                    
                    repo_data = result.get('data', {}).get('repository')
                    if not repo_data:
                        break
                    
                    default_branch = repo_data.get('defaultBranchRef')
                    if not default_branch:
                        break
                        
                    target = default_branch.get('target')
                    if not target:
                        break
                        
                    history = target.get('history')
                    if not history:
                        break
                    
                    commits = history.get('nodes', [])
                    
                    for commit in commits:
                        if commit and commit.get('author'):
                            author = commit['author']
                            if author.get('user'):
                                user = author['user']
                                if user and user.get('id'):
                                    user_id = user['id']
                                    commits_by_user[user_id] += 1
                                    commits_processed += 1
                    
                    pbar.update(len(commits))
                    
                    # Check for next page
                    page_info = history.get('pageInfo', {})
                    has_next = page_info.get('hasNextPage', False)
                    
                    if has_next:
                        variables['after'] = page_info.get('endCursor')
                        page_count += 1
                        
                        # Log progress every 10 pages
                        if page_count % 10 == 0:
                            logger.info(f"Processed {page_count} pages, {commits_processed} commits")
                    
                except Exception as e:
                    logger.error(f"Error getting commits for {repo_full_name}: {e}")
                    break
        
        logger.info(f"Processed {commits_processed} commits from {len(commits_by_user)} users")
        
        # Calculate ALL collaboration edges (no limit)
        collaborations = {}
        users = list(commits_by_user.keys())
        
        logger.info(f"Generating collaboration edges for {len(users)} users...")
        
        # Warning for large repos
        if len(users) > 500:
            edge_count = len(users) * (len(users) - 1) // 2
            logger.warning(f"This will generate {edge_count:,} collaboration edges!")
        
        # Generate ALL collaboration edges
        with tqdm(total=len(users)*(len(users)-1)//2, desc="Generating edges") as pbar:
            for i in range(len(users)):
                for j in range(i + 1, len(users)):
                    user_a = users[i]
                    user_b = users[j]
                    
                    # Create collaboration edge
                    key = tuple(sorted([user_a, user_b]))
                    
                    # Get repository ID safely
                    repo_id = repo_data.get('id', '') if 'repo_data' in locals() else ''
                    
                    collaborations[key] = {
                        'user_A': key[0],
                        'user_B': key[1],
                        'repo_id': repo_id,
                        'common_commits_count': min(commits_by_user[user_a], commits_by_user[user_b]),
                        'commit_count_A': commits_by_user[user_a],
                        'commit_count_B': commits_by_user[user_b],
                        'weight': 1  # Can be adjusted based on collaboration strength
                    }
                    pbar.update(1)
        
        logger.info(f"Generated {len(collaborations)} collaboration edges")
        return collaborations
    
    def get_user_repo_contributions_rest(self, login: str, repo_full_name: str) -> Dict:
        """Get user contributions using REST API as fallback"""
        owner, name = repo_full_name.split('/')
        
        logger.info(f"Getting contributions for {login} in {repo_full_name} (REST)")
        
        try:
            # Get user info
            user_result = self.rest_client.get(f"users/{login}")
            if not user_result:
                return {}
            
            user_id = user_result.get('node_id', '')
            
            # Get exact commit count
            commits_count = 0
            page = 1
            while True:
                commits_result = self.rest_client.get(
                    f"repos/{owner}/{name}/commits",
                    params={'author': login, 'per_page': 100, 'page': page}
                )
                if not commits_result:
                    break
                commits_count += len(commits_result)
                if len(commits_result) < 100:
                    break
                page += 1
            
            # Get exact PRs count
            pr_count = 0
            page = 1
            while True:
                prs_result = self.rest_client.get(
                    f"repos/{owner}/{name}/pulls",
                    params={'creator': login, 'state': 'all', 'per_page': 100, 'page': page}
                )
                if not prs_result:
                    break
                pr_count += len(prs_result)
                if len(prs_result) < 100:
                    break
                page += 1
            
            # Get exact issues count
            issues_count = 0
            page = 1
            while True:
                issues_result = self.rest_client.get(
                    f"repos/{owner}/{name}/issues",
                    params={'creator': login, 'state': 'all', 'per_page': 100, 'page': page}
                )
                if not issues_result:
                    break
                issues_count += len(issues_result)
                if len(issues_result) < 100:
                    break
                page += 1
            
            return {
                'user_id': user_id,
                'repo_id': '',  # We'd need another call to get this
                'commits_count': commits_count,
                'PR_count': pr_count,
                'issues_count': issues_count,
                'reviews_count': 0  # Can't easily get this from REST
            }
            
        except Exception as e:
            logger.error(f"Error getting contributions via REST: {e}")
            return {}
    
    def get_user_repo_contributions(self, login: str, repo_full_name: str) -> Dict:
        """Get user contributions to a specific repository"""
        owner, name = repo_full_name.split('/')
        
        logger.info(f"Getting contributions for {login} in {repo_full_name}")
        
        try:
            # First try simplified GraphQL query
            result = self.client.execute_query(
                self.get_user_repo_contributions_query(),
                {
                    'owner': owner,
                    'name': name,
                    'login': login
                }
            )
            
            if not result or 'data' not in result:
                # Fallback to REST API
                return self.get_user_repo_contributions_rest(login, repo_full_name)
            
            data = result['data']
            repo = data.get('repository', {})
            user = data.get('user', {})
            
            if not user:
                return {}
            
            contributions = user.get('contributionsCollection', {})
            
            # Get exact PR count for this user using REST API
            pr_count = 0
            try:
                page = 1
                while True:
                    pr_result = self.rest_client.get(
                        f"repos/{owner}/{name}/pulls",
                        params={'creator': login, 'state': 'all', 'per_page': 100, 'page': page}
                    )
                    if not pr_result:
                        break
                    pr_count += len(pr_result)
                    if len(pr_result) < 100:
                        break
                    page += 1
            except:
                pr_count = contributions.get('totalPullRequestContributions', 0)
            
            return {
                'user_id': user.get('id', ''),
                'repo_id': repo.get('id', ''),
                'commits_count': contributions.get('totalCommitContributions', 0),
                'PR_count': pr_count,
                'issues_count': repo.get('issues', {}).get('totalCount', 0),
                'reviews_count': contributions.get('totalPullRequestReviewContributions', 0)
            }
            
        except Exception as e:
            logger.error(f"Error getting contributions: {e}")
            # Fallback to REST API
            return self.get_user_repo_contributions_rest(login, repo_full_name)
    
    def save_collaborations(self, collaborations: List[Dict]):
        """Save collaboration edges to CSV"""
        headers = ['user_A', 'user_B', 'repo_id', 'common_commits_count',
                  'commit_count_A', 'commit_count_B', 'weight']
        
        csv_writer = CSVWriter(COLLABORATION_CSV, headers)
        
        # Save in batches to avoid memory issues
        batch_size = 10000
        total = len(collaborations)
        
        for i in range(0, total, batch_size):
            batch = collaborations[i:i+batch_size]
            for collab in tqdm(batch, desc=f"Saving collaborations batch {i//batch_size + 1}"):
                csv_writer.write_row(collab)
            
            logger.info(f"Saved {min(i+batch_size, total)}/{total} collaborations")
    
    def save_user_repo_contributions(self, contributions: List[Dict]):
        """Save user-repo contributions to CSV"""
        headers = ['user_id', 'repo_id', 'commits_count', 'PR_count',
                  'issues_count', 'reviews_count']
        
        csv_writer = CSVWriter(USER_REPO_CONTRIB_CSV, headers)
        
        for contrib in tqdm(contributions, desc="Saving user-repo contributions"):
            if contrib:  # Only save non-empty contributions
                csv_writer.write_row(contrib)