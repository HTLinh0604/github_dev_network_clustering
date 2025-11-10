"""
Reset step 4 - FIXED matrix size issue
"""
import json
import os
import logging
import sys
import numpy as np
import pandas as pd
from typing import List, Dict, Set, Tuple
from collections import defaultdict
from tqdm import tqdm
from scipy.sparse import csr_matrix, save_npz
from config import *
from utils import CheckpointManager
from graphql_client import GitHubGraphQLClient, GitHubRESTClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('recrawl_step4_fixed.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# CONFIGURATION
MAX_CONTRIBUTORS_PER_REPO = 500
MAX_COMMITS_PAGES = 100
MATRIX_SAMPLE_SIZE = 100  # Only create sample matrix for top N users

class FixedCollaborationCrawler:
    """Crawl collaboration with CORRECT weight calculation"""
    
    def __init__(self, client: GitHubGraphQLClient):
        self.client = client
        self.rest_client = GitHubRESTClient(client.api_keys)
        # Track user collaborations globally
        self.user_repo_map = defaultdict(set)  # user_id -> set of repo_ids
        self.repo_contributors = defaultdict(set)  # repo_id -> set of user_ids
        self.user_commits_per_repo = defaultdict(lambda: defaultdict(int))  # user_id -> repo_id -> commit_count
        
    def get_repo_commits_query(self) -> str:
        """Query to get commit history"""
        return """
        query GetRepoCommits($owner: String!, $name: String!, $first: Int!, $after: String) {
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
    
    def get_top_contributors_rest(self, owner: str, name: str, limit: int = 500) -> List[Dict]:
        """Get top contributors by commit count"""
        contributors = []
        page = 1
        per_page = 100
        
        logger.info(f"Getting top {limit} contributors for {owner}/{name}")
        
        while len(contributors) < limit:
            try:
                result = self.rest_client.get(
                    f"repos/{owner}/{name}/contributors",
                    params={'page': page, 'per_page': per_page, 'anon': 'false'}
                )
                
                if not result:
                    break
                
                for contrib in result:
                    # Get user node_id
                    user_info = self.rest_client.get(f"users/{contrib['login']}")
                    if user_info:
                        contributors.append({
                            'login': contrib['login'],
                            'contributions': contrib.get('contributions', 0),
                            'id': user_info.get('node_id', '')
                        })
                    
                    if len(contributors) >= limit:
                        break
                
                if len(result) < per_page:
                    break
                    
                page += 1
                
            except Exception as e:
                logger.error(f"Error getting contributors: {e}")
                break
        
        contributors.sort(key=lambda x: x['contributions'], reverse=True)
        return contributors[:limit]
    
    def process_repository(self, repo_full_name: str, repo_id: str) -> Dict[str, int]:
        """Process a single repository and collect contributor data"""
        owner, name = repo_full_name.split('/')
        
        logger.info(f"Processing repository: {repo_full_name}")
        
        # Get top contributors
        top_contributors = self.get_top_contributors_rest(owner, name, MAX_CONTRIBUTORS_PER_REPO)
        top_contributor_logins = {c['login']: c['id'] for c in top_contributors}
        
        # Track contributors for this repo
        repo_contributors_ids = set()
        commits_by_user = defaultdict(int)
        
        # Get commit data
        variables = {
            'owner': owner,
            'name': name,
            'first': 100,
            'after': None
        }
        
        page_count = 0
        while page_count < MAX_COMMITS_PAGES:
            try:
                result = self.client.execute_query(
                    self.get_repo_commits_query(),
                    variables
                )
                
                if not result or 'data' not in result:
                    break
                
                repo_data = result.get('data', {}).get('repository')
                if not repo_data:
                    break
                
                history = repo_data.get('defaultBranchRef', {}).get('target', {}).get('history', {})
                commits = history.get('nodes', [])
                
                for commit in commits:
                    if commit and commit.get('author', {}).get('user'):
                        user = commit['author']['user']
                        user_id = user.get('id')
                        user_login = user.get('login')
                        
                        # Only count commits from top contributors
                        if user_login and user_login in top_contributor_logins:
                            if user_id:
                                repo_contributors_ids.add(user_id)
                                commits_by_user[user_id] += 1
                                # Update global tracking
                                self.user_repo_map[user_id].add(repo_id)
                                self.user_commits_per_repo[user_id][repo_id] += 1
                
                page_info = history.get('pageInfo', {})
                if not page_info.get('hasNextPage'):
                    break
                    
                variables['after'] = page_info.get('endCursor')
                page_count += 1
                
            except Exception as e:
                logger.error(f"Error processing commits: {e}")
                break
        
        # Store repo contributors
        self.repo_contributors[repo_id] = repo_contributors_ids
        
        logger.info(f"Found {len(repo_contributors_ids)} contributors with {sum(commits_by_user.values())} commits")
        return commits_by_user
    
    def calculate_global_collaborations(self) -> List[Dict]:
        """Calculate collaboration edges based on COMMON REPOS"""
        logger.info("Calculating global collaboration edges...")
        
        collaborations = {}
        all_users = list(self.user_repo_map.keys())
        
        total_pairs = len(all_users) * (len(all_users) - 1) // 2
        logger.info(f"Processing {total_pairs} user pairs...")
        
        with tqdm(total=total_pairs, desc="Creating collaboration edges") as pbar:
            for i in range(len(all_users)):
                for j in range(i + 1, len(all_users)):
                    user_a = all_users[i]
                    user_b = all_users[j]
                    
                    # Find common repos
                    repos_a = self.user_repo_map[user_a]
                    repos_b = self.user_repo_map[user_b]
                    common_repos = repos_a.intersection(repos_b)
                    
                    if common_repos:
                        # Create ONE edge with weight = number of common repos
                        key = tuple(sorted([user_a, user_b]))
                        
                        # Calculate total commits for each user in common repos
                        commits_a = sum(self.user_commits_per_repo[user_a][repo] for repo in common_repos)
                        commits_b = sum(self.user_commits_per_repo[user_b][repo] for repo in common_repos)
                        
                        collaborations[key] = {
                            'user_A': key[0],
                            'user_B': key[1],
                            'repo_id': ','.join(list(common_repos)[:10]),  # Limit to first 10 repos to avoid too long string
                            'common_repos_count': len(common_repos),  # This is the WEIGHT
                            'commit_count_A': commits_a,
                            'commit_count_B': commits_b,
                            'weight': len(common_repos)  # Weight = number of common repos
                        }
                    
                    pbar.update(1)
        
        logger.info(f"Created {len(collaborations)} unique collaboration edges")
        return list(collaborations.values())
    
    def create_adjacency_matrix_sample(self) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Create SAMPLE adjacency matrix for top users only"""
        logger.info("Creating adjacency matrix sample...")
        
        # Get all users sorted by number of repos (most active first)
        all_users = sorted(
            self.user_repo_map.keys(), 
            key=lambda u: len(self.user_repo_map[u]), 
            reverse=True
        )
        
        # Take only top N users for matrix
        sample_users = all_users[:MATRIX_SAMPLE_SIZE]
        n_sample = len(sample_users)
        
        logger.info(f"Creating {n_sample}x{n_sample} sample matrix from top {n_sample} users")
        
        # Create user index mapping for sample
        user_to_idx = {user: idx for idx, user in enumerate(sample_users)}
        
        # Initialize sample matrix
        adj_matrix = np.zeros((n_sample, n_sample), dtype=int)
        
        # Fill matrix with common repo counts
        for i, user_a in enumerate(sample_users):
            for j, user_b in enumerate(sample_users):
                if i < j:  # Only upper triangle
                    repos_a = self.user_repo_map[user_a]
                    repos_b = self.user_repo_map[user_b]
                    common_repos = len(repos_a.intersection(repos_b))
                    adj_matrix[i, j] = common_repos
                    adj_matrix[j, i] = common_repos  # Symmetric
        
        # Create DataFrame for visualization
        df_matrix = pd.DataFrame(
            adj_matrix,
            index=sample_users,
            columns=sample_users
        )
        
        logger.info(f"Created {n_sample}x{n_sample} sample adjacency matrix")
        
        # Also save full user mapping
        full_user_mapping = {user: idx for idx, user in enumerate(all_users)}
        
        return df_matrix, full_user_mapping
    
    def create_sparse_adjacency_matrix(self) -> Tuple[csr_matrix, Dict[str, int]]:
        """Create SPARSE adjacency matrix for all users (memory efficient)"""
        logger.info("Creating sparse adjacency matrix for all users...")
        
        all_users = sorted(list(self.user_repo_map.keys()))
        n_users = len(all_users)
        
        logger.info(f"Creating sparse matrix for {n_users} users")
        
        # Create user index mapping
        user_to_idx = {user: idx for idx, user in enumerate(all_users)}
        
        # Build sparse matrix using COO format (then convert to CSR)
        rows = []
        cols = []
        data = []
        
        for i, user_a in enumerate(tqdm(all_users, desc="Building sparse matrix")):
            repos_a = self.user_repo_map[user_a]
            for j in range(i + 1, n_users):
                user_b = all_users[j]
                repos_b = self.user_repo_map[user_b]
                common_repos = len(repos_a.intersection(repos_b))
                
                if common_repos > 0:
                    rows.extend([i, j])
                    cols.extend([j, i])
                    data.extend([common_repos, common_repos])
        
        # Create sparse matrix
        sparse_matrix = csr_matrix(
            (data, (rows, cols)), 
            shape=(n_users, n_users),
            dtype=np.int32
        )
        
        logger.info(f"Created sparse matrix: {sparse_matrix.shape}, {sparse_matrix.nnz} non-zero entries")
        
        return sparse_matrix, user_to_idx
    
    def create_adjacency_list(self) -> Dict[str, List[Tuple[str, int]]]:
        """Create adjacency list representation"""
        logger.info("Creating adjacency list...")
        
        adj_list = defaultdict(list)
        
        for user_a in tqdm(self.user_repo_map.keys(), desc="Building adjacency list"):
            repos_a = self.user_repo_map[user_a]
            
            neighbors = []
            for user_b in self.user_repo_map.keys():
                if user_a != user_b:
                    repos_b = self.user_repo_map[user_b]
                    common_repos = repos_a.intersection(repos_b)
                    
                    if common_repos:
                        neighbors.append((user_b, len(common_repos)))
            
            # Sort neighbors by weight (number of common repos)
            neighbors.sort(key=lambda x: x[1], reverse=True)
            adj_list[user_a] = neighbors[:100]  # Keep top 100 neighbors for each user
        
        logger.info(f"Created adjacency list for {len(adj_list)} users")
        return dict(adj_list)

class Step4FixedRecrawler:
    """Reset and recrawl step 4 with FIXED logic"""
    
    def __init__(self):
        self.checkpoint = CheckpointManager(CHECKPOINT_FILE)
        self.graphql_client = GitHubGraphQLClient(GITHUB_API_KEYS)
        self.rest_client = GitHubRESTClient(GITHUB_API_KEYS)
        self.collab_crawler = FixedCollaborationCrawler(self.graphql_client)
        
    def reset_step4(self):
        """Reset step 4 in checkpoint"""
        logger.info("=" * 60)
        logger.info("RESETTING STEP 4")
        logger.info("=" * 60)
        
        checkpoint_data = self.checkpoint.checkpoint_data
        
        # Remove step 4 from completed steps
        steps_completed = checkpoint_data.get('steps_completed', [])
        if 'step4_collaborations' in steps_completed:
            steps_completed.remove('step4_collaborations')
            self.checkpoint.set('steps_completed', steps_completed)
            logger.info("✓ Removed step4_collaborations")
        
        # Remove collaboration flags
        keys_to_remove = [k for k in checkpoint_data.keys() if k.startswith('collab_processed_')]
        for key in keys_to_remove:
            del checkpoint_data[key]
        
        logger.info(f"✓ Removed {len(keys_to_remove)} collaboration flags")
        self.checkpoint.save_checkpoint(checkpoint_data)
        
        # Backup CSV files
        self.backup_csv_files()
    
    def backup_csv_files(self):
        """Backup existing CSV files"""
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        files_to_backup = [
            COLLABORATION_CSV,
            USER_REPO_CONTRIB_CSV
        ]
        
        for file_path in files_to_backup:
            if os.path.exists(file_path):
                backup_path = f"{file_path}.backup_{timestamp}"
                os.rename(file_path, backup_path)
                logger.info(f"✓ Backed up to {backup_path}")
    
    def recrawl_with_fixed_logic(self):
        """Recrawl with FIXED collaboration logic"""
        logger.info("\n" + "=" * 60)
        logger.info("RE-CRAWLING WITH FIXED LOGIC")
        logger.info("=" * 60)
        
        repos = self.checkpoint.get('union_repos', [])
        
        # Filter valid repos
        valid_repos = [r for r in repos if r.get('nameWithOwner') and 
                      '/' in r['nameWithOwner'] and
                      r['nameWithOwner'] != 'recovered/repo']
        
        logger.info(f"Processing {len(valid_repos)} repositories")
        
        # Step 1: Process all repositories first
        for repo in tqdm(valid_repos, desc="Collecting repository data"):
            repo_name = repo.get('nameWithOwner')
            repo_id = repo.get('id')
            
            if not repo_id:
                continue
            
            try:
                # Process repository and collect contributor data
                self.collab_crawler.process_repository(repo_name, repo_id)
                
            except Exception as e:
                logger.error(f"Error processing {repo_name}: {e}")
                continue
        
        # Step 2: Calculate global collaborations (ONE edge per user pair)
        logger.info("\nCalculating global collaborations...")
        collaborations = self.collab_crawler.calculate_global_collaborations()
        
        # Step 3: Create adjacency data
        # Sample matrix for visualization
        adj_matrix_sample, user_mapping = self.collab_crawler.create_adjacency_matrix_sample()
        
        # Sparse matrix for full data
        sparse_matrix, sparse_mapping = self.collab_crawler.create_sparse_adjacency_matrix()
        
        # Adjacency list
        adj_list = self.collab_crawler.create_adjacency_list()
        
        # Step 4: Save collaboration edges
        self.save_collaborations(collaborations)
        
        # Step 5: Save user-repo contributions
        self.save_contributions()
        
        # Step 6: Save adjacency data
        self.save_adjacency_data(adj_matrix_sample, sparse_matrix, adj_list, user_mapping, sparse_mapping)
        
        # Mark as completed
        steps_completed = self.checkpoint.get('steps_completed', [])
        if 'step4_collaborations' not in steps_completed:
            steps_completed.append('step4_collaborations')
            self.checkpoint.set('steps_completed', steps_completed)
        
        logger.info("\n✓ Fixed collaboration crawl completed!")
    
    def save_collaborations(self, collaborations: List[Dict]):
        """Save collaboration edges to CSV"""
        from utils import CSVWriter
        
        headers = ['user_A', 'user_B', 'common_repos', 'common_repos_count',
                  'commit_count_A', 'commit_count_B', 'weight']
        
        csv_writer = CSVWriter(COLLABORATION_CSV, headers)
        
        for collab in tqdm(collaborations, desc="Saving collaborations"):
            csv_writer.write_row({
                'user_A': collab['user_A'],
                'user_B': collab['user_B'],
                'common_repos': collab['repo_id'],  # List of common repos (limited)
                'common_repos_count': collab['common_repos_count'],
                'commit_count_A': collab['commit_count_A'],
                'commit_count_B': collab['commit_count_B'],
                'weight': collab['weight']  # Number of common repos
            })
        
        logger.info(f"✓ Saved {len(collaborations)} collaboration edges")
    
    def save_contributions(self):
        """Save user-repo contributions"""
        from utils import CSVWriter
        
        headers = ['user_id', 'repo_id', 'commits_count', 'PR_count',
                  'issues_count', 'reviews_count']
        
        csv_writer = CSVWriter(USER_REPO_CONTRIB_CSV, headers)
        
        count = 0
        for user_id, repos in tqdm(self.collab_crawler.user_commits_per_repo.items(), 
                                   desc="Saving contributions"):
            for repo_id, commit_count in repos.items():
                csv_writer.write_row({
                    'user_id': user_id,
                    'repo_id': repo_id,
                    'commits_count': commit_count,
                    'PR_count': 0,
                    'issues_count': 0,
                    'reviews_count': 0
                })
                count += 1
        
        logger.info(f"✓ Saved {count} user-repo contributions")
    
    def save_adjacency_data(self, adj_matrix_sample: pd.DataFrame, sparse_matrix: csr_matrix,
                           adj_list: Dict, user_mapping: Dict, sparse_mapping: Dict):
        """Save all adjacency data"""
        output_dir = os.path.join(CSV_DIR, "adjacency_data")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save sample adjacency matrix for visualization
        matrix_file = os.path.join(output_dir, f"adjacency_matrix_top{MATRIX_SAMPLE_SIZE}.csv")
        adj_matrix_sample.to_csv(matrix_file)
        logger.info(f"✓ Saved adjacency matrix sample to {matrix_file}")
        
        # Save sparse matrix
        sparse_file = os.path.join(output_dir, "adjacency_matrix_sparse.npz")
        save_npz(sparse_file, sparse_matrix)
        logger.info(f"✓ Saved sparse matrix to {sparse_file}")
        
        # Save adjacency list (limited to avoid huge file)
        adj_list_file = os.path.join(output_dir, "adjacency_list.json")
        with open(adj_list_file, 'w') as f:
            # Convert to serializable format
            serializable_list = {
                user: neighbors  # Already limited to top 100 neighbors
                for user, neighbors in adj_list.items()
            }
            json.dump(serializable_list, f, indent=2)
        logger.info(f"✓ Saved adjacency list to {adj_list_file}")
        
        # Save user mappings
        mapping_file = os.path.join(output_dir, "user_mapping_full.json")
        with open(mapping_file, 'w') as f:
            json.dump(user_mapping, f, indent=2)
        logger.info(f"✓ Saved user mapping to {mapping_file}")
        
        sparse_mapping_file = os.path.join(output_dir, "user_mapping_sparse.json")
        with open(sparse_mapping_file, 'w') as f:
            json.dump(sparse_mapping, f, indent=2)
        
        # Save summary statistics
        self.save_statistics(adj_list, sparse_matrix, output_dir)
    
    def save_statistics(self, adj_list: Dict, sparse_matrix: csr_matrix, output_dir: str):
        """Save collaboration statistics"""
        stats_file = os.path.join(output_dir, "collaboration_stats.txt")
        
        with open(stats_file, 'w') as f:
            f.write("COLLABORATION STATISTICS\n")
            f.write("=" * 50 + "\n\n")
            
            # Basic stats
            f.write(f"Total users: {len(adj_list)}\n")
            f.write(f"Total edges (sparse matrix): {sparse_matrix.nnz // 2}\n")
            f.write(f"Matrix density: {sparse_matrix.nnz / (sparse_matrix.shape[0] * sparse_matrix.shape[1]):.4%}\n\n")
            
            # Degree distribution
            degrees = [len(neighbors) for neighbors in adj_list.values()]
            if degrees:
                f.write("Degree distribution:\n")
                f.write(f"  - Min degree: {min(degrees)}\n")
                f.write(f"  - Max degree: {max(degrees)}\n")
                f.write(f"  - Avg degree: {sum(degrees)/len(degrees):.2f}\n")
                f.write(f"  - Median degree: {sorted(degrees)[len(degrees)//2]}\n\n")
            
            # Top connected users
            connected_users = [(user, len(neighbors)) for user, neighbors in adj_list.items()]
            connected_users.sort(key=lambda x: x[1], reverse=True)
            
            f.write("Top 20 most connected users:\n")
            for user, count in connected_users[:20]:
                f.write(f"  - {user[:30]}: {count} connections\n")
            
            # Weight distribution
            weights = []
            for neighbors in adj_list.values():
                for _, weight in neighbors:
                    weights.append(weight)
            
            if weights:
                f.write(f"\nCommon repos distribution:\n")
                f.write(f"  - Min common repos: {min(weights)}\n")
                f.write(f"  - Max common repos: {max(weights)}\n")
                f.write(f"  - Avg common repos: {sum(weights)/len(weights):.2f}\n")
                f.write(f"  - Users with 10+ common repos: {sum(1 for w in weights if w >= 10)}\n")
        
        logger.info(f"✓ Saved statistics to {stats_file}")

def main():
    """Main function"""
    print("=" * 60)
    print("STEP 4 WITH FIXED COLLABORATION LOGIC")
    print("=" * 60)
    
    print("\nFixed Issues:")
    print("  ✓ Matrix size handling for large datasets")
    print("  ✓ Sample matrix for top 100 users")
    print("  ✓ Sparse matrix for all users")
    print("  ✓ Limited adjacency list (top 100 neighbors)")
    
    print("\nExpected output:")
    print("  1. collaboration_edges.csv")
    print("  2. user_repo_contributions.csv")
    print(f"  3. adjacency_matrix_top{MATRIX_SAMPLE_SIZE}.csv")
    print("  4. adjacency_matrix_sparse.npz")
    print("  5. adjacency_list.json")
    print("  6. collaboration_stats.txt")
    
    response = input("\nProceed? (yes/no): ")
    
    if response.lower() == 'yes':
        recrawler = Step4FixedRecrawler()
        
        # Don't reset if we already have the data
        if input("Reset step 4? (yes/no): ").lower() == 'yes':
            recrawler.reset_step4()
        
        # Continue from where we left off
        print("\nContinuing with fixed logic...")
        recrawler.recrawl_with_fixed_logic()
        
        print("\n✓ Completed!")
    else:
        print("Cancelled")

if __name__ == "__main__":
    main()