"""
Check data completeness and suggest next steps - FIXED
"""
import os
import csv
import json
from config import *
from utils import CheckpointManager

def check_csv_completeness():
    """Check completeness of all CSV files"""
    print("=" * 70)
    print("DATA COMPLETENESS CHECK")
    print("=" * 70)
    
    # Load checkpoint for comparison
    checkpoint = CheckpointManager(CHECKPOINT_FILE)
    checkpoint_data = checkpoint.checkpoint_data
    
    # Expected counts from checkpoint
    expected_repos = len(checkpoint_data.get('union_repos', []))
    expected_contributors = len(checkpoint_data.get('contributors', []))
    repos_processed = sum(1 for k in checkpoint_data.keys() if k.startswith('repo_processed_'))
    collabs_processed = sum(1 for k in checkpoint_data.keys() if k.startswith('collab_processed_'))
    
    print(f"\nCheckpoint Summary:")
    print(f"  - Expected repos: {expected_repos}")
    print(f"  - Expected contributors: {expected_contributors}")
    print(f"  - Repos processed: {repos_processed}")
    print(f"  - Collaborations processed: {collabs_processed}")
    
    print("\n" + "-" * 70)
    print("CSV FILES STATUS:")
    print("-" * 70)
    
    csv_files = [
        (USERS_CSV, "Users", expected_contributors),
        (REPOS_CSV, "Repositories", expected_repos),
        (COLLABORATION_CSV, "Collaboration Edges", None),
        (SOCIAL_GRAPH_CSV, "Social Graph Edges", None),
        (ACTIVITY_CSV, "Activity Data", expected_contributors),
        (USER_REPO_CONTRIB_CSV, "User-Repo Contributions", None),
        (STAR_EVENTS_CSV, "Star Events", None)
    ]
    
    issues = []
    
    for file_path, name, expected in csv_files:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                row_count = sum(1 for row in reader) - 1  # Subtract header
            
            file_size = os.path.getsize(file_path)
            size_mb = file_size / (1024 * 1024)
            
            status = "✓"
            if expected and row_count < expected:
                status = "⚠"
                issues.append(f"{name}: {row_count}/{expected} rows")
            elif row_count == 0:
                status = "✗"
                issues.append(f"{name}: EMPTY")
            
            print(f"{status} {name:30} {row_count:10,} rows  {size_mb:8.2f} MB")
        else:
            print(f"✗ {name:30} NOT FOUND")
            issues.append(f"{name}: FILE NOT FOUND")
    
    # Analysis
    print("\n" + "=" * 70)
    print("ANALYSIS:")
    print("=" * 70)
    
    if issues:
        print("\n⚠️  ISSUES FOUND:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("\n✓ All data appears complete!")
    
    # Check for potential missing collaboration data
    if os.path.exists(COLLABORATION_CSV):
        with open(COLLABORATION_CSV, 'r') as f:
            collab_count = sum(1 for _ in f) - 1
        
        print(f"\n✓ Found {collab_count:,} collaboration edges")
    
    # Check specific repos
    print("\n" + "-" * 70)
    print("REPOSITORY ANALYSIS:")
    print("-" * 70)
    
    # Check if major repos were processed
    major_repos = ['tensorflow/tensorflow', 'keras-team/keras']
    for repo_name in major_repos:
        collab_key = f'collab_processed_{repo_name}'
        if collab_key in checkpoint_data:
            print(f"✓ {repo_name}: Processed")
        else:
            print(f"⚠ {repo_name}: Not processed for collaborations")
    
    return len(issues) == 0

def show_data_statistics():
    """Show detailed statistics about the data"""
    print("\n" + "=" * 70)
    print("DATA STATISTICS:")
    print("=" * 70)
    
    # Load and analyze repos.csv
    if os.path.exists(REPOS_CSV):
        with open(REPOS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            repos = list(reader)
        
        if repos:
            total_stars = sum(int(r.get('stars', 0)) for r in repos)
            total_forks = sum(int(r.get('forks', 0)) for r in repos)
            
            # Top repos by stars
            top_repos = sorted(repos, key=lambda x: int(x.get('stars', 0)), reverse=True)[:5]
            
            print(f"\nRepository Statistics:")
            print(f"  - Total repos: {len(repos)}")
            print(f"  - Total stars: {total_stars:,}")
            print(f"  - Total forks: {total_forks:,}")
            print(f"\nTop 5 repos by stars:")
            for r in top_repos:
                stars = int(r.get('stars', 0))
                print(f"  - {r.get('name', 'unknown')}: {stars:,} stars")
    
    # Load and analyze users.csv
    if os.path.exists(USERS_CSV):
        with open(USERS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            users = list(reader)
        
        if users:
            total_followers = sum(int(u.get('followers_count', 0)) for u in users)
            
            # Top users by followers
            top_users = sorted(users, key=lambda x: int(x.get('followers_count', 0)), reverse=True)[:5]
            
            print(f"\nUser Statistics:")
            print(f"  - Total users: {len(users)}")
            print(f"  - Total followers: {total_followers:,}")
            print(f"\nTop 5 users by followers:")
            for u in top_users:
                followers = int(u.get('followers_count', 0))
                print(f"  - {u.get('login', 'unknown')}: {followers:,} followers")
    
    # Analyze collaboration density
    if os.path.exists(COLLABORATION_CSV):
        with open(COLLABORATION_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            collabs = list(reader)
        
        if collabs:
            print(f"\nCollaboration Statistics:")
            print(f"  - Total collaboration edges: {len(collabs):,}")
            
            # Calculate average collaboration strength
            if collabs[:1000]:  # Sample first 1000
                avg_commits = sum(int(c.get('common_commits_count', 0)) for c in collabs[:1000]) / min(1000, len(collabs))
                print(f"  - Average common commits (sample): {avg_commits:.2f}")

def main():
    """Main function"""
    check_csv_completeness()
    show_data_statistics()

if __name__ == "__main__":
    main()