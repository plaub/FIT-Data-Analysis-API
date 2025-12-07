"""
Setup script to configure the FastAPI Backend.
Run this script after installation to set up your environment.
"""

import os
from pathlib import Path

def setup_environment():
    """Interactive setup for environment configuration."""
    print("=" * 80)
    print("FastAPI Backend Setup")
    print("=" * 80)
    print()
    
    # Check if .env already exists
    env_file = Path(".env")
    if env_file.exists():
        print("⚠️  .env file already exists!")
        response = input("Do you want to overwrite it? (y/n): ").lower()
        if response != 'y':
            print("Setup cancelled.")
            return
    
    print("Please provide the following configuration:")
    print()
    
    # Google Cloud Configuration
    print("📋 Google Cloud Configuration")
    print("-" * 40)
    
    project_id = input("BigQuery Project ID: ").strip()
    dataset = input("BigQuery Dataset (default: fitness_data): ").strip() or "fitness_data"
    # Default for Docker environment, but user can override if running locally
    credentials_path = input("Service Account Key Path (internal Docker path, default: /app/keys/service_account_key.json): ").strip() or "/app/keys/service_account_key.json"
    
    print()
    print("🔴 Redis Configuration (press Enter for defaults)")
    print("-" * 40)
    
    redis_host = input("Redis Host (default: redis): ").strip() or "redis"
    redis_port = input("Redis Port (default: 6379): ").strip() or "6379"
    
    print()
    print("⚡ Caching & Performance (press Enter for defaults)")
    print("-" * 40)
    
    ttl_sessions = input("Cache TTL for Sessions (seconds, default: 300): ").strip() or "300"
    ttl_summary = input("Cache TTL for Summary (seconds, default: 3600): ").strip() or "3600"
    ttl_details = input("Cache TTL for Details (seconds, default: 604800): ").strip() or "604800"
    rate_limit = input("Rate Limit per Minute (default: 30): ").strip() or "30"

    print()
    print("🌐 CORS Configuration")
    print("-" * 40)
    cors_origins = input("CORS Origins (comma-separated, default: http://localhost:4321,http://localhost:3000): ").strip() or "http://localhost:4321,http://localhost:3000"

    # Create .env file
    env_content = f"""# BIGQUERY CONFIG
BIGQUERY_PROJECT_ID="{project_id}"
BIGQUERY_DATASET="{dataset}"
# Pfad zum Service Account Key, gemountet ueber Docker
GOOGLE_APPLICATION_CREDENTIALS="{credentials_path}"

# REDIS CONFIG
REDIS_HOST="{redis_host}"
REDIS_PORT={redis_port}

# CACHE TIME-TO-LIVE (in Sekunden)
CACHE_TTL_SUMMARY={ttl_summary}  # 1 Stunde fuer aggregierte Daten
CACHE_TTL_SESSIONS={ttl_sessions}  # 5 Minuten fuer die Liste der letzten Sessions
CACHE_TTL_DETAILS={ttl_details}  # 1 Woche fuer Session Details

# RATE LIMITING
# RATE LIMITING
RATE_LIMIT_PER_MINUTE="{rate_limit}"  # Max. 30 Anfragen pro Minute pro IP

# CORS
CORS_ORIGINS="{cors_origins}"
"""
    
    with open(".env", "w") as f:
        f.write(env_content)
    
    print()
    print("✅ Configuration saved to .env")
    print()
    print("=" * 80)
    print("Setup Complete!")
    print("=" * 80)
    print()
    print("Next steps:")
    print("1. Ensure your Service Account Key is at the configured path (e.g., ./keys/service_account_key.json locally mapping to /app/keys/ inside Docker)")
    print("2. Release the beasts: docker-compose up --build")
    print()

if __name__ == "__main__":
    try:
        setup_environment()
    except KeyboardInterrupt:
        print("\n\nSetup cancelled by user.")
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
