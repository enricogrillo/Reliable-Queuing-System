import random
import string


def generate_random_string(length: int = 5) -> str:
    """Generate random alphanumeric string."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


def generate_broker_id() -> str:
    """Generate broker ID: B-{5 random chars}"""
    return f"B-{generate_random_string()}"


def generate_client_id() -> str:
    """Generate client ID: C-{5 random chars}"""
    return f"C-{generate_random_string()}"


def generate_cluster_id() -> str:
    """Generate cluster ID: G-{5 random chars}"""
    return f"G-{generate_random_string()}"


def generate_queue_id(cluster_id: str) -> str:
    """Generate queue ID: Q-{cluster_part}{5 random chars}"""
    # Extract cluster part (remove G- prefix)
    cluster_part = cluster_id[2:] if cluster_id.startswith("G-") else cluster_id
    return f"Q-{cluster_part}{generate_random_string()}"


def extract_cluster_from_queue_id(queue_id: str) -> str:
    """Extract cluster ID from queue ID"""
    if not queue_id.startswith("Q-"):
        raise ValueError("Invalid queue ID format")
    
    # Remove Q- prefix
    queue_part = queue_id[2:]
    
    # Extract cluster part (remove last 5 chars which are random)
    if len(queue_part) < 6:  # Need at least 1 char for cluster + 5 for random
        raise ValueError("Invalid queue ID format")
    
    cluster_part = queue_part[:-5]  # Remove last 5 characters (random part)
    return f"G-{cluster_part}"
