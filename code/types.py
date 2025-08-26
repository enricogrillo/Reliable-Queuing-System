#!/usr/bin/env python3
"""
Common types and enums used across the broker system.
"""

from enum import Enum
from dataclasses import dataclass


class BrokerRole(Enum):
    """Broker role enumeration."""
    LEADER = "leader"
    REPLICA = "replica"


class BrokerStatus(Enum):
    """Broker status enumeration."""
    STARTING = "starting"
    ACTIVE = "active"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass
class ClusterMember:
    """Information about a cluster member."""
    broker_id: str
    host: str
    port: int
    role: BrokerRole
    status: BrokerStatus
    last_heartbeat: float
    cluster_version: int
