"""Storage services module.

This module contains service implementations for storage operations,
including the ArtifactProducer for centralized file creation.
"""

from .artifact_producer import ArtifactProducer

__all__ = ["ArtifactProducer"]