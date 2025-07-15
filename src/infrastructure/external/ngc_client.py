"""
NGC Client for dealing with ECS Secret Agent.

Functions for retrieving secrets from the NGC secret management system.
"""

import subprocess
import logging
from functools import lru_cache
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def has_value(value: str) -> bool:
    """Check if a value is not None and not empty."""
    return value is not None and value.strip() != ""


class NGCClient:
    """
    SecretAgent class for dealing with ECS Secret Agent.
    
    This class provides methods to retrieve secrets from the ECS Secret Agent.
    """
    
    secrets: Dict[str, str] = dict()
    
    def get_secret(
        self,
        secret_nickname: str,
        csiid: str,
        get_secret_command: str = "ngc getSecret --secretNickname {secret_nickname} --csiid {csiid}",
    ) -> str:
        """
        Retrieve a secret using the NGC command.
        
        Args:
            secret_nickname: The nickname of the secret to retrieve
            csiid: The CSIID for authentication
            get_secret_command: The command template to execute
            
        Returns:
            The secret value as a string
        """
        
        if self.secrets and secret_nickname in self.secrets:
            secret: str = self.secrets[secret_nickname]
            return secret
        
        try:
            cmd: str = get_secret_command.format(
                secret_nickname=secret_nickname,
                csiid=csiid
            )
            
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True,
                universal_newlines=True,
            )
            
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                raise ValueError(f"Error executing command: {cmd} Stdout: {stdout} Stderr: {stderr}")
            if stdout:
                secret_value: str = stdout.strip()
                if not has_value(secret_value):
                    raise ValueError(f"Secret value is empty for {secret_nickname}")
                
                self.secrets[secret_nickname] = secret_value
                logger.info("Database password fetched using ngc client")
                return self.secrets[secret_nickname]
            else:
                raise ValueError(f"No stdout emitted from {cmd}")
                
        except ValueError as error:
            logger.error(error)
            raise
        except Exception as error:
            logger.error(f"Unexpected error fetching secret: {error}")
            raise


@lru_cache()
def get_ngc_agent():
    """Get the singleton instance of NGCClient."""
    return NGCClient()