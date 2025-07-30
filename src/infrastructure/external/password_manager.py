"""Singleton Class for managing PostgreSQL password retrieval"""

from src.infrastructure.external.ngc_client import (
    NGCClient,
    get_ngc_agent,
    has_value
)

from src.infrastructure.config.settings import (
    get_settings,
    Settings,
)

from typing import Union


def get_postgres_password(settings: Settings) -> Union[str, None]:
    """
    Helper method to get the POSTGRESQL password from a given settings object.
    
    If a password is not provided, it will call the SecretAgent.
    
    Args:
        settings: The settings object containing the POSTGRESQL password.
    
    Returns:
        The POSTGRESQL password, or None if not found.
    """
    db_password: str = settings.POSTGRESQL_PASSWORD
    
    if not has_value(db_password):
        # Get the password from the SecretAgent
        secretagent: NGCClient = get_ngc_agent()
        
        # Get the CSIID from settings or use default
        csiid = getattr(settings, 'NGC_CSIID', '179492')
        
        # Get the password from the SecretAgent
        db_password: str = secretagent.get_secret(
            secret_nickname=settings.POSTGRESQL_PASSWORD_SECRET_NAME,
            csiid=csiid
        )
    
    return db_password


class PasswordManager:
    """
    PasswordManager class to retrieve passwords for PostgreSQL.
    Implements singleton pattern with retry logic.
    """
    
    _instance = None
    _postgresql_password = None
    __max_retries = 3
    
    def __new__(cls, *args, **kwargs):
        """Ensures only one instance of PasswordManager is created."""
        if cls._instance is None:
            cls._instance = super(PasswordManager, cls).__new__(cls)
        return cls._instance
    
    def postgresql_fetch(self) -> Union[str, None]:
        """
        Retrieves the PostgreSQL password using the SecretAgent.
        Retries up to a maximum number of times if retrieval fails.
        
        Returns:
            The PostgreSQL password or None if all attempts fail.
        """
        for i in range(self.__max_retries):
            try:
                settings: Settings = get_settings()
                
                # get the PostgreSQL password using the SecretAgent
                self._postgresql_password = get_postgres_password(settings)
                if self._postgresql_password:
                    print("PostgreSQL Password retrieved successfully")
                    break
            except Exception as e:
                if i == self.__max_retries - 1:
                    print("Password retrieve exceeds max retry...")
                    raise e
                else:
                    print(f"Password retrieve attempt {i + 1} failed. Retrying...")
        
        return self._postgresql_password
    
    def get_postgres_password(self) -> Union[str, None]:
        """
        Returns the postgres password retrieved by the most recent fetch method.
        
        Returns:
            The cached PostgreSQL password or None.
        """
        return self._postgresql_password


# Create singleton instance
password_manager = PasswordManager()


def get_password_manager() -> PasswordManager:
    """
    Returns the PasswordManager singleton instance.
    
    Returns:
        The PasswordManager instance.
    """
    return password_manager