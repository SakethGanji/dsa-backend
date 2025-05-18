from pydantic import BaseModel, constr
from datetime import datetime

class UserCreate(BaseModel):
    soeid: constr(min_length=7, max_length=7)
    password: str  # Changed from password_hash
    role_id: int

class UserOut(BaseModel):
    id: int
    soeid: str
    role_id: int
    created_at: datetime
    updated_at: datetime
