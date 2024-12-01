from pydantic import BaseModel

class AdjustmentRequest(BaseModel):
    name: str
    value: int
    operations: str
