
from pydantic import BaseModel
from typing import List, Dict

class ModelDatialFile(BaseModel):
    model_file_path: str
    digests: List[str]