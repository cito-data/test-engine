import json
from typing import Any

def getSchemaObject() -> Any:
  with open('cito-dw-schema.json') as f:
    return json.load(f)
    