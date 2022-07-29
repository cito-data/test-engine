from typing import Tuple

def insert(materializationAddress: str, valueSets: list[Tuple]):
  return f"""insert into {materializationAddress}
  values
  {', '.join(str(valueSet) for valueSet in valueSets)};"""