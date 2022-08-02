def getDistributionQuery(materializationAddress: str, columnName: str):
    return f"""select median({columnName}) from {materializationAddress};"""


def getFreshnessQuery(materializationAddress: str, columnName: str):
    return f""" select convert_timezone('UTC', {columnName}) from {materializationAddress}
    where last_altered is not null
    order by last_altered desc limit 1;
  """

def getCardinalityQuery(materializationAddress: str, columnName: str):
    return f"""
    select count(distinct({columnName})) as distinct_value_count, 
    from {materializationAddress};
  """

def getUniquenessQuery(materializationAddress: str, columnName: str):
    return f"""
    select count(distinct({columnName})) as distinct_value_count,
    count({columnName}) as non_null_value_count 
    from {materializationAddress};
  """

def getNullnessQuery(materializationAddress: str, columnName: str):
    return f"""
    select sum(case when {columnName} is null then 1 else 0 end) as null_value_count,
    count({columnName}) as non_null_value_count 
    from {materializationAddress};
  """

def getSortednessQuery():
    pass
