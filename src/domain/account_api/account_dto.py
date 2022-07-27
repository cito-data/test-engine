from dataclasses import dataclass


@dataclass
class AccountDto:
  id: str
  userId: str
  organizationId: str
  modifiedOn: int