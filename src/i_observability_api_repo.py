

from abc import ABC, abstractmethod
from test_execution_result import QualTestExecutionResult, QuantTestExecutionResult


class IObservabilityApiRepo(ABC):
    @abstractmethod
    def sendQuantTestExecutionResult(self, result: QuantTestExecutionResult, jwt: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def sendQualTestExecutionResult(self, result: QualTestExecutionResult, jwt: str) -> None:
        raise NotImplementedError
