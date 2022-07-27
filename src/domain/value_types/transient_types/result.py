from typing import Generic, TypeVar, Union
import logging

logger = logging.getLogger(__name__)


T = TypeVar("T")
U = TypeVar("U")

class Result(Generic(T)):

  def __init__(self, success: bool, value: Union[T, None], error: Union[str, None]) -> None:
    self._error = error
    self._success = success
    self._value = value 

  @property
  def error(self) -> Union[str, None]:
    return self._error

  @property
  def success(self) -> bool:
    return self._success

  @property
  def value(self) -> Union[T, None]:
    if not self._success:
      errorMessage = f'An error occured. Cannot get the value of an error result: {self._error}'
      logger.error(errorMessage)
      raise Exception(errorMessage)      
    return self._value

  @staticmethod
  def ok(value: Union[U, None]):
    return Result(True, value, None)

  @staticmethod
  def fail(error: str):
    return Result(False, None, error)