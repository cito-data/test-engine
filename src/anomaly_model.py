
from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from typing import Union
import pandas as pd 

@dataclass
class ResultDto:
  meanAbsoluteDeviation: Union[float, None]
  medianAbsoluteDeviation: float
  modifiedZScore: float
  
  isAnomaly: bool

  expectedValue: float
  expectedValueUpperBound: float
  expectedValueLowerBound: float

  deviation: float

  executedOn: str

class AnomalyModel(ABC):

  _newDataPoint: float
  _historicalData: "list[float]"
  _threshold: int

  _dataSeries: pd.Series

  _median: float
  _medianAbsoluteDeviation: float
  _meanAbsoluteDeviation: Union[float, None] = None
  _modifiedZScore: float

  _expectedValue: float
  _expectedValueUpperBound: float
  _expectedValueLowerBound: float

  _deviation: float
  
  @abstractmethod
  def __init__(self, newDataPoint: float, historicalData: "list[float]", threshold: int) -> None:
    self._newDataPoint = newDataPoint
    self._historicalData = historicalData
    self._threshold = threshold
  
  def _absoluteDeviation(self, x) -> float:
    return abs(x - self._median)

  def _calculateMedianAbsoluteDeviation(self) -> float:
    self._median = float(self._dataSeries.median())
    absoluteDeviation = self._dataSeries.apply(self._absoluteDeviation)
    return float(absoluteDeviation.median())

  def _mad(dataSeries):
    return(dataSeries - dataSeries.mean()).abs().mean()

  def _calculateModifiedZScore(self, x) -> float:
    # https://www.ibm.com/docs/en/cognos-analytics/11.1.0?topic=terms-modified-z-score
    if self._medianAbsoluteDeviation == 0 and self._meanAbsoluteDeviation == 0:
      return 0.0
    if self._medianAbsoluteDeviation == 0:
      self._meanAbsoluteDeviation = self._mad(self._dataSeries)
      return (x - self._median)/(1.253314*self._meanAbsoluteDeviation)
    return (x - self._median)/(1.486*self._medianAbsoluteDeviation)

  def _calculateBound(self, zScoreThreshold: int) -> float:
    if self._medianAbsoluteDeviation == 0:
      return (1.253314*self._meanAbsoluteDeviation)*zScoreThreshold + self._median
    return (1.486*self._medianAbsoluteDeviation)*zScoreThreshold + self._median

  def _isAnomaly(self) -> bool:
    return bool(abs(self._modifiedZScore) > self._threshold)
    
  def run(self) -> ResultDto:
    self._dataSeries = pd.Series([self._newDataPoint] + self._historicalData)
    self._medianAbsoluteDeviation = self._calculateMedianAbsoluteDeviation()
    self._meanAbsoluteDeviation = self._mad(self._dataSeries)

    self._modifiedZScore = self._calculateModifiedZScore(self._newDataPoint)

    self._expectedValue = self._median
    self._expectedValueUpperBound = self._calculateBound(self._threshold*1)
    self._expectedValueLowerBound = self._calculateBound(self._threshold*-1)

    self._deviation = self._newDataPoint/self._expectedValue

    return ResultDto(self._meanAbsoluteDeviation, self._medianAbsoluteDeviation, self._modifiedZScore, self._isAnomaly(), self._expectedValue, self._expectedValueUpperBound, self._expectedValueLowerBound, self._deviation, datetime.datetime.utcnow().isoformat())

class CommonModel(AnomalyModel):
  def __init__(self, newData: float, historicalData: "list[float]", threshold: int, ) -> None:
    super().__init__(newData, historicalData, threshold)

  

  

     
