
from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from typing import Union
import pandas as pd 
from prophet import Prophet

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

@dataclass
class Frame:
  executedAt: pd.Series
  values: pd.Series

class AnomalyModel(ABC):

  _newDataPoint: "tuple[str, float]"
  _historicalData: "list[tuple[str, float]]"
  _threshold: int

  _data: pd.DataFrame

  _median: float
  _medianAbsoluteDeviation: float
  _meanAbsoluteDeviation: Union[float, None] = None
  _modifiedZScore: float

  _expectedValue: float
  _expectedValueUpperBound: float
  _expectedValueLowerBound: float

  _deviation: float
  
  @abstractmethod
  def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int) -> None:
    self._newDataPoint = newDataPoint
    self._historicalData = historicalData
    self._threshold = threshold
  
  def _absoluteDeviation(self, x) -> float:
    return abs(x - self._median)

  def _calculateMedianAbsoluteDeviation(self) -> float:
    values = self._data['y']
    self._median = float(values.median())
    absoluteDeviation = values.apply(self._absoluteDeviation)
    return float(absoluteDeviation.median())

  def _mad(self):
    values = self._data['y']
    return(values - values.mean()).abs().mean()

  def _calculateModifiedZScore(self) -> float:
    # https://www.ibm.com/docs/en/cognos-analytics/11.1.0?topic=terms-modified-z-score
    x = self._newDataPoint[1]

    if self._medianAbsoluteDeviation == 0 and self._meanAbsoluteDeviation == 0:
      return 0.0
    if self._medianAbsoluteDeviation == 0:
      self._meanAbsoluteDeviation = self._mad()
      return (x - self._median)/(1.253314*self._meanAbsoluteDeviation)
    return (x - self._median)/(1.486*self._medianAbsoluteDeviation)

  def _calculateBound(self, zScoreThreshold: int) -> float:
    if self._medianAbsoluteDeviation == 0:
      return (1.253314*self._meanAbsoluteDeviation)*zScoreThreshold + self._median
    return (1.486*self._medianAbsoluteDeviation)*zScoreThreshold + self._median

  def _isAnomaly(self) -> bool:
    return bool(abs(self._modifiedZScore) > self._threshold)
    
  def _buildFrame(self) -> Frame:
    executedAt = []
    values = []
    
    for el in self._historicalData:
      executedAt.append(el[0])
      values.append(el[1])
    
    frame = {'ds': pd.Series(executedAt + [self._newDataPoint[0]]), 'y': pd.Series(values + [self._newDataPoint[1]])}

    return frame

  def run(self) -> ResultDto:
    self._data = pd.DataFrame(self._buildFrame())
    self._medianAbsoluteDeviation = self._calculateMedianAbsoluteDeviation()
    self._meanAbsoluteDeviation = self._mad()

    self._modifiedZScore = self._calculateModifiedZScore()

    self._expectedValue = self._median
    self._expectedValueUpperBound = self._calculateBound(self._threshold*1)
    self._expectedValueLowerBound = self._calculateBound(self._threshold*-1)

    self._deviation = self._newDataPoint[1]/self._expectedValue if self._expectedValue > 0 else 0 

    m = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True)
    m.fit(self._data)

    dates = pd.date_range(
        end=pd.Timestamp.now(),
        periods=1,
        )

    future = pd.DataFrame({'ds': dates})

    forecast = m.predict(future)


    return ResultDto(self._meanAbsoluteDeviation, self._medianAbsoluteDeviation, self._modifiedZScore, self._isAnomaly(), self._expectedValue, self._expectedValueUpperBound, self._expectedValueLowerBound, self._deviation)

class CommonModel(AnomalyModel):
  def __init__(self, newData: float, historicalData: "list[tuple[str, float]]", threshold: int, ) -> None:
    super().__init__(newData, historicalData, threshold)

  

  

     
