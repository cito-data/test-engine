
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

    forecastValue: float
    forecastValueUpperBound: float
    forecastValueLowerBound: float

    yearlySeasonalityForecastValue: float
    yearlySeasonalityForecastValueUpperBound: float
    yearlySeasonalityForecastValueLowerBound: float

    weeklySeasonalityForecastValue: float
    weeklySeasonalityForecastValueUpperBound: float
    weeklySeasonalityForecastValueLowerBound: float

    dailySeasonalityForecastValue: float
    dailySeasonalityForecastValueUpperBound: float
    dailySeasonalityForecastValueLowerBound: float

    deviation: float


class AnomalyModel(ABC):

    _newDataPoint: pd.DataFrame
    _historicalData: pd.DataFrame
    _threshold: int

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
        self._newDataPoint = self._buildNewDataPointFrame(newDataPoint)
        self._historicalData = self._buildHistoricalDF(historicalData)
        self._threshold = threshold

    def _absoluteDeviation(self, x) -> float:
        return abs(x - self._median)

    def _calculateMedianAbsoluteDeviation(self) -> float:
        allData = pd.concat(
            [self._historicalData, self._newDataPoint], ignore_index=True)
        values = allData['y']
        self._median = float(values.median())
        absoluteDeviation = values.apply(self._absoluteDeviation)
        return float(absoluteDeviation.median())

    def _mad(self):
        allData = pd.concat(
            [self._historicalData, self._newDataPoint], ignore_index=True)
        values = allData['y']
        return (values - values.mean()).abs().mean()

    def _calculateModifiedZScore(self) -> float:
        # https://www.ibm.com/docs/en/cognos-analytics/11.1.0?topic=terms-modified-z-score
        y = self._newDataPoint['y'].values[0]

        if self._medianAbsoluteDeviation == 0 and self._meanAbsoluteDeviation == 0:
            return 0.0
        if self._medianAbsoluteDeviation == 0:
            self._meanAbsoluteDeviation = self._mad()
            return (y - self._median)/(1.253314*self._meanAbsoluteDeviation)
        return (y - self._median)/(1.486*self._medianAbsoluteDeviation)

    def _calculateBound(self, zScoreThreshold: int) -> float:
        if self._medianAbsoluteDeviation == 0:
            return (1.253314*self._meanAbsoluteDeviation)*zScoreThreshold + self._median
        return (1.486*self._medianAbsoluteDeviation)*zScoreThreshold + self._median

    def _isAnomaly(self) -> bool:
        return bool(abs(self._modifiedZScore) > self._threshold)

    def _buildNewDataPointFrame(self, newDataPoint: "tuple[str, float]") -> pd.DataFrame:
        return pd.DataFrame({'ds': pd.Series([newDataPoint[0]]), 'y': pd.Series([newDataPoint[1]])})

    def _buildHistoricalDF(self, historicalData: "list[tuple[str, float]]") -> pd.DataFrame:
        executedAt = []
        values = []

        for el in historicalData:
            executedAt.append(el[0])
            values.append(el[1])

        frame = {'ds': pd.Series(executedAt), 'y': pd.Series(values)}

        return pd.DataFrame(frame)

    def run(self) -> ResultDto:
        self._medianAbsoluteDeviation = self._calculateMedianAbsoluteDeviation()
        self._meanAbsoluteDeviation = self._mad()

        self._modifiedZScore = self._calculateModifiedZScore()

        self._expectedValue = self._median
        self._expectedValueUpperBound = self._calculateBound(self._threshold*1)
        self._expectedValueLowerBound = self._calculateBound(
            self._threshold*-1)

        self._deviation = self._newDataPoint['y'].values[0] / \
            self._expectedValue if self._expectedValue > 0 else 0

        m = Prophet()
        m.fit(self._historicalData)

        dates = pd.date_range(
            end=pd.Timestamp.now(),
            periods=1,
        )

        future = pd.DataFrame({'ds': dates})

        forecast = m.predict(future)

        forecastResult = {
            'yhat': forecast['yhat'].values[0],
            'yhat_lower': forecast['yhat_lower'].values[0],
            'yhat_upper': forecast['yhat_upper'].values[0],
            'daily': forecast['daily'].values[0] if 'daily' in forecast.columns else None,
            'daily_lower': forecast['daily_lower'].values[0] if 'daily_lower' in forecast.columns else None,
            'daily_upper': forecast['daily_upper'].values[0] if 'daily_upper' in forecast.columns else None,
            'weekly': forecast['weekly'].values[0] if 'weekly' in forecast.columns else None,
            'weekly_lower': forecast['weekly_lower'].values[0] if 'weekly_lower' in forecast.columns else None,
            'weekly_upper': forecast['weekly_upper'].values[0] if 'weekly_upper' in forecast.columns else None,
            'yearly': forecast['yearly'].values[0] if 'yearly' in forecast.columns else None,
            'yearly_lower': forecast['yearly_lower'].values[0] if 'yearly_lower' in forecast.columns else None,
            'yearly_upper': forecast['yearly_upper'].values[0] if 'yearly_upper' in forecast.columns else None,
        }

        return ResultDto(self._meanAbsoluteDeviation, self._medianAbsoluteDeviation, self._modifiedZScore, self._isAnomaly(), self._expectedValue, self._expectedValueUpperBound, self._expectedValueLowerBound, forecastResult['yhat'], forecastResult['yhat_upper'], forecastResult['yhat_lower'], forecastResult['daily'], forecastResult['daily_upper'], forecastResult['daily_lower'], forecastResult['weekly'], forecastResult['weekly_upper'], forecastResult['weekly_lower'], forecastResult['yearly'], forecastResult['yearly_upper'], forecastResult['yearly_lower'], self._deviation)


class CommonModel(AnomalyModel):
    def __init__(self, newData: float, historicalData: "list[tuple[str, float]]", threshold: int, ) -> None:
        super().__init__(newData, historicalData, threshold)
