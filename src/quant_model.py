
from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from typing import Union
import pandas as pd
from prophet import Prophet


@dataclass
class _AnalysisResult(ABC):
    expectedValue: float
    expectedValueUpper: float
    expectedValueLower: float
    deviation: float
    isAnomaly: bool


@dataclass
class _ZScoreResult(_AnalysisResult):
    median: float
    medianAbsoluteDeviation: float
    meanAbsoluteDeviation: Union[float, None]
    modifiedZScore: float


@dataclass
class ResultDto:
    meanAbsoluteDeviation: Union[float, None]
    medianAbsoluteDeviation: float
    modifiedZScore: float

    expectedValue: float
    expectedValueUpper: float
    expectedValueLower: float

    deviation: float

    isAnomaly: bool


class _Analysis(ABC):
    _newDataPoint: pd.DataFrame
    _historicalData: pd.DataFrame
    _threshold: int

    @abstractmethod
    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int) -> None:
        self._newDataPoint = self._buildNewDataPointFrame(newDataPoint)
        self._historicalData = self._buildHistoricalDF(historicalData)
        self._threshold = threshold

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

    @abstractmethod
    def _runAnomalyCheck(self):
        return

    @abstractmethod
    def analyze(self):
        return


class _ZScoreAnalysis(_Analysis):
    _median: float
    _medianAbsoluteDeviation: float
    _meanAbsoluteDeviation: Union[float, None]
    _modifiedZScore: float
    _expectedValue: float
    _expectedValueUpper: float
    _expectedValueLower: float

    def __init__(self, newDataPoint: float, historicalData: "list[tuple[str, float]]", threshold: int, ) -> None:
        super().__init__(newDataPoint, historicalData, threshold)

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

    def _runAnomalyCheck(self) -> _AnalysisResult:
        newValue = self._newDataPoint['y'].values[0]

        isAnomaly = bool(abs(self._modifiedZScore) > self._threshold)
        deviation = newValue / \
            self._expectedValue if self._expectedValue > 0 else 0

        return _AnalysisResult(self._expectedValue, self._expectedValueUpper, self._expectedValueLower, deviation, isAnomaly)

    def analyze(self) -> _ZScoreResult:
        self._medianAbsoluteDeviation = self._calculateMedianAbsoluteDeviation()
        self._meanAbsoluteDeviation = self._mad()

        self._modifiedZScore = self._calculateModifiedZScore()

        self._expectedValue = self._median
        self._expectedValueUpper = self._calculateBound(self._threshold*1)
        self._expectedValueLower = self._calculateBound(
            self._threshold*-1)

        anomalyCheckResult = self._runAnomalyCheck()

        return _ZScoreResult(anomalyCheckResult.expectedValue, anomalyCheckResult.expectedValueUpper, anomalyCheckResult.expectedValueLower, anomalyCheckResult.deviation, anomalyCheckResult.isAnomaly, self._median, self._medianAbsoluteDeviation, self._meanAbsoluteDeviation, self._modifiedZScore)


class _ForecastAnalysis(_Analysis):
    _yhat: float
    _yhat_lower: float
    _yhat_upper: float
    _daily: Union[float, None]
    _daily_lower: Union[float, None]
    _daily_upper: Union[float, None]
    _weekly: Union[float, None]
    _weekly_lower: Union[float, None]
    _weekly_upper: Union[float, None]
    _yearly: Union[float, None]
    _yearly_lower: Union[float, None]
    _yearly_upper: Union[float, None]

    def __init__(self, newDataPoint: float, historicalData: "list[tuple[str, float]]", threshold: int, ) -> None:
        super().__init__(newDataPoint, historicalData, threshold)

    def _runAnomalyCheck(self) -> _AnalysisResult:
        newValue = self._newDataPoint['y'].values[0]

        if self._daily and (newValue <= self._daily_upper and newValue >= self._daily_lower):
            deviation = self._newDataPoint['y'].values[0] / \
                self._daily if self._daily > 0 else 0
            return _AnalysisResult(self._daily, self._daily_upper, self._daily_lower, deviation, False)

        if self._weekly and (newValue <= self._weekly_upper and newValue >= self._weekly_lower):
            deviation = self._newDataPoint['y'].values[0] / \
                self._weekly if self._weekly > 0 else 0
            return _AnalysisResult(self._weekly, self._weekly_upper, self._weekly_lower, deviation, False)

        if self._yearly and (newValue <= self._yearly_upper and newValue >= self._yearly_lower):
            deviation = self._newDataPoint['y'].values[0] / \
                self._yearly if self._yearly > 0 else 0
            return _AnalysisResult(self._yearly, self._yearly_upper, self._yearly_lower, deviation, False)

        deviation = self._newDataPoint['y'].values[0] / \
            self._yhat if self._yhat > 0 else 0
        isAnomaly = bool(newValue > self._yhat_upper or newValue < self._yhat_lower)

        return _AnalysisResult(self._yhat, self._yhat_upper, self._yhat_lower, deviation, isAnomaly)

    def analyze(self) -> _AnalysisResult:
        m = Prophet()
        m.fit(self._historicalData)

        dates = pd.date_range(
            end=pd.Timestamp.now(),
            periods=1,
        )

        future = pd.DataFrame({'ds': dates})

        forecast = m.predict(future)

        self._yhat = forecast['yhat'].values[0]
        self._yhat_lower = forecast['yhat_lower'].values[0]
        self._yhat_upper = forecast['yhat_upper'].values[0]
        self._daily = forecast['daily'].values[0] if 'daily' in forecast.columns else None
        self._daily_lower = forecast['daily_lower'].values[0] if 'daily_lower' in forecast.columns else None
        self._daily_upper = forecast['daily_upper'].values[0] if 'daily_upper' in forecast.columns else None
        self._weekly = forecast['weekly'].values[0] if 'weekly' in forecast.columns else None
        self._weekly_lower = forecast['weekly_lower'].values[0] if 'weekly_lower' in forecast.columns else None
        self._weekly_upper = forecast['weekly_upper'].values[0] if 'weekly_upper' in forecast.columns else None
        self._yearly = forecast['yearly'].values[0] if 'yearly' in forecast.columns else None
        self._yearly_lower = forecast['yearly_lower'].values[0] if 'yearly_lower' in forecast.columns else None
        self._yearly_upper = forecast['yearly_upper'].values[0] if 'yearly_upper' in forecast.columns else None

        anomalyCheckResult = self._runAnomalyCheck()

        return _AnalysisResult(anomalyCheckResult.expectedValue, anomalyCheckResult.expectedValueUpper, anomalyCheckResult.expectedValueLower, anomalyCheckResult.deviation, anomalyCheckResult.isAnomaly)


class _AnomalyModel(ABC):
    _newDataPoint: "tuple[str, float]"

    _zScoreAnalysis: _ZScoreAnalysis
    _forecastAnalysis: _ForecastAnalysis

    @abstractmethod
    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int) -> None:
        self._zScoreAnalysis = _ZScoreAnalysis(
            newDataPoint, historicalData, threshold)
        self._forecastAnalysis = _ForecastAnalysis(
            newDataPoint, historicalData, threshold)
        self._newDataPoint = newDataPoint

    def run(self) -> ResultDto:
        zScoreAnalysisResult = self._zScoreAnalysis.analyze()
        forecastAnalysisResult = self._forecastAnalysis.analyze()

        isAnomaly = zScoreAnalysisResult.isAnomaly and forecastAnalysisResult.isAnomaly

        deviation = zScoreAnalysisResult.deviation if abs(zScoreAnalysisResult.expectedValue - self._newDataPoint[1]) <= abs(
            forecastAnalysisResult.expectedValue - self._newDataPoint[1]) else forecastAnalysisResult.deviation

        expectedValue = zScoreAnalysisResult.expectedValue if abs(zScoreAnalysisResult.expectedValue - self._newDataPoint[1]) <= abs(
            forecastAnalysisResult.expectedValue - self._newDataPoint[1]) else forecastAnalysisResult.expectedValue
        expectedValueUpper = zScoreAnalysisResult.expectedValueUpper \
            if zScoreAnalysisResult.expectedValueUpper > forecastAnalysisResult.expectedValueUpper \
            else forecastAnalysisResult.expectedValueUpper
        expectedValueLower = zScoreAnalysisResult.expectedValueLower \
            if zScoreAnalysisResult.expectedValueLower < forecastAnalysisResult.expectedValueLower \
            else forecastAnalysisResult.expectedValueLower

        return ResultDto(zScoreAnalysisResult.meanAbsoluteDeviation, zScoreAnalysisResult.medianAbsoluteDeviation, zScoreAnalysisResult.modifiedZScore, expectedValue, expectedValueUpper, expectedValueLower, deviation, isAnomaly)

class CommonModel(_AnomalyModel):
    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int, ) -> None:
        super().__init__(newDataPoint, historicalData, threshold)
