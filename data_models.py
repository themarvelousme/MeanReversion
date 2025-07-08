"""
Data models for mean reversion analysis system
"""
from dataclasses import dataclass
from typing import List


@dataclass
class MeanReversionSignal:
    """Data class to store mean reversion analysis results"""
    symbol: str
    current_price: float
    mean_price: float
    std_dev: float
    z_score: float
    rsi: float
    bollinger_position: float
    is_oversold: bool
    is_overbought: bool
    reversion_strength: str
    days_from_mean: int
    adf_pvalue: float
    hurst_exponent: float
    volume_ratio: float
    price_change_5d: float
    volatility: float
    market_cap: str
    sector: str


@dataclass
class TradingRecommendation:
    """Data class for trading recommendations"""
    signal: MeanReversionSignal
    action: str  # 'BUY_PUTS', 'BUY_CALLS', 'WAIT', 'AVOID'
    confidence: str  # 'HIGH', 'MEDIUM', 'LOW'
    time_horizon: str  # 'SHORT' (1-2 weeks), 'MEDIUM' (2-6 weeks), 'LONG' (6+ weeks)
    entry_criteria: List[str]
    exit_criteria: List[str]
    risk_factors: List[str]
    educational_notes: List[str]
