"""
Enhanced mean reversion detection system with multithreading support
"""
import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats
from typing import List, Dict, Tuple, Optional
import warnings
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from tqdm import tqdm
import logging
from datetime import datetime

from data_models import MeanReversionSignal, TradingRecommendation

warnings.filterwarnings('ignore')


class EnhancedMeanReversionDetector:
    """
    Enhanced mean reversion detection system with multithreading support
    """
    
    def __init__(self, lookback_period: int = 50, z_threshold: float = 2.0, max_workers: int = None, 
                 log_filename: str = "analysis.log"):
        """
        Initialize the enhanced detector
        
        Args:
            lookback_period: Number of days to look back for calculations
            z_threshold: Z-score threshold for extreme values
            max_workers: Maximum number of threads (None = auto-detect)
            log_filename: File to write logs
        """
        self.lookback_period = lookback_period
        self.z_threshold = z_threshold
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        self.lock = threading.Lock()
        
        # Default blue chip stocks
        self.default_stocks = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK-B',
            'UNH', 'JNJ', 'XOM', 'JPM', 'PG', 'CVX', 'HD', 'MA', 'PFE', 'ABBV',
            'BAC', 'KO', 'AVGO', 'PEP', 'TMO', 'COST', 'WMT', 'DIS', 'ABT',
            'CRM', 'ACN', 'VZ', 'ADBE', 'NFLX', 'NKE', 'ORCL', 'CMCSA', 'T',
            'INTC', 'MRK', 'COP', 'WFC', 'AMD', 'TXN', 'MDT', 'UPS', 'QCOM',
            'HON', 'AMAT', 'LOW', 'SPGI', 'LMT', 'IBM', 'GS', 'CVS', 'GILD'
        ]
        
        # Sector mappings (simplified)
        self.sector_map = {
            'AAPL': 'Technology', 'MSFT': 'Technology', 'GOOGL': 'Technology',
            'AMZN': 'Consumer Discretionary', 'TSLA': 'Consumer Discretionary',
            'META': 'Technology', 'NVDA': 'Technology', 'JPM': 'Financials',
            'BAC': 'Financials', 'GS': 'Financials', 'WFC': 'Financials',
            'JNJ': 'Healthcare', 'PFE': 'Healthcare', 'ABBV': 'Healthcare',
            'XOM': 'Energy', 'CVX': 'Energy', 'COP': 'Energy',
            'KO': 'Consumer Staples', 'PG': 'Consumer Staples', 'WMT': 'Consumer Staples'
        }
        
        # Performance tracking
        self.analysis_times = []
        self.failed_symbols = []
        
        # Setup logging
        logging.basicConfig(
            filename=log_filename,
            filemode='w',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )
        self.logger = logging.getLogger()
    
    def load_stock_list(self, file_path: str) -> List[str]:
        """
        Load stock symbols from a file
        
        Supports:
        - CSV files with 'symbol' column
        - TXT files with one symbol per line
        - JSON files with list of symbols
        """
        if not os.path.exists(file_path):
            self.logger.warning(f"File {file_path} not found. Using default stock list.")
            return self.default_stocks
        
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.csv':
                df = pd.read_csv(file_path)
                if 'symbol' in df.columns:
                    return df['symbol'].str.upper().tolist()
                elif 'Symbol' in df.columns:
                    return df['Symbol'].str.upper().tolist()
                else:
                    # Assume first column contains symbols
                    return df.iloc[:, 0].str.upper().tolist()
            
            elif file_ext == '.txt':
                with open(file_path, 'r') as f:
                    symbols = [line.strip().upper() for line in f if line.strip()]
                return symbols
            
            elif file_ext == '.json':
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return [s.upper() for s in data]
                    elif isinstance(data, dict) and 'symbols' in data:
                        return [s.upper() for s in data['symbols']]
            
            self.logger.warning(f"Unsupported file format: {file_ext}. Using default stock list.")
            return self.default_stocks
            
        except Exception as e:
            self.logger.error(f"Error loading stock list from {file_path}: {e}")
            return self.default_stocks
    
    def create_sample_stock_file(self, file_path: str = "stock_list.csv"):
        """Create a sample stock list file for users"""
        sample_data = {
            'symbol': ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'JPM', 'JNJ', 'KO', 'PG'],
            'sector': ['Technology', 'Technology', 'Technology', 'Auto', 'Finance', 'Healthcare', 'Consumer', 'Consumer'],
            'notes': ['Apple Inc', 'Microsoft', 'Alphabet', 'Tesla', 'JPMorgan', 'Johnson & Johnson', 'Coca Cola', 'Procter & Gamble']
        }
        
        df = pd.DataFrame(sample_data)
        df.to_csv(file_path, index=False)
        self.logger.info(f"Sample stock list created: {file_path}")
    
    def fetch_data(self, symbol: str, period: str = "1y") -> pd.DataFrame:
        """Fetch stock data with additional metrics - thread-safe"""
        try:
            stock = yf.Ticker(symbol)
            data = stock.history(period=period)
            
            # Add volume moving average
            if not data.empty:
                data['Volume_MA'] = data['Volume'].rolling(window=20).mean()
                data['Volume_Ratio'] = data['Volume'] / data['Volume_MA']
            
            return data
        except Exception as e:
            with self.lock:
                self.failed_symbols.append((symbol, str(e)))
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_stock_info(self, symbol: str) -> Dict:
        """Get additional stock information - thread-safe"""
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            market_cap = info.get('marketCap', 0)
            if market_cap > 200e9:
                cap_size = "Large Cap"
            elif market_cap > 10e9:
                cap_size = "Mid Cap"
            elif market_cap > 2e9:
                cap_size = "Small Cap"
            else:
                cap_size = "Micro Cap"
            
            return {
                'market_cap': cap_size,
                'sector': info.get('sector', self.sector_map.get(symbol, 'Unknown')),
                'beta': info.get('beta', 1.0),
                'forward_pe': info.get('forwardPE', None)
            }
        except Exception as e:
            self.logger.error(f"Error getting stock info for {symbol}: {e}")
            return {
                'market_cap': 'Unknown',
                'sector': self.sector_map.get(symbol, 'Unknown'),
                'beta': 1.0,
                'forward_pe': None
            }
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std_dev: int = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate Bollinger Bands"""
        sma = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        return upper_band, sma, lower_band
    
    def adf_test(self, series: pd.Series) -> float:
        """Augmented Dickey-Fuller test for stationarity"""
        try:
            from statsmodels.tsa.stattools import adfuller
            result = adfuller(series.dropna())
            return result[1]
        except ImportError:
            return self.simple_stationarity_test(series)
    
    def simple_stationarity_test(self, series: pd.Series) -> float:
        """Simple stationarity test using autocorrelation"""
        series_clean = series.dropna()
        if len(series_clean) < 2:
            return 1.0
        
        autocorr = np.corrcoef(series_clean[:-1], series_clean[1:])[0, 1]
        return abs(autocorr)
    
    def hurst_exponent(self, prices: pd.Series) -> float:
        """Calculate Hurst exponent"""
        try:
            prices_clean = prices.dropna()
            if len(prices_clean) < 20:
                return 0.5
            
            returns = np.log(prices_clean / prices_clean.shift(1)).dropna()
            lags = range(2, min(20, len(returns)//2))
            rs_values = []
            
            for lag in lags:
                chunks = [returns[i:i+lag] for i in range(0, len(returns), lag) if len(returns[i:i+lag]) == lag]
                
                if len(chunks) == 0:
                    continue
                
                rs_chunk = []
                for chunk in chunks:
                    mean_return = chunk.mean()
                    cumulative_devs = (chunk - mean_return).cumsum()
                    
                    R = cumulative_devs.max() - cumulative_devs.min()
                    S = chunk.std()
                    
                    if S != 0:
                        rs_chunk.append(R/S)
                
                if rs_chunk:
                    rs_values.append(np.mean(rs_chunk))
            
            if len(rs_values) < 2:
                return 0.5
            
            log_lags = np.log(lags[:len(rs_values)])
            log_rs = np.log(rs_values)
            
            slope, _, _, _, _ = stats.linregress(log_lags, log_rs)
            return slope
        
        except Exception as e:
            self.logger.error(f"Error calculating Hurst exponent: {e}")
            return 0.5
    
    def analyze_stock_wrapper(self, symbol: str) -> Tuple[str, Optional[MeanReversionSignal], float]:
        """Wrapper for multithreaded stock analysis"""
        start_time = time.time()
        signal = self.analyze_stock(symbol)
        analysis_time = time.time() - start_time
        
        with self.lock:
            self.analysis_times.append(analysis_time)
        
        return symbol, signal, analysis_time
    
    def analyze_stock(self, symbol: str) -> Optional[MeanReversionSignal]:
        """Enhanced stock analysis with additional metrics"""
        data = self.fetch_data(symbol)
        
        if data.empty or len(data) < self.lookback_period:
            return None
        
        # Get stock info
        stock_info = self.get_stock_info(symbol)
        
        # Get recent data
        recent_data = data.tail(self.lookback_period)
        prices = recent_data['Close']
        volumes = recent_data['Volume']
        current_price = prices.iloc[-1]
        
        # Calculate basic statistics
        mean_price = prices.mean()
        std_dev = prices.std()
        z_score = (current_price - mean_price) / std_dev if std_dev > 0 else 0
        
        # Calculate additional metrics
        rsi = self.calculate_rsi(prices).iloc[-1]
        
        upper_bb, middle_bb, lower_bb = self.calculate_bollinger_bands(prices)
        bb_width = upper_bb.iloc[-1] - lower_bb.iloc[-1]
        if bb_width > 0:
            bollinger_position = (current_price - lower_bb.iloc[-1]) / bb_width
        else:
            bollinger_position = 0.5
        
        # Volume analysis
        volume_ratio = volumes.iloc[-1] / volumes.mean() if volumes.mean() > 0 else 1.0
        
        # Price change analysis
        price_5d_ago = prices.iloc[-6] if len(prices) > 5 else prices.iloc[0]
        price_change_5d = (current_price - price_5d_ago) / price_5d_ago * 100
        
        # Volatility (20-day rolling std of returns)
        returns = prices.pct_change().dropna()
        volatility = returns.rolling(window=20).std().iloc[-1] * np.sqrt(252) * 100  # Annualized
        
        # Determine conditions
        is_oversold = (z_score < -self.z_threshold) or (rsi < 30) or (bollinger_position < 0.1)
        is_overbought = (z_score > self.z_threshold) or (rsi > 70) or (bollinger_position > 0.9)
        
        reversion_strength = self._calculate_reversion_strength(z_score, rsi, bollinger_position)
        
        # Days from mean
        days_from_mean = 0
        for i in range(len(prices)-1, -1, -1):
            if abs(prices.iloc[i] - mean_price) < std_dev * 0.5:
                days_from_mean = len(prices) - 1 - i
                break
        
        # Statistical tests
        adf_pvalue = self.adf_test(prices)
        hurst_exp = self.hurst_exponent(prices)
        
        return MeanReversionSignal(
            symbol=symbol,
            current_price=current_price,
            mean_price=mean_price,
            std_dev=std_dev,
            z_score=z_score,
            rsi=rsi,
            bollinger_position=bollinger_position,
            is_oversold=is_oversold,
            is_overbought=is_overbought,
            reversion_strength=reversion_strength,
            days_from_mean=days_from_mean,
            adf_pvalue=adf_pvalue,
            hurst_exponent=hurst_exp,
            volume_ratio=volume_ratio,
            price_change_5d=price_change_5d,
            volatility=volatility,
            market_cap=stock_info['market_cap'],
            sector=stock_info['sector']
        )
    
    def _calculate_reversion_strength(self, z_score: float, rsi: float, bb_pos: float) -> str:
        """Calculate overall reversion strength"""
        extreme_count = 0
        
        if abs(z_score) > 2.5:
            extreme_count += 2
        elif abs(z_score) > 1.5:
            extreme_count += 1
        
        if rsi < 25 or rsi > 75:
            extreme_count += 2
        elif rsi < 35 or rsi > 65:
            extreme_count += 1
        
        if bb_pos < 0.05 or bb_pos > 0.95:
            extreme_count += 2
        elif bb_pos < 0.15 or bb_pos > 0.85:
            extreme_count += 1
        
        if extreme_count >= 4:
            return "Strong"
        elif extreme_count >= 2:
            return "Moderate"
        elif extreme_count >= 1:
            return "Weak"
        else:
            return "None"
    
    def scan_market(self, stock_file: str = None, custom_symbols: List[str] = None, 
                   show_progress: bool = True) -> List[MeanReversionSignal]:
        """Enhanced multithreaded market scanning"""
        if custom_symbols:
            symbols = custom_symbols
        elif stock_file:
            symbols = self.load_stock_list(stock_file)
        else:
            symbols = self.default_stocks
        
        # Reset tracking variables
        self.analysis_times = []
        self.failed_symbols = []
        
        results = []
        start_time = time.time()
        
        self.logger.info(f"Starting multithreaded analysis of {len(symbols)} stocks using {self.max_workers} workers.")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.analyze_stock_wrapper, symbol): symbol for symbol in symbols}
            
            if show_progress:
                for future in tqdm(as_completed(futures), total=len(futures), desc="Analyzing stocks"):
                    symbol = futures[future]
                    try:
                        _, signal, analysis_time = future.result()
                        if signal:
                            results.append(signal)
                        self.logger.info(f"Processed {symbol} in {analysis_time:.2f} seconds.")
                    except Exception as e:
                        self.failed_symbols.append((symbol, str(e)))
                        self.logger.error(f"Error processing {symbol}: {e}")
            else:
                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        _, signal, analysis_time = future.result()
                        if signal:
                            results.append(signal)
                        self.logger.info(f"Processed {symbol} in {analysis_time:.2f} seconds.")
                    except Exception as e:
                        self.failed_symbols.append((symbol, str(e)))
                        self.logger.error(f"Error processing {symbol}: {e}")
        
        total_time = time.time() - start_time
        avg_time = np.mean(self.analysis_times) if self.analysis_times else 0
        
        self.logger.info(f"Analysis completed in {total_time:.2f} seconds")
        self.logger.info(f"Average time per stock: {avg_time:.2f} seconds")
        
        if self.failed_symbols:
            self.logger.warning(f"Failed to analyze {len(self.failed_symbols)} stocks: {[s for s, _ in self.failed_symbols]}")
        
        return results
    
    def generate_trading_recommendation(self, signal: MeanReversionSignal) -> TradingRecommendation:
        """Generate trading recommendations based on signal"""
        # Determine action
        if signal.is_oversold and signal.reversion_strength in ["Strong", "Moderate"]:
            action = "BUY_CALLS"
            confidence = "HIGH" if signal.reversion_strength == "Strong" else "MEDIUM"
            horizon = "SHORT"
        elif signal.is_overbought and signal.reversion_strength in ["Strong", "Moderate"]:
            action = "BUY_PUTS"
            confidence = "HIGH" if signal.reversion_strength == "Strong" else "MEDIUM"
            horizon = "SHORT"
        else:
            action = "WAIT"
            confidence = "LOW"
            horizon = "SHORT"
        
        entry_criteria = []
        exit_criteria = []
        risk_factors = []
        educational_notes = []
        
        if action == "BUY_CALLS":
            entry_criteria.append("Price significantly below mean (z-score < -2)")
            entry_criteria.append("RSI indicates oversold (below 30)")
            entry_criteria.append("Bollinger position near lower band (< 0.1)")
            
            exit_criteria.append("Price returns near or above mean")
            exit_criteria.append("RSI rises above 50")
            
            risk_factors.append("Stock may continue downward trend if market conditions worsen")
            risk_factors.append("High volatility can increase option premium")
            
            educational_notes.append("Mean reversion strategy assumes price will revert to average")
        
        elif action == "BUY_PUTS":
            entry_criteria.append("Price significantly above mean (z-score > 2)")
            entry_criteria.append("RSI indicates overbought (above 70)")
            entry_criteria.append("Bollinger position near upper band (> 0.9)")
            
            exit_criteria.append("Price returns near or below mean")
            exit_criteria.append("RSI falls below 50")
            
            risk_factors.append("Stock may continue upward momentum longer than expected")
            risk_factors.append("Market news or events can invalidate mean reversion")
            
            educational_notes.append("Mean reversion strategy assumes price will revert to average")
        
        else:
            entry_criteria.append("No clear trading signal detected")
            exit_criteria.append("Wait for stronger signals")
        
        return TradingRecommendation(
            signal=signal,
            action=action,
            confidence=confidence,
            time_horizon=horizon,
            entry_criteria=entry_criteria,
            exit_criteria=exit_criteria,
            risk_factors=risk_factors,
            educational_notes=educational_notes
        )
