"""
Celery tasks for stock analysis database operations
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
from decimal import Decimal

from celery import Celery
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Celery app configuration
app = Celery('stock_analysis')
app.conf.update(
    broker_url=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
    result_backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0'),
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# Make sure the app is available for task discovery
celery_app = app

# Database connection configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'stock_analysis'),
    'user': os.getenv('DB_USER', 'marc'),
    'password': os.getenv('DB_PASSWORD', 'tr32iocy'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_db_connection():
    """Get database connection with error handling"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

# PUSH TASKS (Writing to Database)

@app.task(bind=True, retry_backoff=True, max_retries=3)
def create_or_update_stock(self, symbol: str, name: str = None, sector: str = None, 
                          market_cap_category: str = None) -> Dict[str, Any]:
    """
    Create or update stock information
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Check if stock exists
                cur.execute("SELECT id FROM stocks WHERE symbol = %s", (symbol,))
                result = cur.fetchone()
                
                if result:
                    # Update existing stock
                    update_fields = []
                    params = []
                    
                    if name:
                        update_fields.append("name = %s")
                        params.append(name)
                    if sector:
                        update_fields.append("sector = %s")
                        params.append(sector)
                    if market_cap_category:
                        update_fields.append("market_cap_category = %s")
                        params.append(market_cap_category)
                    
                    if update_fields:
                        update_fields.append("updated_at = CURRENT_TIMESTAMP")
                        params.append(result['id'])
                        
                        query = f"UPDATE stocks SET {', '.join(update_fields)} WHERE id = %s RETURNING *"
                        cur.execute(query, params)
                        updated_stock = cur.fetchone()
                        
                        logger.info(f"Updated stock {symbol}")
                        return {'action': 'updated', 'stock': dict(updated_stock)}
                    else:
                        return {'action': 'no_change', 'stock': dict(result)}
                else:
                    # Create new stock
                    cur.execute("""
                        INSERT INTO stocks (symbol, name, sector, market_cap_category)
                        VALUES (%s, %s, %s, %s)
                        RETURNING *
                    """, (symbol, name, sector, market_cap_category))
                    
                    new_stock = cur.fetchone()
                    logger.info(f"Created new stock {symbol}")
                    return {'action': 'created', 'stock': dict(new_stock)}
                    
    except Exception as e:
        logger.error(f"Error creating/updating stock {symbol}: {e}")
        raise self.retry(exc=e, countdown=60)

@app.task(bind=True, retry_backoff=True, max_retries=3)
def save_stock_analysis(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Save complete stock analysis (analysis + recommendation)
    
    Expected format:
    {
        'symbol': 'MT',
        'analysis': {
            'current_price': 33.53,
            'mean_price': 30.84,
            'price_deviation_percent': 8.7,
            'z_score': 3.15,
            'rsi': 71.4,
            'bollinger_position': 1.03,
            'volatility_percent': 31.8,
            'volume_ratio': 0.9,
            'five_day_change_percent': 7.4,
            'hurst_exponent': 0.62,
            'strength_rating': 'Strong',
            'market_condition': 'OVERBOUGHT'
        },
        'recommendation': {
            'action': 'BUY_PUTS',
            'confidence_level': 'HIGH',
            'time_horizon': 'SHORT',
            'entry_criteria': [...],
            'exit_criteria': [...],
            'risk_factors': [...],
            'educational_notes': '...'
        }
    }
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get stock ID
                cur.execute("SELECT id FROM stocks WHERE symbol = %s", (analysis_data['symbol'],))
                stock = cur.fetchone()
                
                if not stock:
                    raise ValueError(f"Stock {analysis_data['symbol']} not found")
                
                stock_id = stock['id']
                analysis = analysis_data['analysis']
                
                # Insert analysis
                cur.execute("""
                    INSERT INTO stock_analyses (
                        stock_id, current_price, mean_price, price_deviation_percent,
                        z_score, rsi, bollinger_position, volatility_percent,
                        volume_ratio, five_day_change_percent, hurst_exponent,
                        strength_rating, market_condition
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) RETURNING id
                """, (
                    stock_id, analysis['current_price'], analysis['mean_price'],
                    analysis['price_deviation_percent'], analysis['z_score'],
                    analysis['rsi'], analysis['bollinger_position'],
                    analysis['volatility_percent'], analysis['volume_ratio'],
                    analysis['five_day_change_percent'], analysis['hurst_exponent'],
                    analysis['strength_rating'], analysis['market_condition']
                ))
                
                analysis_id = cur.fetchone()['id']
                
                # Insert recommendation if provided
                if 'recommendation' in analysis_data:
                    rec = analysis_data['recommendation']
                    cur.execute("""
                        INSERT INTO trading_recommendations (
                            analysis_id, action, confidence_level, time_horizon,
                            entry_criteria, exit_criteria, risk_factors, educational_notes
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        analysis_id, rec['action'], rec['confidence_level'],
                        rec['time_horizon'], json.dumps(rec.get('entry_criteria', [])),
                        json.dumps(rec.get('exit_criteria', [])),
                        json.dumps(rec.get('risk_factors', [])),
                        rec.get('educational_notes', '')
                    ))
                    
                    recommendation_id = cur.fetchone()['id']
                    
                    logger.info(f"Saved analysis and recommendation for {analysis_data['symbol']}")
                    return {
                        'analysis_id': analysis_id,
                        'recommendation_id': recommendation_id,
                        'symbol': analysis_data['symbol']
                    }
                else:
                    logger.info(f"Saved analysis for {analysis_data['symbol']}")
                    return {
                        'analysis_id': analysis_id,
                        'symbol': analysis_data['symbol']
                    }
                    
    except Exception as e:
        logger.error(f"Error saving analysis for {analysis_data.get('symbol', 'unknown')}: {e}")
        raise self.retry(exc=e, countdown=60)

@app.task(bind=True, retry_backoff=True, max_retries=3)
def bulk_save_analyses(self, analyses_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Save multiple stock analyses in bulk
    """
    results = []
    errors = []
    
    for analysis_data in analyses_data:
        try:
            result = save_stock_analysis.apply_async(args=[analysis_data])
            results.append({
                'symbol': analysis_data['symbol'],
                'task_id': result.id,
                'status': 'queued'
            })
        except Exception as e:
            errors.append({
                'symbol': analysis_data.get('symbol', 'unknown'),
                'error': str(e)
            })
    
    return {
        'queued': len(results),
        'errors': len(errors),
        'results': results,
        'error_details': errors
    }

# PULL TASKS (Reading from Database)

@app.task(bind=True, retry_backoff=True, max_retries=3)
def get_latest_analysis(self, symbol: str) -> Dict[str, Any]:
    """
    Get the latest analysis for a stock
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        s.symbol, s.name, s.sector, s.market_cap_category,
                        sa.*,
                        tr.action, tr.confidence_level, tr.time_horizon,
                        tr.entry_criteria, tr.exit_criteria, tr.risk_factors,
                        tr.educational_notes
                    FROM stocks s
                    JOIN stock_analyses sa ON s.id = sa.stock_id
                    LEFT JOIN trading_recommendations tr ON sa.id = tr.analysis_id
                    WHERE s.symbol = %s
                    ORDER BY sa.analysis_date DESC
                    LIMIT 1
                """, (symbol,))
                
                result = cur.fetchone()
                
                if result:
                    # Convert to dict and handle JSON fields
                    analysis = dict(result)
                    if analysis['entry_criteria']:
                        analysis['entry_criteria'] = json.loads(analysis['entry_criteria'])
                    if analysis['exit_criteria']:
                        analysis['exit_criteria'] = json.loads(analysis['exit_criteria'])
                    if analysis['risk_factors']:
                        analysis['risk_factors'] = json.loads(analysis['risk_factors'])
                    
                    return analysis
                else:
                    return {'error': f'No analysis found for {symbol}'}
                    
    except Exception as e:
        logger.error(f"Error getting latest analysis for {symbol}: {e}")
        raise self.retry(exc=e, countdown=60)

@app.task(bind=True, retry_backoff=True, max_retries=3)
def get_analyses_by_condition(self, market_condition: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get analyses by market condition (OVERBOUGHT, OVERSOLD, etc.)
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        s.symbol, s.name,
                        sa.current_price, sa.mean_price, sa.rsi, sa.z_score,
                        sa.market_condition, sa.strength_rating, sa.analysis_date,
                        tr.action, tr.confidence_level
                    FROM stocks s
                    JOIN stock_analyses sa ON s.id = sa.stock_id
                    LEFT JOIN trading_recommendations tr ON sa.id = tr.analysis_id
                    WHERE sa.market_condition = %s
                    ORDER BY sa.analysis_date DESC
                    LIMIT %s
                """, (market_condition, limit))
                
                results = cur.fetchall()
                return [dict(row) for row in results]
                
    except Exception as e:
        logger.error(f"Error getting analyses by condition {market_condition}: {e}")
        raise self.retry(exc=e, countdown=60)

@app.task(bind=True, retry_backoff=True, max_retries=3)
def get_high_confidence_recommendations(self, confidence_level: str = 'HIGH', 
                                      limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get high confidence recommendations
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        s.symbol, s.name,
                        sa.current_price, sa.z_score, sa.rsi, sa.market_condition,
                        tr.action, tr.confidence_level, tr.time_horizon,
                        tr.entry_criteria, tr.exit_criteria, tr.risk_factors,
                        sa.analysis_date
                    FROM trading_recommendations tr
                    JOIN stock_analyses sa ON tr.analysis_id = sa.id
                    JOIN stocks s ON sa.stock_id = s.id
                    WHERE tr.confidence_level = %s
                    ORDER BY sa.analysis_date DESC
                    LIMIT %s
                """, (confidence_level, limit))
                
                results = cur.fetchall()
                
                # Process JSON fields
                processed_results = []
                for row in results:
                    analysis = dict(row)
                    if analysis['entry_criteria']:
                        analysis['entry_criteria'] = json.loads(analysis['entry_criteria'])
                    if analysis['exit_criteria']:
                        analysis['exit_criteria'] = json.loads(analysis['exit_criteria'])
                    if analysis['risk_factors']:
                        analysis['risk_factors'] = json.loads(analysis['risk_factors'])
                    processed_results.append(analysis)
                
                return processed_results
                
    except Exception as e:
        logger.error(f"Error getting high confidence recommendations: {e}")
        raise self.retry(exc=e, countdown=60)

@app.task(bind=True, retry_backoff=True, max_retries=3)
def get_stock_history(self, symbol: str, days: int = 30) -> List[Dict[str, Any]]:
    """
    Get analysis history for a stock
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        sa.analysis_date, sa.current_price, sa.mean_price,
                        sa.z_score, sa.rsi, sa.market_condition, sa.strength_rating,
                        tr.action, tr.confidence_level
                    FROM stock_analyses sa
                    JOIN stocks s ON sa.stock_id = s.id
                    LEFT JOIN trading_recommendations tr ON sa.id = tr.analysis_id
                    WHERE s.symbol = %s 
                    AND sa.analysis_date >= %s
                    ORDER BY sa.analysis_date DESC
                """, (symbol, datetime.now() - timedelta(days=days)))
                
                results = cur.fetchall()
                return [dict(row) for row in results]
                
    except Exception as e:
        logger.error(f"Error getting stock history for {symbol}: {e}")
        raise self.retry(exc=e, countdown=60)

# UTILITY TASKS

@app.task(bind=True, retry_backoff=True, max_retries=3)
def cleanup_old_analyses(self, days_to_keep: int = 90) -> Dict[str, int]:
    """
    Clean up old analyses older than specified days
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Delete old recommendations first (due to foreign key)
                cur.execute("""
                    DELETE FROM trading_recommendations 
                    WHERE analysis_id IN (
                        SELECT id FROM stock_analyses 
                        WHERE analysis_date < %s
                    )
                """, (datetime.now() - timedelta(days=days_to_keep),))
                
                recommendations_deleted = cur.rowcount
                
                # Delete old analyses
                cur.execute("""
                    DELETE FROM stock_analyses 
                    WHERE analysis_date < %s
                """, (datetime.now() - timedelta(days=days_to_keep),))
                
                analyses_deleted = cur.rowcount
                
                logger.info(f"Cleaned up {analyses_deleted} analyses and {recommendations_deleted} recommendations")
                return {
                    'analyses_deleted': analyses_deleted,
                    'recommendations_deleted': recommendations_deleted
                }
                
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise self.retry(exc=e, countdown=60)

@app.task
def get_database_stats() -> Dict[str, Any]:
    """
    Get database statistics
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                stats = {}
                
                # Stock count
                cur.execute("SELECT COUNT(*) as count FROM stocks")
                stats['total_stocks'] = cur.fetchone()['count']
                
                # Analysis count
                cur.execute("SELECT COUNT(*) as count FROM stock_analyses")
                stats['total_analyses'] = cur.fetchone()['count']
                
                # Recommendation count
                cur.execute("SELECT COUNT(*) as count FROM trading_recommendations")
                stats['total_recommendations'] = cur.fetchone()['count']
                
                # Latest analysis date
                cur.execute("SELECT MAX(analysis_date) as latest FROM stock_analyses")
                stats['latest_analysis'] = cur.fetchone()['latest']
                
                # Market condition breakdown
                cur.execute("""
                    SELECT market_condition, COUNT(*) as count 
                    FROM stock_analyses 
                    WHERE analysis_date >= %s 
                    GROUP BY market_condition
                """, (datetime.now() - timedelta(days=7),))
                
                stats['recent_conditions'] = {row['market_condition']: row['count'] 
                                            for row in cur.fetchall()}
                
                return stats
                
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        raise

if __name__ == "__main__":
    app.start()