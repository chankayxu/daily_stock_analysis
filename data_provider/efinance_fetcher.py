# -*- coding: utf-8 -*-
"""
===================================
EfinanceFetcher - 优先数据源 (Priority 0)
===================================

数据来源：东方财富爬虫（通过 efinance 库）
特点：免费、无需 Token、数据全面、API 简洁
仓库：https://github.com/Micro-sheep/efinance

与 AkshareFetcher 类似，但 efinance 库：
1. API 更简洁易用
2. 支持批量获取数据
3. 更稳定的接口封装

防封禁策略：
1. 每次请求前随机休眠 1.5-3.0 秒
2. 随机轮换 User-Agent
3. 使用 tenacity 实现指数退避重试
4. 熔断器机制：连续失败后自动冷却
"""

import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import requests  # 引入 requests 以捕获异常
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from patch.eastmoney_patch import eastmoney_patch
from src.config import get_config
from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS
from .realtime_types import (
    UnifiedRealtimeQuote, RealtimeSource,
    get_realtime_circuit_breaker,
    safe_float, safe_int  # 使用统一的类型转换函数
)


# 保留旧的类型别名，用于向后兼容
@dataclass
class EfinanceRealtimeQuote:
    """
    实时行情数据（来自 efinance）- 向后兼容别名
    
    新代码建议使用 UnifiedRealtimeQuote
    """
    code: str
    name: str = ""
    price: float = 0.0           # 最新价
    change_pct: float = 0.0      # 涨跌幅(%)
    change_amount: float = 0.0   # 涨跌额
    
    # 量价指标
    volume: int = 0              # 成交量
    amount: float = 0.0          # 成交额
    turnover_rate: float = 0.0   # 换手率(%)
    amplitude: float = 0.0       # 振幅(%)
    
    # 价格区间
    high: float = 0.0            # 最高价
    low: float = 0.0             # 最低价
    open_price: float = 0.0      # 开盘价
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'name': self.name,
            'price': self.price,
            'change_pct': self.change_pct,
            'change_amount': self.change_amount,
            'volume': self.volume,
            'amount': self.amount,
            'turnover_rate': self.turnover_rate,
            'amplitude': self.amplitude,
            'high': self.high,
            'low': self.low,
            'open': self.open_price,
        }


logger = logging.getLogger(__name__)


# User-Agent 池，用于随机轮换
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]


# 缓存实时行情数据（避免重复请求）
# TTL 设为 10 分钟 (600秒)：批量分析场景下避免重复拉取
_realtime_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 600,  # 10分钟缓存有效期
    'region': 'cn' # 记录缓存对应的市场区域
}

# ETF 实时行情缓存（与股票分开缓存）
_etf_realtime_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 600  # 10分钟缓存有效期
}


def _is_etf_code(stock_code: str) -> bool:
    """
    判断代码是否为 ETF 基金
    
    ETF 代码规则：
    - 上交所 ETF: 51xxxx, 52xxxx, 56xxxx, 58xxxx
    - 深交所 ETF: 15xxxx, 16xxxx, 18xxxx
    """
    etf_prefixes = ('51', '52', '56', '58', '15', '16', '18')
    return stock_code.startswith(etf_prefixes) and len(stock_code) == 6


def _is_us_code(stock_code: str) -> bool:
    """
    判断代码是否为美股
    """
    code = stock_code.strip().upper()
    return bool(re.match(r'^[A-Z]{1,5}(\.[A-Z])?$', code))


class EfinanceFetcher(BaseFetcher):
    """
    Efinance 数据源实现
    
    优先级：0（最高，优先于 AkshareFetcher）
    数据来源：东方财富网（通过 efinance 库封装）
    仓库：https://github.com/Micro-sheep/efinance
    """
    
    name = "EfinanceFetcher"
    priority = int(os.getenv("EFINANCE_PRIORITY", "0"))
    
    def __init__(self, sleep_min: float = 1.5, sleep_max: float = 3.0):
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self._last_request_time: Optional[float] = None
        if get_config().enable_eastmoney_patch:
            eastmoney_patch()
    
    def _set_random_user_agent(self) -> None:
        try:
            random_ua = random.choice(USER_AGENTS)
            logger.debug(f"设置 User-Agent: {random_ua[:50]}...")
        except Exception as e:
            logger.debug(f"设置 User-Agent 失败: {e}")
    
    def _enforce_rate_limit(self) -> None:
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            min_interval = self.sleep_min
            if elapsed < min_interval:
                additional_sleep = min_interval - elapsed
                logger.debug(f"补充休眠 {additional_sleep:.2f} 秒")
                time.sleep(additional_sleep)
        
        self.random_sleep(self.sleep_min, self.sleep_max)
        self._last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(1),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((
            ConnectionError,
            TimeoutError,
            requests.exceptions.RequestException,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        if _is_us_code(stock_code):
            raise DataFetchError(f"EfinanceFetcher 不支持美股 {stock_code}，请使用 AkshareFetcher 或 YfinanceFetcher")
        
        if _is_etf_code(stock_code):
            return self._fetch_etf_data(stock_code, start_date, end_date)
        else:
            return self._fetch_stock_data(stock_code, start_date, end_date)
    
    def _fetch_stock_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        import efinance as ef
        self._set_random_user_agent()
        self._enforce_rate_limit()
        
        beg_date = start_date.replace('-', '')
        end_date_fmt = end_date.replace('-', '')
        
        logger.info(f"[API调用] ef.stock.get_quote_history(stock_codes={stock_code}, "
                   f"beg={beg_date}, end={end_date_fmt}, klt=101, fqt=1)")
        
        try:
            import time as _time
            api_start = _time.time()
            df = ef.stock.get_quote_history(
                stock_codes=stock_code,
                beg=beg_date,
                end=end_date_fmt,
                klt=101,
                fqt=1
            )
            api_elapsed = _time.time() - api_start
            
            if df is not None and not df.empty:
                logger.info(f"[API返回] ef.stock.get_quote_history 成功: 返回 {len(df)} 行数据, 耗时 {api_elapsed:.2f}s")
            else:
                logger.warning(f"[API返回] ef.stock.get_quote_history 返回空数据, 耗时 {api_elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['banned', 'blocked', '频率', 'rate', '限制']):
                raise RateLimitError(f"efinance 可能被限流: {e}") from e
            raise DataFetchError(f"efinance 获取数据失败: {e}") from e
    
    def _fetch_etf_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        import efinance as ef
        self._set_random_user_agent()
        self._enforce_rate_limit()
        
        logger.info(f"[API调用] ef.fund.get_quote_history(fund_code={stock_code})")
        
        try:
            import time as _time
            api_start = _time.time()
            df = ef.fund.get_quote_history(fund_code=stock_code)
            
            if df is not None and not df.empty and '日期' in df.columns:
                mask = (df['日期'] >= start_date) & (df['日期'] <= end_date)
                df = df[mask].copy()
            
            api_elapsed = _time.time() - api_start
            
            if df is not None and not df.empty:
                logger.info(f"[API返回] ef.fund.get_quote_history 成功: 返回 {len(df)} 行数据, 耗时 {api_elapsed:.2f}s")
            else:
                logger.warning(f"[API返回] ef.fund.get_quote_history 返回空数据, 耗时 {api_elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['banned', 'blocked', '频率', 'rate', '限制']):
                raise RateLimitError(f"efinance 可能被限流: {e}") from e
            raise DataFetchError(f"efinance 获取 ETF 数据失败: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        df = df.copy()
        column_mapping = {
            '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low',
            '成交量': 'volume', '成交额': 'amount', '涨跌幅': 'pct_chg',
            '股票代码': 'code', '股票名称': 'name', '基金代码': 'code', '基金名称': 'name', '单位净值': 'close',
        }
        df = df.rename(columns=column_mapping)
        
        if 'close' in df.columns and 'open' not in df.columns:
            df['open'] = df['close']
            df['high'] = df['close']
            df['low'] = df['close']
            
        if 'volume' not in df.columns: df['volume'] = 0
        if 'amount' not in df.columns: df['amount'] = 0
        if 'code' not in df.columns: df['code'] = stock_code
        
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        return df
    
    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        if _is_etf_code(stock_code):
            return self._get_etf_realtime_quote(stock_code)

        import efinance as ef
        circuit_breaker = get_realtime_circuit_breaker()
        source_key = "efinance"
        
        if not circuit_breaker.is_available(source_key):
            return None
        
        try:
            current_time = time.time()
            if (_realtime_cache['data'] is not None and 
                current_time - _realtime_cache['timestamp'] < _realtime_cache['ttl']):
                df = _realtime_cache['data']
            else:
                self._set_random_user_agent()
                self._enforce_rate_limit()
                
                logger.info(f"[API调用] ef.stock.get_realtime_quotes() 获取实时行情...")
                df = ef.stock.get_realtime_quotes()
                circuit_breaker.record_success(source_key)
                
                _realtime_cache['data'] = df
                _realtime_cache['timestamp'] = current_time
                _realtime_cache['region'] = 'cn'
            
            code_col = '股票代码' if '股票代码' in df.columns else 'code'
            row = df[df[code_col] == stock_code]
            if row.empty:
                return None
            
            row = row.iloc[0]
            
            name_col = '股票名称' if '股票名称' in df.columns else 'name'
            price_col = '最新价' if '最新价' in df.columns else 'price'
            pct_col = '涨跌幅' if '涨跌幅' in df.columns else 'pct_chg'
            chg_col = '涨跌额' if '涨跌额' in df.columns else 'change'
            vol_col = '成交量' if '成交量' in df.columns else 'volume'
            amt_col = '成交额' if '成交额' in df.columns else 'amount'
            turn_col = '换手率' if '换手率' in df.columns else 'turnover_rate'
            amp_col = '振幅' if '振幅' in df.columns else 'amplitude'
            high_col = '最高' if '最高' in df.columns else 'high'
            low_col = '最低' if '最低' in df.columns else 'low'
            open_col = '开盘' if '开盘' in df.columns else 'open'
            vol_ratio_col = '量比' if '量比' in df.columns else 'volume_ratio'
            pe_col = '市盈率' if '市盈率' in df.columns else 'pe_ratio'
            total_mv_col = '总市值' if '总市值' in df.columns else 'total_mv'
            circ_mv_col = '流通市值' if '流通市值' in df.columns else 'circ_mv'
            
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=str(row.get(name_col, '')),
                source=RealtimeSource.EFINANCE,
                price=safe_float(row.get(price_col)),
                change_pct=safe_float(row.get(pct_col)),
                change_amount=safe_float(row.get(chg_col)),
                volume=safe_int(row.get(vol_col)),
                amount=safe_float(row.get(amt_col)),
                turnover_rate=safe_float(row.get(turn_col)),
                amplitude=safe_float(row.get(amp_col)),
                high=safe_float(row.get(high_col)),
                low=safe_float(row.get(low_col)),
                open_price=safe_float(row.get(open_col)),
                volume_ratio=safe_float(row.get(vol_ratio_col)),
                pe_ratio=safe_float(row.get(pe_col)),
                total_mv=safe_float(row.get(total_mv_col)),
                circ_mv=safe_float(row.get(circ_mv_col)),
            )
            return quote
            
        except Exception as e:
            logger.error(f"[API错误] 获取 {stock_code} 实时行情(efinance)失败: {e}")
            circuit_breaker.record_failure(source_key, str(e))
            return None

    def _get_etf_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        import efinance as ef
        circuit_breaker = get_realtime_circuit_breaker()
        source_key = "efinance_etf"

        if not circuit_breaker.is_available(source_key):
            return None

        try:
            current_time = time.time()
            if (_etf_realtime_cache['data'] is not None and
                current_time - _etf_realtime_cache['timestamp'] < _etf_realtime_cache['ttl']):
                df = _etf_realtime_cache['data']
            else:
                self._set_random_user_agent()
                self._enforce_rate_limit()

                df = ef.stock.get_realtime_quotes(['ETF'])
                circuit_breaker.record_success(source_key)
                _etf_realtime_cache['data'] = df
                _etf_realtime_cache['timestamp'] = current_time

            if df is None or df.empty:
                return None

            code_col = '股票代码' if '股票代码' in df.columns else 'code'
            code_series = df[code_col].astype(str).str.zfill(6)
            target_code = str(stock_code).strip().zfill(6)
            row = df[code_series == target_code]
            if row.empty:
                return None

            row = row.iloc[0]
            name_col = '股票名称' if '股票名称' in df.columns else 'name'
            price_col = '最新价' if '最新价' in df.columns else 'price'
            pct_col = '涨跌幅' if '涨跌幅' in df.columns else 'pct_chg'
            chg_col = '涨跌额' if '涨跌额' in df.columns else 'change'
            vol_col = '成交量' if '成交量' in df.columns else 'volume'
            amt_col = '成交额' if '成交额' in df.columns else 'amount'
            turn_col = '换手率' if '换手率' in df.columns else 'turnover_rate'
            amp_col = '振幅' if '振幅' in df.columns else 'amplitude'
            high_col = '最高' if '最高' in df.columns else 'high'
            low_col = '最低' if '最低' in df.columns else 'low'
            open_col = '开盘' if '开盘' in df.columns else 'open'

            quote = UnifiedRealtimeQuote(
                code=target_code,
                name=str(row.get(name_col, '')),
                source=RealtimeSource.EFINANCE,
                price=safe_float(row.get(price_col)),
                change_pct=safe_float(row.get(pct_col)),
                change_amount=safe_float(row.get(chg_col)),
                volume=safe_int(row.get(vol_col)),
                amount=safe_float(row.get(amt_col)),
                turnover_rate=safe_float(row.get(turn_col)),
                amplitude=safe_float(row.get(amp_col)),
                high=safe_float(row.get(high_col)),
                low=safe_float(row.get(low_col)),
                open_price=safe_float(row.get(open_col)),
            )
            return quote
        except Exception as e:
            circuit_breaker.record_failure(source_key, str(e))
            return None

    def get_main_indices(self, region: str = "cn") -> Optional[List[Dict[str, Any]]]:
        """
        获取主要指数实时行情 (efinance)
        """
        if region == "hk":
            # efinance 官方接口对港股指数批量获取支持有限，返回 None 交给 AkshareFetcher 处理
            logger.debug("[efinance] 暂不支持批量获取港股指数，降级交给其他数据源")
            return None
            
        import efinance as ef
        indices_map = {
            '000001': ('上证指数', 'sh000001'),
            '399001': ('深证成指', 'sz399001'),
            '399006': ('创业板指', 'sz399006'),
            '000688': ('科创50', 'sh000688'),
            '000016': ('上证50', 'sh000016'),
            '000300': ('沪深300', 'sh000300'),
        }

        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            df = ef.stock.get_realtime_quotes(['沪深系列指数'])
            if df is None or df.empty:
                return None

            code_col = '股票代码' if '股票代码' in df.columns else 'code'
            code_series = df[code_col].astype(str).str.zfill(6)

            results: List[Dict[str, Any]] = []
            for code, (name, full_code) in indices_map.items():
                row = df[code_series == code]
                if row.empty:
                    continue
                item = row.iloc[0]

                price_col = '最新价' if '最新价' in df.columns else 'price'
                pct_col = '涨跌幅' if '涨跌幅' in df.columns else 'pct_chg'
                chg_col = '涨跌额' if '涨跌额' in df.columns else 'change'
                open_col = '开盘' if '开盘' in df.columns else 'open'
                high_col = '最高' if '最高' in df.columns else 'high'
                low_col = '最低' if '最低' in df.columns else 'low'
                vol_col = '成交量' if '成交量' in df.columns else 'volume'
                amt_col = '成交额' if '成交额' in df.columns else 'amount'
                amp_col = '振幅' if '振幅' in df.columns else 'amplitude'

                current = safe_float(item.get(price_col, 0))
                change_amount = safe_float(item.get(chg_col, 0))

                results.append({
                    'code': full_code,
                    'name': name,
                    'current': current,
                    'change': change_amount,
                    'change_pct': safe_float(item.get(pct_col, 0)),
                    'open': safe_float(item.get(open_col, 0)),
                    'high': safe_float(item.get(high_col, 0)),
                    'low': safe_float(item.get(low_col, 0)),
                    'prev_close': current - change_amount if current or change_amount else 0,
                    'volume': safe_float(item.get(vol_col, 0)),
                    'amount': safe_float(item.get(amt_col, 0)),
                    'amplitude': safe_float(item.get(amp_col, 0)),
                })

            return results if results else None
        except Exception as e:
            logger.error(f"[efinance] 获取指数行情失败: {e}")
            return None

    def get_market_stats(self, region: str = "cn") -> Optional[Dict[str, Any]]:
        """
        获取市场涨跌统计 (efinance)
        支持 A股(cn) 和 港股(hk)
        """
        import efinance as ef

        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            current_time = time.time()
            # 检查缓存是否有效且对应的 region 匹配
            if (
                _realtime_cache['data'] is not None and
                current_time - _realtime_cache['timestamp'] < _realtime_cache['ttl'] and
                _realtime_cache.get('region') == region
            ):
                df = _realtime_cache['data']
            else:
                if region == "hk":
                    logger.info("[API调用] ef.stock.get_realtime_quotes(['港股']) 获取港股市场统计...")
                    df = ef.stock.get_realtime_quotes(['港股'])
                else:
                    logger.info("[API调用] ef.stock.get_realtime_quotes() 获取A股市场统计...")
                    df = ef.stock.get_realtime_quotes()
                    
                _realtime_cache['data'] = df
                _realtime_cache['timestamp'] = current_time
                _realtime_cache['region'] = region

            if df is None or df.empty:
                logger.warning(f"[API返回] {region} 市场统计数据为空")
                return None

            change_col = '涨跌幅' if '涨跌幅' in df.columns else 'pct_chg'
            amount_col = '成交额' if '成交额' in df.columns else 'amount'
            if change_col not in df.columns:
                return None

            df[change_col] = pd.to_numeric(df[change_col], errors='coerce')
            stats = {
                'up_count': len(df[df[change_col] > 0]),
                'down_count': len(df[df[change_col] < 0]),
                'flat_count': len(df[df[change_col] == 0]),
                'limit_up_count': len(df[df[change_col] >= 9.9]) if region == "cn" else 0, # 港股无涨跌幅限制，暂设为0
                'limit_down_count': len(df[df[change_col] <= -9.9]) if region == "cn" else 0,
                'total_amount': 0.0,
            }
            if amount_col in df.columns:
                df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce')
                # 港股成交额单位也是元，转换为亿
                stats['total_amount'] = df[amount_col].sum() / 1e8
            return stats
        except Exception as e:
            logger.error(f"[efinance] 获取 {region} 市场统计失败: {e}")
            return None

    def get_sector_rankings(self, n: int = 5, region: str = "cn") -> Optional[Tuple[List[Dict], List[Dict]]]:
        """
        获取板块涨跌榜 (efinance)
        """
        if region == "hk":
            # efinance 暂未提供直接的港股板块排行接口，返回 None 交给其他数据源处理
            logger.debug("[efinance] 暂不支持港股板块排行，降级交给其他数据源")
            return None
            
        import efinance as ef

        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            logger.info("[API调用] ef.stock.get_realtime_quotes(['行业板块']) 获取板块行情...")
            df = ef.stock.get_realtime_quotes(['行业板块'])
            if df is None or df.empty:
                logger.warning("[efinance] 板块行情数据为空")
                return None

            change_col = '涨跌幅' if '涨跌幅' in df.columns else 'pct_chg'
            name_col = '股票名称' if '股票名称' in df.columns else 'name'
            if change_col not in df.columns or name_col not in df.columns:
                return None

            df[change_col] = pd.to_numeric(df[change_col], errors='coerce')
            df = df.dropna(subset=[change_col])
            top = df.nlargest(n, change_col)
            bottom = df.nsmallest(n, change_col)

            top_sectors = [
                {'name': str(row[name_col]), 'change_pct': float(row[change_col])}
                for _, row in top.iterrows()
            ]
            bottom_sectors = [
                {'name': str(row[name_col]), 'change_pct': float(row[change_col])}
                for _, row in bottom.iterrows()
            ]
            return top_sectors, bottom_sectors
        except Exception as e:
            logger.error(f"[efinance] 获取板块排行失败: {e}")
            return None
    
    def get_base_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        import efinance as ef
        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            info = ef.stock.get_base_info(stock_code)
            if info is None:
                return None
            
            if isinstance(info, pd.Series):
                return info.to_dict()
            elif isinstance(info, pd.DataFrame):
                if not info.empty:
                    return info.iloc[0].to_dict()
            return None
        except Exception as e:
            logger.error(f"[API错误] 获取 {stock_code} 基本信息失败: {e}")
            return None
    
    def get_belong_board(self, stock_code: str) -> Optional[pd.DataFrame]:
        import efinance as ef
        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            df = ef.stock.get_belong_board(stock_code)
            if df is not None and not df.empty:
                return df
            return None
        except Exception as e:
            logger.error(f"[API错误] 获取 {stock_code} 所属板块失败: {e}")
            return None
    
    def get_enhanced_data(self, stock_code: str, days: int = 60) -> Dict[str, Any]:
        result = {
            'code': stock_code,
            'daily_data': None,
            'realtime_quote': None,
            'base_info': None,
            'belong_board': None,
        }
        try:
            df = self.get_daily_data(stock_code, days=days)
            result['daily_data'] = df
        except Exception as e:
            logger.error(f"获取 {stock_code} 日线数据失败: {e}")
        
        result['realtime_quote'] = self.get_realtime_quote(stock_code)
        result['base_info'] = self.get_base_info(stock_code)
        result['belong_board'] = self.get_belong_board(stock_code)
        return result
