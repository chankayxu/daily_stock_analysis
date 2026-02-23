# -*- coding: utf-8 -*-
"""
===================================
股票智能分析系统 - 大盘复盘模块（支持 A股 / 港股 / 美股）
===================================

职责：
1. 根据 MARKET_REVIEW_REGION 配置选择市场区域（cn / hk / us / both / all）
2. 执行大盘复盘分析并生成复盘报告
3. 保存和发送复盘报告
"""

import logging
from datetime import datetime
from typing import Optional

from src.config import get_config
from src.notification import NotificationService
from src.market_analyzer import MarketAnalyzer
from src.search_service import SearchService
from src.analyzer import GeminiAnalyzer


logger = logging.getLogger(__name__)


def run_market_review(
    notifier: NotificationService,
    analyzer: Optional[GeminiAnalyzer] = None,
    search_service: Optional[SearchService] = None,
    send_notification: bool = True,
    merge_notification: bool = False
) -> Optional[str]:
    """
    执行大盘复盘分析

    Args:
        notifier: 通知服务
        analyzer: AI分析器（可选）
        search_service: 搜索服务（可选）
        send_notification: 是否发送通知
        merge_notification: 是否合并推送（跳过本次推送，由 main 层合并个股+大盘后统一发送，Issue #190）

    Returns:
        复盘报告文本
    """
    logger.info("开始执行大盘复盘分析...")
    config = get_config()
    region = getattr(config, 'market_review_region', 'cn') or 'cn'
    
    # 支持的区域配置：cn(A股), hk(港股), us(美股), both(A股+美股), all(A股+港股+美股)
    if region not in ('cn', 'hk', 'us', 'both', 'all'):
        region = 'hk'

    try:
        if region in ('both', 'all'):
            # 确定需要执行的市场列表
            if region == 'both':
                regions_to_run = ['cn', 'us']
            else:
                regions_to_run = ['cn', 'hk', 'us']
                
            region_names = {'cn': 'A股', 'hk': '港股', 'us': '美股'}
            reports = []
            
            # 顺序执行各市场复盘
            for r in regions_to_run:
                logger.info(f"生成 {region_names[r]} 大盘复盘报告...")
                market_analyzer = MarketAnalyzer(
                    search_service=search_service, analyzer=analyzer, region=r
                )
                report = market_analyzer.run_daily_review()
                if report:
                    reports.append(f"# {region_names[r]}大盘复盘\n\n{report}")
            
            # 合并多市场报告
            if reports:
                review_report = "\n\n---\n\n> 以下为其他市场复盘\n\n".join(reports)
            else:
                review_report = None
                
        else:
            # 单一市场复盘
            market_analyzer = MarketAnalyzer(
                search_service=search_service,
                analyzer=analyzer,
                region=region,
            )
            review_report = market_analyzer.run_daily_review()
        
        if review_report:
            # 保存报告到文件
            date_str = datetime.now().strftime('%Y%m%d')
            report_filename = f"market_review_{date_str}.md"
            filepath = notifier.save_report_to_file(
                f"# 🎯 大盘复盘\n\n{review_report}", 
                report_filename
            )
            logger.info(f"大盘复盘报告已保存: {filepath}")
            
            # 推送通知（合并模式下跳过，由 main 层统一发送）
            if merge_notification and send_notification:
                logger.info("合并推送模式：跳过大盘复盘单独推送，将在个股+大盘复盘后统一发送")
            elif send_notification and notifier.is_available():
                # 添加标题
                report_content = f"🎯 大盘复盘\n\n{review_report}"

                success = notifier.send(report_content, email_send_to_all=True)
                if success:
                    logger.info("大盘复盘推送成功")
                else:
                    logger.warning("大盘复盘推送失败")
            elif not send_notification:
                logger.info("已跳过推送通知 (--no-notify)")
            
            return review_report
        
    except Exception as e:
        logger.error(f"大盘复盘分析失败: {e}")
    
    return None
