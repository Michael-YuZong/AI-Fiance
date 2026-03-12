"""Persistent lessons learned from external financial reviews."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List


@dataclass(frozen=True)
class ReviewLesson:
    lesson_id: str
    title: str
    scope: str
    owner_layer: str
    rationale: str


ACTIVE_REVIEW_LESSONS: List[ReviewLesson] = [
    ReviewLesson(
        lesson_id="L001",
        title="终稿不得暴露内部过程",
        scope="all_reports",
        owner_layer="release_check",
        rationale="客户稿不能出现模型版本、外部复核、修稿记录等内部流程词。",
    ),
    ReviewLesson(
        lesson_id="L002",
        title="终稿必须是解释版",
        scope="all_reports",
        owner_layer="release_check",
        rationale="不能只给结论和分数，必须解释为什么这么判断、为什么不是别的标的。",
    ),
    ReviewLesson(
        lesson_id="L003",
        title="重复模板理由视为解释不合格",
        scope="all_reports",
        owner_layer="release_check",
        rationale="不同标的反复复用同一句理由，会让成稿看起来像模板而不是分析。",
    ),
    ReviewLesson(
        lesson_id="L004",
        title="盘中/竞价语言必须有执行层证据",
        scope="all_reports",
        owner_layer="release_check",
        rationale="没有 VWAP、开盘缺口、首30分钟或竞价证据时，不能把日线结论写成盘中/竞价判断。",
    ),
    ReviewLesson(
        lesson_id="L005",
        title="基金画像缺失不允许过稿",
        scope="fund_and_etf_reports",
        owner_layer="release_check",
        rationale="基金类型、基金公司、基金经理、成立日期、基准这类基础画像字段缺失会直接伤害可信度。",
    ),
    ReviewLesson(
        lesson_id="L006",
        title="商品/期货 ETF 不能套股票估值框架",
        scope="etf_and_fund_logic",
        owner_layer="opportunity_engine",
        rationale="商品/期货 ETF 要按产品结构、跟踪标的和容量分析，不能拿股票 PE、北向和成分股集中度硬套。",
    ),
    ReviewLesson(
        lesson_id="L007",
        title="ADX 不能脱离 DI 方向解释",
        scope="technical_logic",
        owner_layer="opportunity_engine",
        rationale="强趋势只有在 DI+ 优于 DI- 时才应当被当作做多加分项。",
    ),
    ReviewLesson(
        lesson_id="L008",
        title="ETF/基金前瞻催化不能混入无关公司财报",
        scope="catalyst_logic",
        owner_layer="opportunity_engine",
        rationale="非个股产品不能把任意披露类新闻当成自己的前瞻事件。",
    ),
    ReviewLesson(
        lesson_id="L009",
        title="final 导出前必须有独立外审 PASS",
        scope="all_reports",
        owner_layer="report_guard",
        rationale="未经过独立外审并明确 PASS 的 Markdown 不能进入 final。",
    ),
    ReviewLesson(
        lesson_id="L010",
        title="催化面必须结构化事件优先，没新闻不等于没催化",
        scope="catalyst_logic",
        owner_layer="opportunity_engine",
        rationale="财报日历、交易所公告、公司 IR、个股公告应优先于泛新闻；未命中高质量新闻时要区分信息不足和真正没有催化。",
    ),
    ReviewLesson(
        lesson_id="L011",
        title="新闻/事件源降级不能把推荐系统打成假阴性",
        scope="catalyst_logic",
        owner_layer="opportunity_engine",
        rationale="当实时新闻源或事件源降级时，催化维度不能简单坍塌为 0；至少要区分结构化事件、最近有效催化、信息覆盖率和真正没有催化，避免因为数据源抽风导致名单大幅缩水。",
    ),
    ReviewLesson(
        lesson_id="L012",
        title="市场级筹码数据不能伪装成个股级筹码优势",
        scope="chips_logic",
        owner_layer="opportunity_engine",
        rationale="如果筹码结构使用的是市场级北向/行业级资金数据，就不能把同一个值写成多只股票的个股级优势；要么降权，要么明确披露是市场级代理。",
    ),
    ReviewLesson(
        lesson_id="L013",
        title="合并稿必须披露数据完整度",
        scope="stock_pick_reports",
        owner_layer="release_check",
        rationale="全市场或跨市场合并稿必须告诉读者新闻/事件覆盖率和降级范围，否则容易把覆盖不足误读成真的没有机会。",
    ),
    ReviewLesson(
        lesson_id="L014",
        title="催化证据要能在成稿中直接复核",
        scope="stock_pick_reports",
        owner_layer="renderer",
        rationale="如果催化分显著影响推荐，就不能只给“头条数量/覆盖源数量”，还应在成稿里给出关键催化的原始来源或链接。",
    ),
    ReviewLesson(
        lesson_id="L015",
        title="A股结构化公司事件应优先使用 Tushare 而不是新闻标题猜测",
        scope="cn_stock_reports",
        owner_layer="opportunity_engine",
        rationale="A股已有披露计划、增减持、回购、分红、解禁等结构化接口时，应优先用这些高置信事件构建催化和风险，而不是仅靠新闻标题推断。",
    ),
]


LESSON_MAP: Dict[str, ReviewLesson] = {item.lesson_id: item for item in ACTIVE_REVIEW_LESSONS}


def lesson_prefix(lesson_id: str) -> str:
    lesson = LESSON_MAP.get(lesson_id)
    if not lesson:
        return "[L000]"
    return f"[{lesson.lesson_id}:{lesson.title}]"


def format_lesson_finding(lesson_id: str, message: str) -> str:
    return f"{lesson_prefix(lesson_id)} {message}"


def active_lesson_ids() -> List[str]:
    return [item.lesson_id for item in ACTIVE_REVIEW_LESSONS]


def describe_lessons() -> Iterable[ReviewLesson]:
    return tuple(ACTIVE_REVIEW_LESSONS)
