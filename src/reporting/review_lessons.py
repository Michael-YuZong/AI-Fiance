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
    ReviewLesson(
        lesson_id="L016",
        title="A股资本结构与执行层风险应优先使用 Tushare 结构化数据",
        scope="cn_stock_reports",
        owner_layer="opportunity_engine",
        rationale="解禁、质押、前十大股东、集合竞价、涨跌停边界这类信息如果 Tushare 已可稳定获取，就应优先用于硬检查、筹码结构和执行层，而不是继续用市场级代理或缺省占位。",
    ),
    ReviewLesson(
        lesson_id="L017",
        title="历史相似样本置信度必须说明样本边界并严控数据置信度",
        scope="stock_pick_reports",
        owner_layer="release_check",
        rationale="当前推荐如果要引用历史相似样本，就必须清楚说明是同标的还是跨标的、样本数、窗口、止损/目标口径，以及何时因样本不足或历史降级而拒绝给置信度。",
    ),
    ReviewLesson(
        lesson_id="L018",
        title="正式推荐的过线逻辑与边界案例必须对客户讲清楚",
        scope="stock_pick_reports",
        owner_layer="external_review",
        rationale="如果正式推荐依赖的是相对强弱替代催化、或某个边界阈值刚好踩线，成稿必须解释为什么这只票能进、另一只票为什么没进，避免读者只看到分数却看不懂最终推荐规则。",
    ),
    ReviewLesson(
        lesson_id="L019",
        title="历史相似样本只能作为辅助验证，不能冒充严格回测",
        scope="stock_pick_reports",
        owner_layer="external_review",
        rationale="只基于同标的日线量价/技术状态的相似样本统计，不重建历史新闻、财报和基本面环境，因此只能用于补充执行置信度，不能被包装成完整策略回测或确定性交易胜率。",
    ),
    ReviewLesson(
        lesson_id="L020",
        title="跨市场覆盖率不均时必须明确不同市场的参考强弱",
        scope="cross_market_reports",
        owner_layer="external_review",
        rationale="当 A 股、港股、美股的数据覆盖和高置信事件命中率差异明显时，成稿不能把三地推荐写成同等可靠；必须明确说明哪一市场证据更扎实，哪一市场更偏弱信息排序。",
    ),
    ReviewLesson(
        lesson_id="L021",
        title="同一事件若出现数值冲突，成稿必须统一口径或显式解释差异",
        scope="all_reports",
        owner_layer="external_review",
        rationale="同一份报告里如果对同一次分红、回购、配股等资本回报事件引用了不同总额或不同数字，必须说明是否是税前/税后、含特别分红、不同股本口径或媒体摘录冲突；否则应视为高优先级数据错误。",
    ),
    ReviewLesson(
        lesson_id="L022",
        title="结构化事件必须做新鲜度控制，陈旧事件不能按满额催化计分",
        scope="catalyst_logic",
        owner_layer="opportunity_engine",
        rationale="像分红预案、回购、旧公告这类结构化事件如果距离报告日已明显过久，就不能继续按当前催化满分处理，至少需要衰减或降级成背景信息。",
    ),
    ReviewLesson(
        lesson_id="L023",
        title="样本置信度必须和总推荐置信度分开命名",
        scope="stock_pick_reports",
        owner_layer="renderer",
        rationale="历史相似样本得出的只是样本层置信度，不能直接叫“当前置信度”，否则会和正式推荐层级冲突，让读者误解成整只票的总置信度。",
    ),
    ReviewLesson(
        lesson_id="L024",
        title="覆盖率与覆盖源分数必须说明分母和阈值",
        scope="stock_pick_reports",
        owner_layer="renderer",
        rationale="结构化事件覆盖率、高置信新闻覆盖率、覆盖源计分这些数字如果没有明确分母和阈值定义，看起来像精确指标，实际却不具备可解释性。",
    ),
    ReviewLesson(
        lesson_id="L025",
        title="结论文案必须拆开估值、质量和信息不足，不能混成一句模板话",
        scope="all_reports",
        owner_layer="renderer",
        rationale="像“估值偏高或财务安全边际不足”这类混合表述会把估值、财务质量、催化不足和信息覆盖不足揉成一团，读者无法知道真正卡点是什么。",
    ),
    ReviewLesson(
        lesson_id="L026",
        title="相关性/分散度基准映射必须可解释",
        scope="cross_market_reports",
        owner_layer="external_review",
        rationale="不同市场、不同主题如果使用不同的相关性基准，成稿必须明确这是按市场/主题映射的结果，否则横向比较容易被误读成同一口径下的可比分数。",
    ),
    ReviewLesson(
        lesson_id="L027",
        title="中期宏观判断必须拆开景气、价格与信用指标角色",
        scope="macro_sensitive_reports",
        owner_layer="external_review",
        rationale="如果报告用 PMI、PPI、CPI、社融、M1-M2 去支撑未来 3-6 个月判断，就必须解释它们分别对应景气、价格链条和信用脉冲，而不是把它们都混成一句“宏观偏暖/偏冷”。",
    ),
    ReviewLesson(
        lesson_id="L028",
        title="商品/期货 ETF 必须显式披露展期与期限结构风险",
        scope="commodity_etf_reports",
        owner_layer="external_review",
        rationale="商品/期货 ETF 不是现货，也不是股票主题 ETF。报告如果只写油价、地缘和资金面，而不写展期损益、期限结构、保证金/备付结构和跟踪误差，就会高估其可执行性。",
    ),
    ReviewLesson(
        lesson_id="L029",
        title="客户稿不得暴露原始异常字符串",
        scope="all_reports",
        owner_layer="release_check",
        rationale="像 Too Many Requests、ProxyError、Traceback 这类原始异常属于系统细节，终稿应改写成客户可理解的数据限制说明，不能直接暴露报错串。",
    ),
    ReviewLesson(
        lesson_id="L030",
        title="历史样本验证必须披露非重叠样本、置信区间和样本质量",
        scope="stock_reports",
        owner_layer="release_check",
        rationale="如果成稿引用历史相似样本来支撑当前建议，就不能只给一个胜率或样本分；必须说明严格去重后的非重叠样本数、置信区间以及样本质量，否则读者容易把脆弱统计误读成高置信结论。",
    ),
    ReviewLesson(
        lesson_id="L031",
        title="Pick 覆盖率分母必须对应完整分析样本",
        scope="pick_reports",
        owner_layer="release_check",
        rationale="ETF/基金 pick 稿里披露的覆盖率分母，必须对应“进入完整分析的样本”而不是 top 榜单、展示子集或别的截断集合，否则覆盖率说明会失真。",
    ),
    ReviewLesson(
        lesson_id="L032",
        title="单候选说明不能擅自改写交付等级",
        scope="pick_reports",
        owner_layer="release_check",
        rationale="如果交付等级仍是标准推荐稿，单候选说明不能再把同一份报告写成观察优先或降级稿；候选数量说明和交付等级必须一致。",
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
