"""Factor metadata contract and state machine.

All new factors must carry this minimum metadata before entering the product layer.
Shared by scan / pick / decision_review / strategy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections import Counter
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional


FactorState = Literal[
    "observation_only",    # 只作为提示或观察线索，不进入主评分
    "scoring_supportive",  # 可以进入产品评分，但还不是高权重主因子
    "production_factor",   # 已进入主评分、叙事和动作链
    "strategy_challenger", # 已满足 point-in-time / lag 条件，允许进入 strategy 候选池验证
    "champion_candidate",  # 已通过产品层与研究层验证，允许进入后续 promotion 讨论
]

VisibilityClass = Literal[
    "realtime",    # 实时可见，无 lag
    "daily_close", # 收盘后可见，T+0 收盘
    "t1",          # T+1 可见
    "t_n",         # T+N 可见（N > 1）
    "quarterly",   # 季度披露
    "event_driven",# 事件驱动，不定期
    "proxy",       # 代理信号，非直接可见
]

SourceType = Literal[
    "price_volume",   # 价量数据
    "fundamental",    # 基本面财务数据
    "macro",          # 宏观数据
    "calendar",       # 日历/事件窗
    "breadth",        # 市场宽度
    "flow",           # 资金流
    "sentiment",      # 情绪/热度
    "etf_specific",   # ETF 专属
    "fund_specific",  # 基金专属
    "proxy",          # 代理信号
]

ProxyLevel = Literal[
    "direct",         # 直接信号，无代理
    "sector_proxy",   # 行业级代理
    "market_proxy",   # 市场级代理
    "rule_based",     # 规则化代理（如日历规则）
    "model_proxy",    # 模型代理
]


@dataclass
class FactorMeta:
    """Minimum metadata contract for all factors entering the product layer."""

    factor_id: str
    family: str  # J-1, J-2, J-3, J-4, J-5
    source_type: SourceType
    visibility_class: VisibilityClass
    proxy_level: ProxyLevel
    state: FactorState
    supports_scoring: bool
    supports_strategy_candidate: bool

    # Optional fields
    source_as_of: Optional[str] = None  # ISO date string of data as-of
    degraded: bool = False
    degraded_reason: Optional[str] = None
    lag_days: int = 0  # known data lag in calendar days
    lag_fixture_ready: bool = True
    visibility_fixture_ready: bool = True
    notes: str = ""

    @property
    def point_in_time_ready(self) -> bool:
        return self.lag_fixture_ready and self.visibility_fixture_ready

    def to_dict(self) -> Dict[str, Any]:
        return {
            "factor_id": self.factor_id,
            "family": self.family,
            "source_type": self.source_type,
            "source_as_of": self.source_as_of,
            "visibility_class": self.visibility_class,
            "proxy_level": self.proxy_level,
            "state": self.state,
            "supports_scoring": self.supports_scoring,
            "supports_strategy_candidate": self.supports_strategy_candidate,
            "degraded": self.degraded,
            "degraded_reason": self.degraded_reason,
            "lag_days": self.lag_days,
            "lag_fixture_ready": self.lag_fixture_ready,
            "visibility_fixture_ready": self.visibility_fixture_ready,
            "point_in_time_ready": self.point_in_time_ready,
            "notes": self.notes,
        }

    @classmethod
    def observation_only(
        cls,
        factor_id: str,
        family: str,
        source_type: SourceType,
        visibility_class: VisibilityClass = "proxy",
        proxy_level: ProxyLevel = "rule_based",
        notes: str = "",
    ) -> "FactorMeta":
        """Convenience constructor for observation-only factors."""
        return cls(
            factor_id=factor_id,
            family=family,
            source_type=source_type,
            visibility_class=visibility_class,
            proxy_level=proxy_level,
            state="observation_only",
            supports_scoring=False,
            supports_strategy_candidate=False,
            lag_fixture_ready=False,
            visibility_fixture_ready=False,
            notes=notes,
        )

    @classmethod
    def scoring_supportive(
        cls,
        factor_id: str,
        family: str,
        source_type: SourceType,
        visibility_class: VisibilityClass = "daily_close",
        proxy_level: ProxyLevel = "direct",
        lag_days: int = 0,
        notes: str = "",
    ) -> "FactorMeta":
        """Convenience constructor for scoring-supportive factors."""
        return cls(
            factor_id=factor_id,
            family=family,
            source_type=source_type,
            visibility_class=visibility_class,
            proxy_level=proxy_level,
            state="scoring_supportive",
            supports_scoring=True,
            supports_strategy_candidate=False,
            lag_days=lag_days,
            notes=notes,
        )

    @classmethod
    def production(
        cls,
        factor_id: str,
        family: str,
        source_type: SourceType,
        visibility_class: VisibilityClass = "daily_close",
        proxy_level: ProxyLevel = "direct",
        lag_days: int = 0,
        notes: str = "",
    ) -> "FactorMeta":
        """Convenience constructor for production factors."""
        return cls(
            factor_id=factor_id,
            family=family,
            source_type=source_type,
            visibility_class=visibility_class,
            proxy_level=proxy_level,
            state="production_factor",
            supports_scoring=True,
            supports_strategy_candidate=False,
            lag_days=lag_days,
            notes=notes,
        )

    @classmethod
    def strategy_challenger(
        cls,
        factor_id: str,
        family: str,
        source_type: SourceType,
        visibility_class: VisibilityClass = "daily_close",
        proxy_level: ProxyLevel = "direct",
        lag_days: int = 0,
        notes: str = "",
    ) -> "FactorMeta":
        """Convenience constructor for factors that can enter the strategy challenger pool."""
        return cls(
            factor_id=factor_id,
            family=family,
            source_type=source_type,
            visibility_class=visibility_class,
            proxy_level=proxy_level,
            state="strategy_challenger",
            supports_scoring=True,
            supports_strategy_candidate=True,
            lag_days=lag_days,
            notes=notes,
        )


# ---------------------------------------------------------------------------
# Factor registry: canonical metadata for all factors in the product layer
# ---------------------------------------------------------------------------

FACTOR_REGISTRY: Dict[str, FactorMeta] = {
    # J-1: Price-volume structure & setup
    "j1_divergence": FactorMeta.strategy_challenger(
        "j1_divergence", "J-1", "price_volume",
        visibility_class="daily_close", proxy_level="direct",
        notes="RSI/MACD/OBV 背离，按最近两组确认摆点识别",
    ),
    "j1_candlestick": FactorMeta.production(
        "j1_candlestick", "J-1", "price_volume",
        visibility_class="daily_close", proxy_level="direct",
        notes="最近 1-3 根 K 线组合形态",
    ),
    "j1_false_break": FactorMeta.strategy_challenger(
        "j1_false_break", "J-1", "price_volume",
        visibility_class="daily_close", proxy_level="direct",
        notes="假突破/失败突破识别",
    ),
    "j1_support_setup": FactorMeta.strategy_challenger(
        "j1_support_setup", "J-1", "price_volume",
        visibility_class="daily_close", proxy_level="direct",
        notes="支撑失效后的 setup 分流",
    ),
    "j1_resistance_zone": FactorMeta.strategy_challenger(
        "j1_resistance_zone", "J-1", "price_volume",
        visibility_class="daily_close", proxy_level="direct",
        notes="近端压力/上方承压区识别",
    ),
    "j1_compression_breakout": FactorMeta.strategy_challenger(
        "j1_compression_breakout", "J-1", "price_volume",
        visibility_class="daily_close", proxy_level="direct",
        notes="压缩后放量启动 vs 情绪追价区分",
    ),
    "j1_volume_structure": FactorMeta.strategy_challenger(
        "j1_volume_structure", "J-1", "price_volume",
        visibility_class="daily_close", proxy_level="direct",
        notes="放量突破/缩量回调/放量滞涨等量价结构",
    ),
    "j1_stk_factor_pro": FactorMeta.production(
        "j1_stk_factor_pro", "J-1", "price_volume",
        visibility_class="daily_close", proxy_level="direct",
        lag_days=1,
        notes="股票每日技术面因子，Tushare stk_factor_pro",
    ),
    "j1_volatility_compression": FactorMeta.strategy_challenger(
        "j1_volatility_compression", "J-1", "price_volume",
        visibility_class="daily_close", proxy_level="direct",
        notes="ATR/布林带宽度压缩状态",
    ),

    # J-2: Seasonal / calendar / event windows
    "j2_monthly_win_rate": FactorMeta.scoring_supportive(
        "j2_monthly_win_rate", "J-2", "calendar",
        visibility_class="daily_close", proxy_level="rule_based",
        notes="标的自身历史月度胜率/逆风率，样本边界需显式披露",
    ),
    "j2_sector_season": FactorMeta.scoring_supportive(
        "j2_sector_season", "J-2", "calendar",
        visibility_class="daily_close", proxy_level="rule_based",
        notes="行业旺季前置窗口，规则化日历代理",
    ),
    "j2_earnings_window": FactorMeta.scoring_supportive(
        "j2_earnings_window", "J-2", "calendar",
        visibility_class="daily_close", proxy_level="rule_based",
        notes="财报前后窗口，按季度日历代理",
    ),
    "j2_index_rebalance": FactorMeta.scoring_supportive(
        "j2_index_rebalance", "J-2", "calendar",
        visibility_class="daily_close", proxy_level="rule_based",
        notes="指数调样/半年末/年末窗口",
    ),
    "j2_holiday_window": FactorMeta.scoring_supportive(
        "j2_holiday_window", "J-2", "calendar",
        visibility_class="daily_close", proxy_level="rule_based",
        notes="节假日消费/出行窗口",
    ),
    "j2_policy_event": FactorMeta.observation_only(
        "j2_policy_event", "J-2", "calendar",
        visibility_class="event_driven", proxy_level="rule_based",
        notes="政策会议/医保谈判/产业展会等主题事件窗，样本不足时降级为观察提示",
    ),
    "j2_commodity_season": FactorMeta.scoring_supportive(
        "j2_commodity_season", "J-2", "calendar",
        visibility_class="daily_close", proxy_level="rule_based",
        notes="商品/能源季节性窗口",
    ),
    "j2_dividend_window": FactorMeta.scoring_supportive(
        "j2_dividend_window", "J-2", "calendar",
        visibility_class="daily_close", proxy_level="rule_based",
        notes="高股息行业分红博弈窗口",
    ),

    # J-3: Breadth / chips
    "j3_benchmark_relative": FactorMeta.strategy_challenger(
        "j3_benchmark_relative", "J-3", "breadth",
        visibility_class="daily_close", proxy_level="direct",
        notes="benchmark-relative 超额拐点，支持和拖累都看 5d/20d 超额收益",
    ),
    "j3_sector_breadth": FactorMeta.scoring_supportive(
        "j3_sector_breadth", "J-3", "breadth",
        visibility_class="daily_close", proxy_level="sector_proxy",
        notes="板块内上涨家数/扩散比例，行业级代理，可表达扩散也可表达退潮",
    ),
    "j3_leader_confirmation": FactorMeta.scoring_supportive(
        "j3_leader_confirmation", "J-3", "breadth",
        visibility_class="daily_close", proxy_level="sector_proxy",
        notes="龙头确认与二线跟随，行业级代理，也识别龙头掉队",
    ),
    "j3_northbound": FactorMeta.scoring_supportive(
        "j3_northbound", "J-3", "flow",
        visibility_class="daily_close", proxy_level="sector_proxy",
        notes="北向/南向资金，行业级代理，支持流入也识别流出拖累",
    ),
    "j3_crowding": FactorMeta.scoring_supportive(
        "j3_crowding", "J-3", "sentiment",
        visibility_class="daily_close", proxy_level="market_proxy",
        notes="拥挤度/热度/反身性风险，公募热度代理，极热时只做轻度拖累或风险提示",
    ),
    "j3_ah_comparison": FactorMeta.scoring_supportive(
        "j3_ah_comparison", "J-3", "proxy",
        visibility_class="daily_close", proxy_level="market_proxy",
        lag_days=1,
        notes="A/H 跨市场比价，优先用 Tushare stk_ah_comparison，不再回退 DXY 代理",
    ),

    # J-4: Quality / earnings revision / valuation synergy
    "j4_pe_ttm": FactorMeta.production(
        "j4_pe_ttm", "J-4", "fundamental",
        visibility_class="daily_close", proxy_level="direct",
        notes="PE TTM，真实估值或指数代理",
    ),
    "j4_valuation_proxy": FactorMeta.scoring_supportive(
        "j4_valuation_proxy", "J-4", "proxy",
        visibility_class="daily_close", proxy_level="market_proxy",
        notes="价格位置/估值代理分位，缺少真实 PE 时的代理口径，不等于真实估值分位",
    ),
    "j4_revenue_growth": FactorMeta.production(
        "j4_revenue_growth", "J-4", "fundamental",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=45,
        notes="营收/利润同比增速，季度财报或指数成分股财务代理",
    ),
    "j4_roe": FactorMeta.production(
        "j4_roe", "J-4", "fundamental",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=45,
        notes="ROE，季度财报，存在 45 天 lag",
    ),
    "j4_gross_margin": FactorMeta.production(
        "j4_gross_margin", "J-4", "fundamental",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=45,
        notes="毛利率，季度财报",
    ),
    "j4_peg": FactorMeta.scoring_supportive(
        "j4_peg", "J-4", "fundamental",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=45,
        notes="PEG 代理，PE/增速，增速用历史代理",
    ),
    "j4_cashflow_quality": FactorMeta.scoring_supportive(
        "j4_cashflow_quality", "J-4", "fundamental",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=45,
        notes="经营现金流质量，季度财报",
    ),
    "j4_leverage": FactorMeta.scoring_supportive(
        "j4_leverage", "J-4", "fundamental",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=45,
        notes="资产负债率/杠杆压力，季度财报",
    ),
    "j4_earnings_momentum": FactorMeta.observation_only(
        "j4_earnings_momentum", "J-4", "fundamental",
        visibility_class="quarterly", proxy_level="rule_based",
        notes="盈利动量/EPS 修正代理，无可靠 point-in-time 源时停在观察层",
    ),
    "j4_convertible_bond_proxy": FactorMeta.scoring_supportive(
        "j4_convertible_bond_proxy", "J-4", "proxy",
        visibility_class="daily_close", proxy_level="direct",
        lag_days=1,
        notes="A 股个股对应可转债辅助层，Tushare cb_basic / cb_daily / cb_factor_pro",
    ),

    # M-1: Macro regime / leading indicators
    "m1_sensitivity_vector": FactorMeta.production(
        "m1_sensitivity_vector", "M-1", "macro",
        visibility_class="t_n", proxy_level="direct",
        lag_days=1,
        notes="利率/美元/油价/人民币 敏感度向量，与当前风格状态做顺逆风匹配",
    ),
    "m1_demand_cycle": FactorMeta.production(
        "m1_demand_cycle", "M-1", "macro",
        visibility_class="t_n", proxy_level="direct",
        lag_days=7,
        notes="PMI/新订单/生产 的景气方向，按板块偏好映射顺逆风",
    ),
    "m1_price_chain": FactorMeta.scoring_supportive(
        "m1_price_chain", "M-1", "macro",
        visibility_class="t_n", proxy_level="direct",
        lag_days=7,
        notes="PPI/价格链条状态，按板块对通胀/通缩的敏感度做映射",
    ),
    "m1_credit_impulse": FactorMeta.scoring_supportive(
        "m1_credit_impulse", "M-1", "macro",
        visibility_class="t_n", proxy_level="direct",
        lag_days=7,
        notes="M1-M2 剪刀差/社融 的信用脉冲方向，更偏中期环境因子",
    ),
    "m1_regime_context": FactorMeta.scoring_supportive(
        "m1_regime_context", "M-1", "macro",
        visibility_class="daily_close", proxy_level="rule_based",
        notes="当前 regime 与板块偏好的映射，不单独决定方向",
    ),

    # J-5: ETF / fund-specific factors
    "j5_etf_premium": FactorMeta.scoring_supportive(
        "j5_etf_premium", "J-5", "etf_specific",
        visibility_class="daily_close", proxy_level="direct",
        notes="ETF 折溢价，场内 ETF 专属",
    ),
    "j5_etf_share_change": FactorMeta.scoring_supportive(
        "j5_etf_share_change", "J-5", "etf_specific",
        visibility_class="t1", proxy_level="direct",
        lag_days=1,
        notes="ETF 份额申赎/资金流，T+1 可见",
    ),
    "j5_fund_factor_pro": FactorMeta.scoring_supportive(
        "j5_fund_factor_pro", "J-5", "etf_specific",
        visibility_class="daily_close", proxy_level="direct",
        lag_days=1,
        notes="场内基金/ETF 技术因子，Tushare fund_factor_pro",
    ),
    "j5_tracking_error": FactorMeta.scoring_supportive(
        "j5_tracking_error", "J-5", "etf_specific",
        visibility_class="daily_close", proxy_level="direct",
        notes="跟踪偏离/跟踪误差",
    ),
    "j5_component_concentration": FactorMeta.scoring_supportive(
        "j5_component_concentration", "J-5", "etf_specific",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=30,
        notes="成分股集中度，季度披露",
    ),
    "j5_theme_purity": FactorMeta.scoring_supportive(
        "j5_theme_purity", "J-5", "etf_specific",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=30,
        notes="主题纯度与跨市场暴露",
    ),
    "j5_directional_catalyst": FactorMeta.scoring_supportive(
        "j5_directional_catalyst", "J-5", "etf_specific",
        visibility_class="event_driven", proxy_level="sector_proxy",
        notes="基于跟踪基准、行业暴露和核心成分关键词共振的 ETF/基金方向催化代理",
    ),
    "j5_fund_benchmark_fit": FactorMeta.scoring_supportive(
        "j5_fund_benchmark_fit", "J-5", "fund_specific",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=30,
        notes="场外基金业绩基准拟合度",
    ),
    "j5_fund_sales_ratio": FactorMeta.scoring_supportive(
        "j5_fund_sales_ratio", "J-5", "fund_specific",
        visibility_class="annual", proxy_level="direct",
        lag_days=30,
        notes="公募基金销售保有结构，行业级渠道环境信息项",
    ),
    "j5_style_drift": FactorMeta.scoring_supportive(
        "j5_style_drift", "J-5", "fund_specific",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=30,
        notes="风格漂移，持仓季报代理",
    ),
    "j5_manager_stability": FactorMeta.scoring_supportive(
        "j5_manager_stability", "J-5", "fund_specific",
        visibility_class="event_driven", proxy_level="direct",
        notes="基金经理稳定性，任职年限代理",
    ),
    "j5_redemption_pressure": FactorMeta.scoring_supportive(
        "j5_redemption_pressure", "J-5", "fund_specific",
        visibility_class="quarterly", proxy_level="direct",
        lag_days=30,
        notes="申赎友好度、费率和确认节奏",
    ),
    "j5_gold_spot_anchor": FactorMeta.scoring_supportive(
        "j5_gold_spot_anchor", "J-5", "etf_specific",
        visibility_class="daily_close", proxy_level="direct",
        lag_days=1,
        notes="黄金现货锚定，基于上海黄金交易所现货日线",
    ),
}


def get_factor_meta(factor_id: str) -> Optional[FactorMeta]:
    """Look up factor metadata by ID. Returns None if not registered."""
    return FACTOR_REGISTRY.get(factor_id)


def is_strategy_candidate(factor_id: str) -> bool:
    """Check if a factor is eligible for strategy challenger pool."""
    meta = get_factor_meta(factor_id)
    if meta is None:
        return False
    return meta.supports_strategy_candidate and not meta.degraded


def factor_state(factor_id: str) -> Optional[FactorState]:
    """Return the current state of a factor."""
    meta = get_factor_meta(factor_id)
    return meta.state if meta else None


def factor_meta_payload(
    factor_id: str,
    *,
    overrides: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return a JSON-safe factor metadata payload with optional runtime overrides."""
    meta = get_factor_meta(factor_id)
    if meta is None:
        return {}
    payload = meta.to_dict()
    for key, value in dict(overrides or {}).items():
        if value is None:
            continue
        payload[key] = value
    payload["point_in_time_ready"] = bool(payload.get("lag_fixture_ready")) and bool(payload.get("visibility_fixture_ready"))
    payload["supports_strategy_candidate"] = bool(payload.get("supports_strategy_candidate")) and bool(payload.get("point_in_time_ready"))
    return payload


def factor_rows_from_dimensions(dimensions: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for dimension in dict(dimensions or {}).values():
        for item in list(dict(dimension or {}).get("factors") or []):
            factor_id = str(item.get("factor_id", "")).strip()
            if factor_id:
                rows.append(dict(item))
    return rows


def summarize_factor_contracts(
    factor_rows: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int = 12,
) -> Dict[str, Any]:
    rows = [dict(item) for item in factor_rows if str(item.get("factor_id", "")).strip()]
    if not rows:
        return {
            "registered_factor_rows": 0,
            "families": {},
            "states": {},
            "visibility_classes": {},
            "proxy_levels": {},
            "fixture_readiness": {
                "total_factors": 0,
                "lag_ready_count": 0,
                "lag_blocked_count": 0,
                "visibility_ready_count": 0,
                "visibility_blocked_count": 0,
                "point_in_time_ready_count": 0,
                "point_in_time_blocked_count": 0,
                "strategy_candidate_total": 0,
                "strategy_candidate_ready_count": 0,
                "degraded_count": 0,
                "max_lag_days": 0,
            },
            "strategy_candidate_factor_ids": [],
            "point_in_time_blockers": [],
            "lag_visibility_blockers": [],
            "degraded_factor_ids": [],
            "sample_rows": [],
        }

    family_counter: Counter[str] = Counter()
    state_counter: Counter[str] = Counter()
    visibility_counter: Counter[str] = Counter()
    proxy_counter: Counter[str] = Counter()
    strategy_candidate_factor_ids: List[str] = []
    point_in_time_blockers: List[Dict[str, str]] = []
    lag_visibility_blockers: List[Dict[str, Any]] = []
    degraded_factor_ids: List[str] = []
    sample_rows: List[Dict[str, Any]] = []
    seen_samples: set[str] = set()
    lag_ready_count = 0
    lag_blocked_count = 0
    visibility_ready_count = 0
    visibility_blocked_count = 0
    point_in_time_ready_count = 0
    point_in_time_blocked_count = 0
    strategy_candidate_total = 0
    strategy_candidate_ready_count = 0
    degraded_count = 0
    max_lag_days = 0

    for item in rows:
        factor_id = str(item.get("factor_id", "")).strip()
        payload = dict(item.get("factor_meta") or factor_meta_payload(factor_id))
        if not payload:
            continue
        base_meta = get_factor_meta(factor_id)
        family = str(payload.get("family", "")).strip()
        state = str(payload.get("state", "")).strip()
        visibility = str(payload.get("visibility_class", "")).strip()
        proxy_level = str(payload.get("proxy_level", "")).strip()
        lag_days = max(int(payload.get("lag_days") or 0), 0)
        lag_ready = bool(payload.get("lag_fixture_ready"))
        visibility_ready = bool(payload.get("visibility_fixture_ready"))
        point_in_time_ready = bool(payload.get("point_in_time_ready"))
        intended_strategy_candidate = bool(base_meta.supports_strategy_candidate) if base_meta is not None else bool(payload.get("supports_strategy_candidate"))
        if family:
            family_counter[family] += 1
        if state:
            state_counter[state] += 1
        if visibility:
            visibility_counter[visibility] += 1
        if proxy_level:
            proxy_counter[proxy_level] += 1
        max_lag_days = max(max_lag_days, lag_days)
        if lag_ready:
            lag_ready_count += 1
        else:
            lag_blocked_count += 1
        if visibility_ready:
            visibility_ready_count += 1
        else:
            visibility_blocked_count += 1
        if point_in_time_ready:
            point_in_time_ready_count += 1
        else:
            point_in_time_blocked_count += 1
        if intended_strategy_candidate:
            strategy_candidate_total += 1
            if point_in_time_ready:
                strategy_candidate_ready_count += 1
        if bool(payload.get("supports_strategy_candidate")):
            strategy_candidate_factor_ids.append(factor_id)
        if bool(payload.get("degraded")):
            degraded_factor_ids.append(factor_id)
            degraded_count += 1
        if not point_in_time_ready:
            point_in_time_blockers.append(
                {
                    "factor_id": factor_id,
                    "family": family,
                    "reason": str(payload.get("degraded_reason") or payload.get("notes") or "lag / visibility fixture incomplete").strip(),
                }
            )
            lag_visibility_blockers.append(
                {
                    "factor_id": factor_id,
                    "family": family,
                    "reason": str(payload.get("degraded_reason") or payload.get("notes") or "lag / visibility fixture incomplete").strip(),
                    "lag_days": lag_days,
                    "visibility_class": visibility,
                    "proxy_level": proxy_level,
                    "lag_fixture_ready": lag_ready,
                    "visibility_fixture_ready": visibility_ready,
                    "point_in_time_ready": point_in_time_ready,
                    "supports_strategy_candidate": intended_strategy_candidate,
                }
            )
        if factor_id not in seen_samples and len(sample_rows) < max(int(sample_limit), 0):
            sample_rows.append(
                {
                    "factor_id": factor_id,
                    "name": str(item.get("name", "")).strip(),
                    "family": family,
                    "state": state,
                    "visibility_class": visibility,
                    "proxy_level": proxy_level,
                    "supports_scoring": bool(payload.get("supports_scoring")),
                    "supports_strategy_candidate": bool(payload.get("supports_strategy_candidate")),
                    "intended_strategy_candidate": intended_strategy_candidate,
                    "lag_days": lag_days,
                    "lag_fixture_ready": lag_ready,
                    "visibility_fixture_ready": visibility_ready,
                    "point_in_time_ready": point_in_time_ready,
                }
            )
            seen_samples.add(factor_id)

    return {
        "registered_factor_rows": sum(family_counter.values()),
        "families": dict(sorted(family_counter.items())),
        "states": dict(sorted(state_counter.items())),
        "visibility_classes": dict(sorted(visibility_counter.items())),
        "proxy_levels": dict(sorted(proxy_counter.items())),
        "fixture_readiness": {
            "total_factors": len(rows),
            "lag_ready_count": lag_ready_count,
            "lag_blocked_count": lag_blocked_count,
            "visibility_ready_count": visibility_ready_count,
            "visibility_blocked_count": visibility_blocked_count,
            "point_in_time_ready_count": point_in_time_ready_count,
            "point_in_time_blocked_count": point_in_time_blocked_count,
            "strategy_candidate_total": strategy_candidate_total,
            "strategy_candidate_ready_count": strategy_candidate_ready_count,
            "degraded_count": degraded_count,
            "max_lag_days": max_lag_days,
        },
        "strategy_candidate_factor_ids": sorted(dict.fromkeys(strategy_candidate_factor_ids)),
        "point_in_time_blockers": point_in_time_blockers,
        "lag_visibility_blockers": lag_visibility_blockers,
        "degraded_factor_ids": sorted(dict.fromkeys(degraded_factor_ids)),
        "sample_rows": sample_rows,
    }


def summarize_factor_contracts_from_analysis(analysis: Mapping[str, Any], *, sample_limit: int = 12) -> Dict[str, Any]:
    summary = summarize_factor_contracts(
        factor_rows_from_dimensions(dict(analysis or {}).get("dimensions") or {}),
        sample_limit=sample_limit,
    )
    summary["symbol"] = str(dict(analysis or {}).get("symbol", "")).strip()
    summary["name"] = str(dict(analysis or {}).get("name", "")).strip()
    return summary


def summarize_factor_contracts_from_analyses(
    analyses: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int = 16,
) -> Dict[str, Any]:
    analysis_rows = list(analyses)
    rows: List[Dict[str, Any]] = []
    for analysis in analysis_rows:
        rows.extend(factor_rows_from_dimensions(dict(analysis or {}).get("dimensions") or {}))
    summary = summarize_factor_contracts(rows, sample_limit=sample_limit)
    summary["analysis_count"] = len(analysis_rows)
    return summary
