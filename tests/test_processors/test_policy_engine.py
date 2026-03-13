"""Tests for policy engine helpers."""

from __future__ import annotations

import src.processors.policy_engine as policy_engine_module
from src.processors.policy_engine import PolicyEngine
from src.utils.data import load_watchlist

OFFICIAL_NOTICE_HTML = """
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <title>关于印发《加快构建新型电力系统行动方案（2024—2027年）》的通知_国务院部门文件_中国政府网</title>
  </head>
  <body>
    <div class="header">首页 登录 打印 收藏</div>
    <div class="policyLibraryOverview_content">
      标 题： 关于印发《加快构建新型电力系统行动方案（2024—2027年）》的通知
      发文机关： 国家发展改革委 国家能源局 国家数据局
      发文字号： 发改能源〔2024〕1128号
      来 源： 国家发展改革委网站
      成文日期： 2024年7月25日
      主题分类： 国土资源、能源\电力
      公文种类： 通知
    </div>
    <div class="pages_content">
      <p>国家发展改革委 国家能源局 国家数据局关于印发《加快构建新型电力系统行动方案（2024—2027年）》的通知。</p>
      <p>为深入贯彻落实习近平总书记关于构建新型电力系统的重要指示精神，进一步加大工作力度，加快推进新型电力系统建设。</p>
      <p>提出推进特高压和配电网改造，要求在2026年6月30日前形成首批重点项目清单，年内启动示范工程申报。</p>
      <p>同时强调防止地方盲目重复建设。</p>
      <a href="./P020240808575593646209.pdf">《加快构建新型电力系统行动方案（2024—2027年）》.pdf</a>
    </div>
    <div class="footer">首页 无障碍 收藏</div>
  </body>
</html>
""".strip()


class FakeResponse:
    def __init__(self, body: str) -> None:
        self.content = body.encode("utf-8")
        self.encoding = "ISO-8859-1"
        self.apparent_encoding = "utf-8"
        self.headers = {"content-type": "text/html"}

    @property
    def text(self) -> str:
        return self.content.decode(self.encoding or "utf-8", errors="replace")

    def raise_for_status(self) -> None:
        return None


class FakeBinaryResponse:
    def __init__(self, body: bytes, content_type: str = "application/pdf") -> None:
        self.content = body
        self.encoding = "utf-8"
        self.apparent_encoding = "utf-8"
        self.headers = {"content-type": content_type}

    @property
    def text(self) -> str:
        return self.content.decode(self.encoding or "utf-8", errors="replace")

    def raise_for_status(self) -> None:
        return None


def test_policy_engine_matches_keyword():
    engine = PolicyEngine()
    match = engine.best_match("电网和特高压投资")
    assert match is not None
    assert match["id"] == "power-grid"


def test_policy_engine_extracts_numbers():
    engine = PolicyEngine()
    numbers = engine.extract_numbers("计划投资 2.5万亿，目标增速 10%，周期 5年。")
    assert "2.5万亿" in numbers
    assert "10%" in numbers


def test_policy_engine_match_policy_reports_aliases_and_confidence():
    engine = PolicyEngine()
    matched = engine.match_policy("关于加快新型电力系统建设并推进特高压投资的通知")

    assert matched is not None
    assert matched.template["id"] == "power-grid"
    assert "特高压" in matched.matched_aliases
    assert matched.confidence_label in {"高", "中"}


def test_policy_engine_classifies_direction_and_stage():
    engine = PolicyEngine()
    text = "关于推动人工智能产业发展的行动计划，提出加快算力基础设施建设。"

    assert engine.classify_policy_direction(text) == "偏支持"
    assert engine.infer_policy_stage("行动计划", text) == "顶层规划/行动方案"


def test_policy_engine_extracts_timeline_points():
    engine = PolicyEngine()
    text = "要求在2026年6月30日前完成首批申报，年内形成重点项目清单。"
    points = engine.extract_timeline_points(text)

    assert points == ["要求在2026年6月30日前完成首批申报", "年内形成重点项目清单"]


def test_policy_engine_load_context_repairs_encoding_and_extracts_notice_page(monkeypatch):
    monkeypatch.setattr(policy_engine_module.requests, "get", lambda *args, **kwargs: FakeResponse(OFFICIAL_NOTICE_HTML))
    engine = PolicyEngine()

    context = engine.load_context("https://www.gov.cn/zhengce/zhengceku/202408/content_6966863.htm")

    assert context.title == "关于印发《加快构建新型电力系统行动方案（2024—2027年）》的通知"
    assert context.metadata["发文机关"] == "国家发展改革委 国家能源局 国家数据局"
    assert context.metadata["来源"] == "国家发展改革委网站"
    assert "加快推进新型电力系统建设" in context.text
    assert context.extraction_quality == "正文抽取部分成功"
    assert context.source_authority == "官方政府站点，且页面含发文机关元信息"
    assert "页面正文" in context.coverage_scope
    assert "附件标题（1个）" in context.coverage_scope
    assert "《加快构建新型电力系统行动方案（2024—2027年）》.pdf" in context.attachment_titles
    assert any("网页编码按 `utf-8` 自动修正。" == item for item in context.extraction_notes)
    assert any("PDF/OFD 附件" in item for item in context.extraction_notes)


def test_policy_engine_keyword_and_long_text_keep_direction_consistent():
    engine = PolicyEngine()
    long_text = (
        "国家发展改革委印发行动方案，提出加快新型电力系统建设，推进特高压和配电网改造，"
        "要求在2026年6月30日前形成首批重点项目清单，年内启动示范工程申报，同时强调防止地方盲目重复建设。"
    )

    keyword_analysis = engine.analyze_context(engine.load_context("电网"), [])
    text_analysis = engine.analyze_context(engine.load_context(long_text), [])

    assert keyword_analysis["policy_direction"] == "偏支持"
    assert text_analysis["policy_direction"] == "偏支持"
    assert keyword_analysis["policy_stage"] == "主题跟踪阶段"
    assert text_analysis["policy_stage"] == "顶层规划/行动方案"
    assert "要求在2026年6月30日前形成首批重点项目清单" in text_analysis["timeline_points"]
    assert any("模板/规则推断" in item for item in keyword_analysis["unconfirmed_lines"])


def test_policy_engine_analyze_context_separates_facts_inference_and_unknowns(monkeypatch):
    monkeypatch.setattr(policy_engine_module.requests, "get", lambda *args, **kwargs: FakeResponse(OFFICIAL_NOTICE_HTML))
    engine = PolicyEngine()
    context = engine.load_context("https://www.gov.cn/zhengce/zhengceku/202408/content_6966863.htm")

    analysis = engine.analyze_context(context, [])

    assert analysis["policy_direction"] == "偏支持"
    assert analysis["policy_stage"] == "顶层规划/行动方案"
    assert analysis["source_authority"] == "官方政府站点，且页面含发文机关元信息"
    assert "附件标题（1个）" in analysis["coverage_scope"]
    assert any("原文元信息：发文机关" in item for item in analysis["body_facts"])
    assert any("页面附件：" in item for item in analysis["body_facts"])
    assert any("提出推进特高压和配电网改造" in item for item in analysis["body_facts"])
    assert any("受益链条映射" in item for item in analysis["inference_lines"])
    assert any("PDF/OFD 附件" in item for item in analysis["unconfirmed_lines"])


def test_policy_engine_load_context_extracts_pdf_attachment_text(monkeypatch):
    def fake_get(url: str, *args, **kwargs):
        if url.endswith(".pdf"):
            return FakeBinaryResponse(b"%PDF-1.4")
        return FakeResponse(OFFICIAL_NOTICE_HTML)

    monkeypatch.setattr(policy_engine_module.requests, "get", fake_get)
    monkeypatch.setattr(
        PolicyEngine,
        "_extract_pdf_text",
        lambda self, content, source, notes: (
            "提出推进特高压和配电网改造，要求在2026年6月30日前形成首批重点项目清单。",
            {"标题": "加快构建新型电力系统行动方案"},
            "加快构建新型电力系统行动方案",
        ),
    )
    engine = PolicyEngine()

    context = engine.load_context("https://www.gov.cn/zhengce/zhengceku/202408/content_6966863.htm")

    assert context.extraction_quality == "公告页正文 + PDF附件已补抽"
    assert "PDF附件正文" in context.coverage_scope
    assert "提出推进特高压和配电网改造" in context.text
    assert any("已补抽 PDF 附件正文" in item for item in context.extraction_notes)


def test_policy_engine_load_context_supports_direct_pdf(monkeypatch):
    monkeypatch.setattr(policy_engine_module.requests, "get", lambda *args, **kwargs: FakeBinaryResponse(b"%PDF-1.4"))
    monkeypatch.setattr(
        PolicyEngine,
        "_extract_pdf_text",
        lambda self, content, source, notes: (
            "推进特高压和配电网改造，年内启动示范工程申报。",
            {"标题": "新型电力系统行动方案", "来源": "国家发展改革委"},
            "新型电力系统行动方案",
        ),
    )
    engine = PolicyEngine()

    context = engine.load_context("https://www.gov.cn/policy/power-grid.pdf")

    assert context.title == "新型电力系统行动方案"
    assert context.extraction_quality == "PDF正文已抽取"
    assert context.coverage_scope == ["PDF正文"]
    assert "推进特高压和配电网改造" in context.text
    assert any("已抽取 PDF 原文" in item for item in context.extraction_notes)


def test_policy_engine_analysis_includes_policy_taxonomy():
    engine = PolicyEngine()

    analysis = engine.analyze_context(engine.load_context("电网"), [])

    assert analysis["policy_taxonomy"]["policy_family"] == "能源基础设施 / 新型电力系统"
    assert analysis["policy_taxonomy"]["source_level"] == "主题关键词/待确认"
    assert analysis["policy_taxonomy"]["policy_tone"] == "偏支持"


def test_policy_engine_watchlist_impact_matches_asset_name_alias():
    engine = PolicyEngine()
    policy = {
        "beneficiary_nodes": ["半导体自主可控"],
        "risk_nodes": [],
        "mapped_assets": ["芯片ETF"],
    }
    impacts = engine.watchlist_impact(policy, [])

    watchlist_names = [item["name"] for item in load_watchlist()]
    assert any("芯片ETF" in name for name in watchlist_names)
    assert any("芯片" in item for item in impacts)
