"""Policy keyword matching and heuristic parsing."""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass, field
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

from src.utils.config import PROJECT_ROOT
from src.utils.data import load_json, load_watchlist

_SUPPORT_TOKENS = ("支持", "鼓励", "推进", "加快", "完善", "实施", "提升", "增强", "扩大", "建设")
_RESTRICT_TOKENS = ("限制", "压降", "严控", "约束", "禁止", "整治", "从严", "防止", "避免", "规范")
_RISK_TOKENS = _RESTRICT_TOKENS + ("风险", "审慎", "过热", "重复建设", "低于预期")
_TIMELINE_PATTERNS = (
    r"\d{4}[—-]\d{4}年",
    r"\d{4}年\d{1,2}月\d{1,2}日",
    r"\d{4}年\d{1,2}月",
    r"\d{1,2}月\d{1,2}日",
    r"\d+年内",
    r"\d+个月内",
    r"月底前",
    r"年内",
)
_NOISE_TOKENS = (
    "首页",
    "登录",
    "个人中心",
    "退出",
    "邮箱",
    "无障碍",
    "打印",
    "收藏",
    "留言",
    "分享到",
    "返回顶部",
)
_HTML_BODY_SELECTORS = (
    ".pages_content",
    ".trs_editor_view",
    ".TRS_Editor",
    ".article-content",
    ".article",
    ".content",
    "article",
    "#UCAP-CONTENT",
    "#zoom",
    ".zoom",
    ".detail-content",
    ".news_show",
    ".txt_con",
    ".entry-content",
)
_METADATA_LABELS = ("标题", "发文机关", "发文字号", "来源", "主题分类", "成文日期", "发布日期", "公文种类")


@dataclass
class PolicyContext:
    title: str
    source: str
    text: str
    metadata: Dict[str, str] = field(default_factory=dict)
    extraction_quality: str = "关键词输入"
    extraction_notes: List[str] = field(default_factory=list)
    content_kind: str = "keyword"
    body_selector: str = ""
    source_authority: str = "待确认"
    coverage_scope: List[str] = field(default_factory=list)
    attachment_titles: List[str] = field(default_factory=list)


@dataclass
class PolicyTemplateMatch:
    template: Dict[str, Any]
    score: int
    matched_aliases: List[str]
    confidence_label: str


@dataclass
class AttachmentResource:
    title: str
    url: str
    kind: str


class PolicyEngine:
    """Heuristic policy analysis without requiring an LLM key."""

    _OFD_NS = {"ofd": "http://www.ofdspec.org/2016"}

    def __init__(self) -> None:
        self.library = load_json(PROJECT_ROOT / "data" / "policy_library.json", default=[]) or []

    def load_context(self, target: str) -> PolicyContext:
        normalized_target = self._normalize_text(target)
        if normalized_target.startswith(("http://", "https://")):
            return self._load_url_context(normalized_target)
        if self._looks_like_long_text(normalized_target):
            return PolicyContext(
                title=self._derive_text_title(normalized_target),
                source="provided-text",
                text=normalized_target,
                extraction_quality="已抽到用户提供长正文",
                extraction_notes=["当前输入为用户提供长正文片段，未核对是否覆盖附件、表格和落款。"],
                content_kind="text",
                source_authority="用户提供文本，未自动核验发文机关和发布日期",
                coverage_scope=["用户提供正文片段"],
            )
        return PolicyContext(
            title=normalized_target,
            source="keyword",
            text=normalized_target,
            extraction_quality="仅关键词，无正文事实",
            extraction_notes=["当前输入为关键词，正文事实、时间线和发文机关都未确认。"],
            content_kind="keyword",
            source_authority="待确认（仅关键词）",
            coverage_scope=["仅关键词"],
        )

    def match_policy(self, text: str) -> Optional[PolicyTemplateMatch]:
        lower_text = self._normalize_text(text).lower()
        best_template: Optional[Dict[str, Any]] = None
        best_aliases: List[str] = []
        best_score = -1
        for item in self.library:
            aliases = [str(alias).strip() for alias in item.get("aliases", []) if str(alias).strip()]
            matched_aliases = [alias for alias in aliases if alias.lower() in lower_text]
            score = len(matched_aliases) * 3
            name = str(item.get("name", "")).strip()
            if name and name.lower() in lower_text:
                score += 2
            if score > best_score:
                best_score = score
                best_template = item
                best_aliases = matched_aliases
        if best_template is None or best_score <= 0:
            return None
        confidence = "高" if best_score >= 6 or len(best_aliases) >= 3 else "中" if best_score >= 3 else "低"
        return PolicyTemplateMatch(
            template=best_template,
            score=best_score,
            matched_aliases=best_aliases,
            confidence_label=confidence,
        )

    def best_match(self, text: str) -> Optional[Dict[str, Any]]:
        matched = self.match_policy(text)
        return matched.template if matched else None

    def analyze_context(
        self,
        context: PolicyContext,
        holdings: Optional[Iterable[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        matched = self.match_policy(f"{context.title} {context.text}")
        template = matched.template if matched else self._default_template(context)
        holdings_list = list(holdings or [])

        direction = self.classify_policy_direction(f"{context.title} {context.text}")
        direction_support, direction_restrict = self._direction_signal_sentences(context.text)
        direction_reason = self._direction_reason(direction_support, direction_restrict)
        if direction == "中性/待原文确认" and matched:
            direction = "偏支持"
            direction_reason = "正文方向词有限，当前按命中的政策主题做弱推断，结论不等同于完整原文审读。"

        stage = self.infer_policy_stage(context.title, context.text)
        stage_reason = self._stage_reason(context.title, context.text, stage)
        if stage == "阶段待原文确认" and context.content_kind == "keyword":
            stage = "主题跟踪阶段"
            stage_reason = "当前只有主题关键词，没有具体文件标题，阶段仅能按主题跟踪处理。"

        timeline_points = self.extract_timeline_points(context.text)
        explicit_risk_sentences = self.extract_risk_sentences(context.text)
        support_points = self.extract_support_points(context.text, template)
        beneficiary_nodes = self.extract_beneficiary_nodes(context.text, template)
        risk_points = self.extract_risk_points(context.text, template)

        headline_numbers = self._merge_unique(
            list(template.get("headline_numbers", [])),
            self.extract_numbers(f"{context.title} {context.text}"),
        )[:6]

        metadata_lines = [
            f"{label}：{value}"
            for label, value in context.metadata.items()
            if value and not (label == "标题" and str(value).strip() == context.title)
        ]
        body_facts: List[str] = []
        if context.content_kind == "url" and context.title:
            body_facts.append(f"原文标题：{context.title}")
        body_facts.extend(f"原文元信息：{line}" for line in metadata_lines[:3])
        if context.attachment_titles and not context.source.lower().endswith((".pdf", ".ofd")):
            body_facts.append(f"页面附件：{'；'.join(context.attachment_titles[:2])}")
        for sentence in self.extract_policy_facts(context.text, template):
            body_facts.append(f"原文明确动作：{sentence}")
        if explicit_risk_sentences:
            body_facts.append(f"原文明确约束：{explicit_risk_sentences[0]}")

        inference_lines: List[str] = []
        if matched:
            alias_text = "、".join(matched.matched_aliases) if matched.matched_aliases else "仅命中主题名"
            inference_lines.append(
                f"模板命中 `{template.get('name', context.title)}`，别名命中 `{alias_text}`，模板置信度 `{matched.confidence_label}`。"
            )
        else:
            inference_lines.append("当前未命中本地政策模板，受益链条和风险点只能按通用规则输出。")
        inference_lines.append(f"政策方向判断为 `{direction}`：{direction_reason}")
        inference_lines.append(f"阶段判断为 `{stage}`：{stage_reason}")
        if beneficiary_nodes:
            inference_lines.append(f"受益链条映射：{' -> '.join(beneficiary_nodes)}。")
        if risk_points:
            inference_lines.append(f"风险映射：{'; '.join(risk_points[:3])}。")

        unconfirmed_lines = list(context.extraction_notes)
        if context.content_kind == "url":
            if context.source.lower().endswith(".pdf"):
                unconfirmed_lines.append("PDF 正文已抽取，但表格、扫描页、图片页和版式内容仍可能存在漏提。")
            elif context.source.lower().endswith(".ofd"):
                unconfirmed_lines.append("OFD 正文已抽取，但表格、矢量版式和特殊对象仍可能存在漏提。")
            elif context.extraction_quality != "正文抽取较完整":
                unconfirmed_lines.append("当前 URL 抽取仍可能漏掉附件、表格、折叠区或 PDF 原件，不能替代完整原文复核。")
        if context.content_kind == "keyword":
            unconfirmed_lines.append("当前没有正文事实，政策方向、受益链条和风险点主要来自模板/规则推断。")
        if context.content_kind == "text":
            unconfirmed_lines.append("当前输入不是官方发布页，发文机关、文号和发布日期未自动核验。")
        if not timeline_points:
            unconfirmed_lines.append("原文未抽到明确时间线，当前节奏判断主要来自标题词和模板经验。")
        if not explicit_risk_sentences:
            unconfirmed_lines.append("原文未抽到明确风险或约束条款，风险点以模板映射为主。")

        summary = self._build_summary(
            context=context,
            direction=direction,
            stage=stage,
            template=template,
            support_points=support_points,
            timeline_points=timeline_points,
        )
        policy_taxonomy = self._build_policy_taxonomy(
            context=context,
            template=template,
            direction=direction,
            stage=stage,
        )

        watchlist_policy = {
            "beneficiary_nodes": template.get("beneficiary_nodes", []),
            "risk_nodes": template.get("risk_nodes", []),
            "mapped_assets": template.get("mapped_assets", []),
        }
        timeline_summary = self._build_timeline_summary(context.title, template, timeline_points)

        return {
            "title": context.title,
            "source": context.source,
            "input_type": self._describe_context_kind(context),
            "theme": template.get("name", context.title),
            "summary": summary,
            "match_confidence": matched.confidence_label if matched else "低",
            "matched_aliases": matched.matched_aliases if matched else [],
            "policy_direction": direction,
            "policy_stage": stage,
            "policy_goal": template.get("policy_goal", ""),
            "timeline": timeline_summary,
            "timeline_points": timeline_points,
            "support_points": support_points,
            "beneficiary_nodes": beneficiary_nodes,
            "risk_points": risk_points,
            "headline_numbers": headline_numbers,
            "watchlist_impact": self.watchlist_impact(watchlist_policy, holdings_list),
            "raw_excerpt": self._build_excerpt(context.text),
            "metadata_lines": metadata_lines,
            "body_facts": body_facts,
            "policy_taxonomy": policy_taxonomy,
            "inference_lines": inference_lines,
            "unconfirmed_lines": self._merge_unique(unconfirmed_lines),
            "extraction_status": context.extraction_quality,
            "source_authority": context.source_authority,
            "coverage_scope": context.coverage_scope,
            "attachment_titles": context.attachment_titles,
        }

    def extract_numbers(self, text: str) -> List[str]:
        matches = re.findall(
            r"\d{4}[—-]\d{4}年|\d{4}年\d{1,2}月\d{1,2}日|\d+(?:\.\d+)?[%万亿亿元万千]+",
            self._normalize_text(text),
        )
        unique: List[str] = []
        for item in matches:
            if item not in unique:
                unique.append(item)
        return unique[:6]

    def classify_policy_direction(self, text: str) -> str:
        lowered = self._normalize_text(text).lower()
        support_hits = sum(lowered.count(token) for token in _SUPPORT_TOKENS)
        restrict_hits = sum(lowered.count(token) for token in _RESTRICT_TOKENS)
        if support_hits and restrict_hits:
            if support_hits >= restrict_hits * 2:
                return "偏支持"
            if restrict_hits >= support_hits * 2:
                return "偏约束"
            return "支持与约束并存"
        if support_hits:
            return "偏支持"
        if restrict_hits:
            return "偏约束"
        return "中性/待原文确认"

    def infer_policy_stage(self, title: str, text: str) -> str:
        title_text = self._normalize_text(title).lower()
        body_text = self._normalize_text(text).lower()
        if "征求意见" in title_text or "征求意见" in body_text:
            return "征求意见阶段"
        if any(token in title_text for token in ("行动计划", "行动方案", "方案", "规划")):
            return "顶层规划/行动方案"
        if any(token in title_text for token in ("实施细则", "细则", "办法", "申报指南", "工作指引")):
            return "执行细则/落地阶段"
        if "试点" in title_text:
            return "试点推进阶段"
        if any(token in title_text for token in ("通知", "意见", "决定")):
            return "政策通知/执行部署"
        if any(token in body_text for token in ("实施细则", "细则", "办法", "申报指南", "工作指引", "申报")):
            return "执行细则/落地阶段"
        if any(token in body_text for token in ("行动计划", "行动方案", "方案", "规划")):
            return "顶层规划/行动方案"
        if "试点" in body_text:
            return "试点推进阶段"
        if any(token in body_text for token in ("通知", "意见", "决定")):
            return "政策通知/执行部署"
        return "阶段待原文确认"

    def extract_timeline_points(self, text: str) -> List[str]:
        seen: List[str] = []
        for sentence in self._split_clauses(text):
            if any(re.search(pattern, sentence) for pattern in _TIMELINE_PATTERNS):
                cleaned = self._trim_sentence(sentence)
                if self._is_low_signal_fragment(cleaned):
                    continue
                if cleaned and cleaned not in seen:
                    seen.append(cleaned)
        return seen[:5]

    def extract_policy_facts(self, text: str, template: Dict[str, Any]) -> List[str]:
        candidates: List[Tuple[int, str]] = []
        fragments = self._merge_unique(self._split_clauses(text), self._split_sentences(text))
        for sentence in fragments:
            trimmed = self._trim_sentence(sentence)
            if self._is_low_signal_fragment(trimmed):
                continue
            score = self._score_fact_sentence(trimmed, template)
            if score >= 3:
                candidates.append((score, trimmed))
        if candidates:
            ranked: List[str] = []
            for _, sentence in sorted(candidates, key=lambda item: (-item[0], len(item[1]))):
                if any(sentence in existing or existing in sentence for existing in ranked):
                    continue
                if sentence not in ranked:
                    ranked.append(sentence)
                if len(ranked) >= 2:
                    break
            if ranked:
                return ranked

        primary_tokens = list(template.get("support_points", [])) + list(_SUPPORT_TOKENS)
        facts = self._find_sentences(text, primary_tokens, limit=2)
        if facts:
            return facts
        secondary_tokens = list(template.get("aliases", []))
        facts = self._find_sentences(text, secondary_tokens, limit=2)
        if facts:
            return facts
        sentences = [item for item in self._split_clauses(text) if not self._is_low_signal_fragment(item)]
        return [self._trim_sentence(sentences[0])] if sentences else []

    def extract_support_points(self, text: str, template: Dict[str, Any]) -> List[str]:
        text_lower = self._normalize_text(text).lower()
        from_text = [
            point
            for point in template.get("support_points", [])
            if str(point).strip() and str(point).lower() in text_lower
        ]
        if from_text:
            return from_text[:4]
        return list(template.get("support_points", []))[:4]

    def extract_beneficiary_nodes(self, text: str, template: Dict[str, Any]) -> List[str]:
        text_lower = self._normalize_text(text).lower()
        from_text = [
            node
            for node in template.get("beneficiary_nodes", [])
            if str(node).strip() and str(node).lower() in text_lower
        ]
        if from_text:
            return from_text[:4]
        return list(template.get("beneficiary_nodes", []))[:4]

    def extract_risk_sentences(self, text: str) -> List[str]:
        return self._find_sentences(text, _RISK_TOKENS, limit=2)

    def extract_risk_points(self, text: str, template: Dict[str, Any]) -> List[str]:
        return self._merge_unique(self.extract_risk_sentences(text), list(template.get("risk_nodes", [])))[:4]

    def watchlist_impact(self, policy: Dict[str, Any], holdings: Iterable[Dict[str, Any]]) -> List[str]:
        watchlist = load_watchlist()
        candidates = list(holdings) + watchlist
        impact_lines: List[str] = []
        beneficiary_nodes = set(policy.get("beneficiary_nodes", []))
        risk_nodes = set(policy.get("risk_nodes", []))
        mapped_assets = [str(item).strip().lower() for item in policy.get("mapped_assets", []) if str(item).strip()]

        seen = set()
        for item in candidates:
            symbol = item.get("symbol")
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            chain_nodes = set(item.get("chain_nodes", []))
            name = str(item.get("name", symbol))
            matched_asset = any(mapped == str(symbol).lower() or mapped in name.lower() for mapped in mapped_assets)
            beneficiary_match = chain_nodes & beneficiary_nodes
            risk_match = chain_nodes & risk_nodes
            if matched_asset or beneficiary_match:
                reason = " / ".join(sorted(beneficiary_match)) if beneficiary_match else "模板显式映射"
                impact_lines.append(f"{symbol} ({name}) 命中受益方向 `{reason}`，适合进入重点跟踪。")
            elif risk_match:
                reason = " / ".join(sorted(risk_match))
                impact_lines.append(f"{symbol} ({name}) 暴露在风险链条 `{reason}` 上，需要观察兑现节奏。")
        return impact_lines

    def _load_url_context(self, target: str) -> PolicyContext:
        response = requests.get(target, timeout=15)
        response.raise_for_status()
        content_type = str(response.headers.get("content-type", "")).lower()
        notes: List[str] = []
        if "ofd" in content_type or target.lower().endswith(".ofd"):
            authority = self._assess_source_authority(target, {})
            text, metadata, ofd_title = self._extract_ofd_text(response.content, target, notes)
            title = ofd_title or urlparse(target).path.rsplit("/", 1)[-1] or urlparse(target).netloc
            quality = "OFD正文已抽取" if text else "仅识别到 OFD 链接，正文未抽取"
            if text:
                notes.append("已抽取 OFD 原文，可用于正文事实、时间线和方向判断。")
            return PolicyContext(
                title=title,
                source=target,
                text=text,
                metadata=metadata,
                extraction_quality=quality,
                extraction_notes=notes or ["当前链接是 OFD 原件，正文仍未抽取。"],
                content_kind="url",
                source_authority=authority,
                coverage_scope=["OFD正文"] if text else ["仅识别到附件链接"],
                attachment_titles=[title or "OFD原件"],
            )
        if "pdf" in content_type or target.lower().endswith(".pdf"):
            authority = self._assess_source_authority(target, {})
            text, metadata, pdf_title = self._extract_pdf_text(response.content, target, notes)
            title = pdf_title or urlparse(target).path.rsplit("/", 1)[-1] or urlparse(target).netloc
            quality = "PDF正文已抽取" if text else "仅识别到 PDF 链接，正文未抽取"
            if text:
                notes.append("已抽取 PDF 原文，可用于正文事实、时间线和方向判断。")
            return PolicyContext(
                title=title,
                source=target,
                text=text,
                metadata=metadata,
                extraction_quality=quality,
                extraction_notes=notes or ["当前链接是 PDF 或类 PDF 原件，正文仍未抽取。"],
                content_kind="url",
                source_authority=authority,
                coverage_scope=["PDF正文"] if text else ["仅识别到附件链接"],
                attachment_titles=[title or "附件原件"],
            )

        html = self._decode_response_text(response, notes)
        soup = BeautifulSoup(html, "html.parser")
        self._strip_noise_tags(soup)
        metadata = self._extract_metadata(soup)
        attachments = self._extract_attachment_resources(soup, target)
        attachment_titles = [item.title for item in attachments]
        title = self._extract_html_title(soup, metadata, target)
        text, selector = self._extract_body_text(soup)
        if selector:
            notes.append(f"正文抽取来源 `{selector}`。")
        if not text and metadata:
            text = "\n".join(f"{label}：{value}" for label, value in metadata.items() if value)
        quality = "正文抽取较完整" if title and len(text) >= 120 else "正文抽取部分成功" if title or text else "仅抽到页面元信息"
        pdf_attachments = [item for item in attachments if item.kind == "pdf"]
        ofd_attachments = [item for item in attachments if item.kind == "ofd"]
        pdf_text = ""
        ofd_text = ""
        if pdf_attachments:
            pdf_text, pdf_metadata, pdf_title = self._extract_pdf_attachment_text(pdf_attachments[0], notes)
            if pdf_text:
                text = self._merge_unique_text(text, pdf_text)
                metadata = self._merge_metadata(metadata, pdf_metadata)
                quality = "公告页正文 + PDF附件已补抽"
                notes.append(f"已补抽 PDF 附件正文 `{pdf_attachments[0].title}`。")
            else:
                notes.append(f"检测到 PDF 附件（{pdf_attachments[0].title}），但当前未抽到可用正文。")
        if ofd_attachments:
            ofd_text, ofd_metadata, ofd_title = self._extract_ofd_attachment_text(ofd_attachments[0], notes)
            if ofd_text:
                text = self._merge_unique_text(text, ofd_text)
                metadata = self._merge_metadata(metadata, ofd_metadata)
                if quality == "公告页正文 + PDF附件已补抽":
                    quality = "公告页正文 + PDF/OFD附件已补抽"
                else:
                    quality = "公告页正文 + OFD附件已补抽"
                notes.append(f"已补抽 OFD 附件正文 `{ofd_attachments[0].title}`。")
            else:
                notes.append(f"检测到 OFD 附件（{ofd_attachments[0].title}），但当前未抽到可用正文。")
        if attachment_titles:
            if not (pdf_text or ofd_text):
                quality = "正文抽取部分成功"
            notes.append(f"检测到 PDF/OFD 附件（{'; '.join(attachment_titles[:2])}），当前只抽取了公告页正文，未展开全部附件原文。")
        if not text:
            notes.append("当前没有抽到足够正文，只能基于标题和元信息做弱判断。")
        source_authority = self._assess_source_authority(target, metadata)
        coverage_scope = self._build_coverage_scope(
            title,
            metadata,
            text,
            attachment_titles,
            pdf_text_extracted=bool(pdf_text),
            ofd_text_extracted=bool(ofd_text),
        )
        return PolicyContext(
            title=title,
            source=target,
            text=text,
            metadata=metadata,
            extraction_quality=quality,
            extraction_notes=notes,
            content_kind="url",
            body_selector=selector,
            source_authority=source_authority,
            coverage_scope=coverage_scope,
            attachment_titles=attachment_titles,
        )

    def _decode_response_text(self, response: requests.Response, notes: List[str]) -> str:
        apparent = getattr(response, "apparent_encoding", None)
        current = getattr(response, "encoding", None)
        if current in (None, "", "ISO-8859-1", "ascii") and apparent:
            response.encoding = apparent
            notes.append(f"网页编码按 `{apparent}` 自动修正。")
        text = response.text
        if self._looks_like_mojibake(text) and apparent and response.encoding != apparent:
            response.encoding = apparent
            text = response.text
            notes.append(f"检测到乱码后回退到 `{apparent}` 重解码。")
        return text

    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, str]:
        blocks: List[str] = []
        for selector in (".policyLibraryOverview_content", ".pages-date", ".article-info", ".detail-info", ".meta", ".info"):
            for node in soup.select(selector):
                text = self._normalize_text(node.get_text(" ", strip=True))
                if text and text not in blocks:
                    blocks.append(text)

        merged = self._normalize_metadata_block(" ".join(blocks))
        metadata: Dict[str, str] = {}
        if merged:
            for label in _METADATA_LABELS:
                value = self._extract_labeled_field(merged, label)
                if value:
                    metadata[label] = value

        if not metadata and soup.title and soup.title.text:
            metadata["标题"] = self._normalize_title(soup.title.text)
        return metadata

    def _extract_html_title(self, soup: BeautifulSoup, metadata: Dict[str, str], target: str) -> str:
        if metadata.get("标题"):
            return metadata["标题"]

        for selector in ("h1", ".pages-title", ".article h1", ".detail_title", ".content h1"):
            node = soup.select_one(selector)
            if node:
                text = self._normalize_title(node.get_text(" ", strip=True))
                if text:
                    return text

        if soup.title and soup.title.text:
            return self._normalize_title(soup.title.text)
        return urlparse(target).netloc

    def _extract_attachment_resources(self, soup: BeautifulSoup, target: str) -> List[AttachmentResource]:
        attachments: List[AttachmentResource] = []
        for node in soup.select("a[href]"):
            href = str(node.get("href", "")).strip()
            lowered = href.lower()
            if not lowered.endswith((".pdf", ".ofd")):
                continue
            title = self._normalize_text(node.get_text(" ", strip=True))
            if not title:
                filename = href.rsplit("/", 1)[-1]
                title = filename or urlparse(target).path.rsplit("/", 1)[-1]
            resource = AttachmentResource(
                title=title,
                url=urljoin(target, href),
                kind="pdf" if lowered.endswith(".pdf") else "ofd",
            )
            if resource.title and not any(
                item.title == resource.title and item.url == resource.url for item in attachments
            ):
                attachments.append(resource)
        return attachments[:4]

    def _extract_attachment_titles(self, soup: BeautifulSoup, target: str) -> List[str]:
        return [item.title for item in self._extract_attachment_resources(soup, target)]

    def _extract_pdf_attachment_text(
        self,
        attachment: AttachmentResource,
        notes: List[str],
    ) -> Tuple[str, Dict[str, str], str]:
        try:
            response = requests.get(attachment.url, timeout=20)
            response.raise_for_status()
        except Exception as exc:
            notes.append(f"尝试补抽 PDF 附件 `{attachment.title}` 失败：{exc.__class__.__name__}。")
            return "", {}, attachment.title
        text, metadata, pdf_title = self._extract_pdf_text(response.content, attachment.url, notes)
        return text, metadata, pdf_title or attachment.title

    def _extract_ofd_attachment_text(
        self,
        attachment: AttachmentResource,
        notes: List[str],
    ) -> Tuple[str, Dict[str, str], str]:
        try:
            response = requests.get(attachment.url, timeout=20)
            response.raise_for_status()
        except Exception as exc:
            notes.append(f"尝试补抽 OFD 附件 `{attachment.title}` 失败：{exc.__class__.__name__}。")
            return "", {}, attachment.title
        text, metadata, ofd_title = self._extract_ofd_text(response.content, attachment.url, notes)
        return text, metadata, ofd_title or attachment.title

    def _extract_pdf_text(
        self,
        content: bytes,
        source: str,
        notes: List[str],
    ) -> Tuple[str, Dict[str, str], str]:
        try:
            from pypdf import PdfReader
        except Exception:
            notes.append("当前环境未安装 `pypdf`，PDF 原文仍未抽取。")
            return "", {}, ""

        try:
            reader = PdfReader(io.BytesIO(content))
        except Exception as exc:
            notes.append(f"PDF 原文抽取失败：{exc.__class__.__name__}。")
            return "", {}, ""

        text_blocks: List[str] = []
        page_limit = min(len(reader.pages), 8)
        for idx in range(page_limit):
            try:
                page_text = self._clean_extracted_text(reader.pages[idx].extract_text() or "")
            except Exception:
                page_text = ""
            if page_text:
                text_blocks.append(page_text)
        if len(reader.pages) > page_limit:
            notes.append(f"PDF 共 {len(reader.pages)} 页，当前只抽取前 {page_limit} 页。")
        if not text_blocks:
            notes.append("PDF 可能是扫描版或图片版，当前没有抽到可用文本。")
        metadata: Dict[str, str] = {}
        title = ""
        pdf_meta = getattr(reader, "metadata", None)
        if pdf_meta:
            title = self._normalize_text(getattr(pdf_meta, "title", "") or pdf_meta.get("/Title", ""))
            if title:
                metadata["标题"] = title
            author = self._normalize_text(getattr(pdf_meta, "author", "") or pdf_meta.get("/Author", ""))
            if author and author.lower() not in {"user", "anonymous", "acrobat", "microsoft", "wps"}:
                metadata["来源"] = author
        if not title:
            title = urlparse(source).path.rsplit("/", 1)[-1]
        return "\n".join(text_blocks).strip(), metadata, title

    def _extract_ofd_text(
        self,
        content: bytes,
        source: str,
        notes: List[str],
    ) -> Tuple[str, Dict[str, str], str]:
        try:
            archive = zipfile.ZipFile(io.BytesIO(content))
        except Exception as exc:
            notes.append(f"OFD 原文抽取失败：{exc.__class__.__name__}。")
            return "", {}, ""

        metadata: Dict[str, str] = {}
        title = ""
        doc_root = "Doc_0/Document.xml"
        try:
            ofd_root = ET.fromstring(archive.read("OFD.xml"))
            doc_info = ofd_root.find(".//ofd:DocInfo", self._OFD_NS)
            if doc_info is not None:
                author = self._normalize_text((doc_info.findtext("ofd:Author", "", self._OFD_NS) or ""))
                creation = self._normalize_text((doc_info.findtext("ofd:CreationDate", "", self._OFD_NS) or ""))
                creator = self._normalize_text((doc_info.findtext("ofd:Creator", "", self._OFD_NS) or ""))
                if author and author.lower() not in {"user", "anonymous", "wps"}:
                    metadata["来源"] = author
                if creation:
                    metadata["成文日期"] = creation
                if creator and creator.lower() not in {"wps 文字"}:
                    metadata["公文种类"] = creator
            doc_root = (
                ofd_root.findtext(".//ofd:DocRoot", default=doc_root, namespaces=self._OFD_NS)
                or doc_root
            )
        except Exception:
            notes.append("OFD 根信息不完整，当前按默认文档结构回退。")

        try:
            document_root = ET.fromstring(archive.read(doc_root))
        except Exception as exc:
            notes.append(f"OFD 文档结构读取失败：{exc.__class__.__name__}。")
            return "", metadata, ""

        page_paths: List[str] = []
        base_dir = doc_root.rsplit("/", 1)[0] if "/" in doc_root else ""
        for page in document_root.findall(".//ofd:Page", self._OFD_NS):
            base_loc = self._normalize_text(page.attrib.get("BaseLoc", ""))
            if not base_loc:
                continue
            full_path = f"{base_dir}/{base_loc}" if base_dir and not base_loc.startswith(base_dir) else base_loc
            page_paths.append(full_path)

        page_texts: List[str] = []
        for page_path in page_paths[:12]:
            page_text = self._extract_ofd_page_text(archive, page_path)
            if page_text:
                page_texts.append(page_text)
        if len(page_paths) > 12:
            notes.append(f"OFD 共 {len(page_paths)} 页，当前只抽取前 12 页。")
        text = "\n".join(page_texts).strip()
        if not text:
            notes.append("OFD 可能包含复杂矢量对象或异常版式，当前没有抽到可用文本。")
            return "", metadata, ""

        title = self._derive_document_title(text, fallback=urlparse(source).path.rsplit("/", 1)[-1])
        if title:
            metadata.setdefault("标题", title)
        return text, metadata, title

    def _extract_ofd_page_text(self, archive: zipfile.ZipFile, page_path: str) -> str:
        try:
            root = ET.fromstring(archive.read(page_path))
        except Exception:
            return ""

        rows: List[Tuple[float, float, str]] = []
        for text_obj in root.findall(".//ofd:TextObject", self._OFD_NS):
            boundary = self._normalize_text(text_obj.attrib.get("Boundary", ""))
            x, y = self._parse_ofd_boundary(boundary)
            text_parts = [self._normalize_text(node.text) for node in text_obj.findall(".//ofd:TextCode", self._OFD_NS)]
            text = "".join(part for part in text_parts if part)
            if text:
                rows.append((y, x, text))
        if not rows:
            return ""

        lines: List[Tuple[float, List[Tuple[float, str]]]] = []
        for y, x, text in sorted(rows, key=lambda item: (round(item[0], 1), item[1])):
            if not lines or abs(lines[-1][0] - y) > 0.8:
                lines.append((y, [(x, text)]))
            else:
                lines[-1][1].append((x, text))
        rendered_lines: List[str] = []
        for _, line_items in lines:
            merged = "".join(text for _, text in sorted(line_items, key=lambda item: item[0]))
            cleaned = self._normalize_text(merged)
            if cleaned and not self._is_noise_line(cleaned):
                rendered_lines.append(cleaned)
        return "\n".join(rendered_lines)

    def _parse_ofd_boundary(self, boundary: str) -> Tuple[float, float]:
        parts = boundary.split()
        if len(parts) >= 2:
            try:
                return float(parts[0]), float(parts[1])
            except Exception:
                return 0.0, 0.0
        return 0.0, 0.0

    def _derive_document_title(self, text: str, fallback: str = "") -> str:
        for line in self._split_sentences(text)[:8]:
            normalized = self._normalize_title(line)
            if 4 <= len(normalized) <= 42 and not re.fullmatch(r"[—\-\d（）()年 ]+", normalized):
                return normalized
        return self._normalize_title(fallback)

    def _extract_body_text(self, soup: BeautifulSoup) -> Tuple[str, str]:
        candidates: List[Tuple[str, str, int]] = []
        seen_text = set()
        for selector in _HTML_BODY_SELECTORS:
            for node in soup.select(selector):
                text = self._clean_extracted_text(node.get_text("\n", strip=True))
                if len(text) < 80:
                    continue
                normalized_key = re.sub(r"\s+", "", text)
                if normalized_key in seen_text:
                    continue
                seen_text.add(normalized_key)
                score = self._score_text_block(text)
                candidates.append((selector, text, score))

        if not candidates:
            for node in soup.find_all(["div", "section", "article"]):
                text = self._clean_extracted_text(node.get_text("\n", strip=True))
                if len(text) < 120:
                    continue
                score = self._score_text_block(text)
                candidates.append((node.name, text, score))

        if not candidates:
            return "", ""

        selector, text, _ = max(candidates, key=lambda item: item[2])
        return text, selector

    def _score_text_block(self, text: str) -> int:
        lowered = text.lower()
        policy_hits = sum(lowered.count(token) for token in ("通知", "方案", "意见", "要求", "推进", "建设", "实施", "发文机关"))
        timeline_hits = sum(1 for pattern in _TIMELINE_PATTERNS if re.search(pattern, text))
        noise_hits = sum(lowered.count(token.lower()) for token in _NOISE_TOKENS)
        return len(text) + policy_hits * 60 + timeline_hits * 50 - noise_hits * 120

    def _strip_noise_tags(self, soup: BeautifulSoup) -> None:
        for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
            tag.decompose()

    def _clean_extracted_text(self, text: str) -> str:
        lines: List[str] = []
        seen = set()
        for raw_line in re.split(r"[\r\n]+", unescape(text)):
            line = self._normalize_text(raw_line)
            if not line or line in seen:
                continue
            seen.add(line)
            if self._is_noise_line(line):
                continue
            lines.append(line)
        return "\n".join(lines)

    def _is_noise_line(self, line: str) -> bool:
        lowered = line.lower()
        noise_hits = sum(1 for token in _NOISE_TOKENS if token in line)
        if noise_hits >= 2:
            return True
        if lowered.endswith((".pdf", ".ofd")):
            return True
        if line.startswith(("字号", "打印", "收藏", "留言", "分享")):
            return True
        return False

    def _extract_labeled_field(self, text: str, label: str) -> str:
        escaped_label = re.escape(label).replace("\\ ", r"\s*")
        label_pattern = "|".join(re.escape(item).replace("\\ ", r"\s*") for item in _METADATA_LABELS)
        pattern = rf"{escaped_label}\s*[：:]\s*(.+?)(?=(?:{label_pattern})\s*[：:]|$)"
        match = re.search(pattern, text)
        if not match:
            return ""
        return self._normalize_text(match.group(1))

    def _normalize_text(self, text: Any) -> str:
        cleaned = unescape(str(text or ""))
        cleaned = cleaned.replace("\xa0", " ").replace("\u3000", " ")
        cleaned = re.sub(r"[ \t\f\v]+", " ", cleaned)
        cleaned = re.sub(r"\r\n?", "\n", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def _normalize_metadata_block(self, text: str) -> str:
        normalized = self._normalize_text(text)
        replacements = {
            "标 题": "标题",
            "发文机关": "发文机关",
            "发文字号": "发文字号",
            "来 源": "来源",
            "主题分类": "主题分类",
            "成文日期": "成文日期",
            "发布日期": "发布日期",
            "公文种类": "公文种类",
        }
        for source, target in replacements.items():
            normalized = normalized.replace(source, target)
        return normalized.replace("\n", " ")

    def _normalize_title(self, title: str) -> str:
        cleaned = self._normalize_text(title)
        cleaned = re.sub(r"[_|｜-](?:国务院.*|中国政府网|.*官网)$", "", cleaned).strip()
        return cleaned

    def _looks_like_long_text(self, text: str) -> bool:
        if len(text) >= 80:
            return True
        punctuation_hits = sum(text.count(token) for token in ("。", "；", "：", "\n"))
        return len(text) >= 40 and punctuation_hits >= 2

    def _derive_text_title(self, text: str) -> str:
        first_sentence = self._split_sentences(text)
        if not first_sentence:
            return text[:36]
        sentence = first_sentence[0]
        return sentence if len(sentence) <= 40 else sentence[:40] + "..."

    def _looks_like_mojibake(self, text: str) -> bool:
        markers = ("å", "ç", "ä", "é", "ï¼", "ã")
        return sum(text.count(marker) for marker in markers) >= 6

    def _split_sentences(self, text: str) -> List[str]:
        normalized = self._normalize_text(text)
        if not normalized:
            return []
        parts = re.split(r"(?<=[。！？；;])|\n+", normalized)
        sentences = [self._trim_sentence(part) for part in parts if self._trim_sentence(part)]
        return sentences

    def _split_clauses(self, text: str) -> List[str]:
        clauses: List[str] = []
        for sentence in self._split_sentences(text):
            parts = re.split(r"[，,]", sentence)
            for part in parts:
                cleaned = self._trim_sentence(part, max_length=80)
                if cleaned:
                    clauses.append(cleaned)
        return clauses or self._split_sentences(text)

    def _trim_sentence(self, text: str, max_length: int = 120) -> str:
        cleaned = self._normalize_text(text).strip(" ：:;；，,。")
        if len(cleaned) <= max_length:
            return cleaned
        return cleaned[: max_length - 1] + "…"

    def _find_sentences(self, text: str, tokens: Sequence[str], limit: int = 2) -> List[str]:
        sentences = self._split_clauses(text)
        matched: List[str] = []
        lowered_tokens = [str(token).lower() for token in tokens if str(token).strip()]
        for sentence in sentences:
            sentence_lower = sentence.lower()
            if self._is_low_signal_fragment(sentence):
                continue
            if any(token in sentence_lower for token in lowered_tokens):
                trimmed = self._trim_sentence(sentence)
                if trimmed not in matched:
                    matched.append(trimmed)
            if len(matched) >= limit:
                break
        return matched

    def _score_fact_sentence(self, sentence: str, template: Dict[str, Any]) -> int:
        normalized = self._normalize_text(sentence)
        lowered = normalized.lower()
        if re.fullmatch(r"\d{4}年\d{1,2}月\d{1,2}日", normalized):
            return -5
        action_hits = sum(lowered.count(token) for token in ("提出", "要求", "推进", "加快", "建设", "实施", "形成", "启动", "申报"))
        support_hits = sum(lowered.count(str(item).lower()) for item in template.get("support_points", []))
        alias_hits = sum(lowered.count(str(item).lower()) for item in template.get("aliases", []))
        beneficiary_hits = sum(lowered.count(str(item).lower()) for item in template.get("beneficiary_nodes", []))
        timeline_hits = sum(1 for pattern in _TIMELINE_PATTERNS if re.search(pattern, normalized))
        restrict_hits = sum(lowered.count(token) for token in _RESTRICT_TOKENS)
        boilerplate_hits = sum(
            lowered.count(token)
            for token in (
                "贯彻落实",
                "总书记",
                "党中央",
                "国务院办公厅",
                "有关要求",
                "重要指示精神",
                "指导思想",
                "现印发给你们",
                "请认真组织实施",
                "因地制宜",
            )
        )
        return (
            action_hits * 4
            + support_hits * 6
            + alias_hits * 3
            + beneficiary_hits * 3
            + timeline_hits * 5
            + restrict_hits * 2
            + min(len(normalized) // 25, 4)
            - boilerplate_hits * 4
        )

    def _is_low_signal_fragment(self, text: str) -> bool:
        lowered = text.lower()
        if len(text) <= 4:
            return True
        if lowered.endswith((".pdf", ".ofd")):
            return True
        if re.fullmatch(r"\d{4}年\d{1,2}月\d{1,2}日", text):
            return True
        if text in {"现印发给你们", "请认真组织实施"}:
            return True
        if any(token in text for token in ("贯彻落实习近平总书记", "有关要求")) and len(text) <= 80:
            return True
        if "关于印发《" in text and "通知" in text:
            return True
        if text.startswith(("各省、自治区", "附件", "相关阅读")):
            return True
        if text.endswith(("通知", "意见", "方案")) and len(text) <= 28:
            return True
        return False

    def _direction_signal_sentences(self, text: str) -> Tuple[List[str], List[str]]:
        support = self._find_sentences(text, _SUPPORT_TOKENS, limit=2)
        restrict = self._find_sentences(text, _RESTRICT_TOKENS, limit=2)
        return support, restrict

    def _direction_reason(self, support: List[str], restrict: List[str]) -> str:
        if support and restrict:
            return f"正文同时出现推进措辞（如“{support[0]}”）和约束措辞（如“{restrict[0]}”），整体仍以支持性部署为主。"
        if support:
            return f"正文明确出现支持/推进措辞，例如“{support[0]}”。"
        if restrict:
            return f"正文主要出现约束/规范措辞，例如“{restrict[0]}”。"
        return "当前缺少足够正文信号，只能依赖标题词和模板进行弱判断。"

    def _stage_reason(self, title: str, text: str, stage: str) -> str:
        normalized_title = self._normalize_text(title)
        if normalized_title:
            return f"标题中含有“{normalized_title}”对应的文件类型线索。"
        if text:
            return f"正文出现与 `{stage}` 对应的部署词。"
        return "当前没有足够标题或正文信号。"

    def _default_template(self, context: PolicyContext) -> Dict[str, Any]:
        return {
            "name": context.title,
            "policy_goal": "从原文中未匹配到现成模板，当前使用通用结构化输出。",
            "timeline": "需要结合后续细则和项目进度跟踪。",
            "support_points": ["原文需人工复核重点支持方向"],
            "beneficiary_nodes": [],
            "risk_nodes": ["落地节奏不确定"],
            "mapped_assets": [],
            "headline_numbers": [],
        }

    def _build_summary(
        self,
        *,
        context: PolicyContext,
        direction: str,
        stage: str,
        template: Dict[str, Any],
        support_points: List[str],
        timeline_points: List[str],
    ) -> str:
        if context.content_kind == "keyword":
            return (
                f"当前更像 `{template.get('name', context.title)}` 这条政策主题，方向先按 `{direction}` 理解，"
                "但正文事实、时间线和发文机关仍待具体文件确认。"
            )
        support_text = "、".join(support_points[:2]) if support_points else template.get("policy_goal", "")
        timeline_text = f"，并已抽到 `{timeline_points[0]}` 这类时间线" if timeline_points else ""
        attachment_text = ""
        if context.attachment_titles and not context.source.lower().endswith((".pdf", ".ofd")):
            attachment_text = f"，页面还挂有 `{context.attachment_titles[0]}` 这类附件但未展开原文"
        return (
            f"原文主要围绕 `{support_text}` 展开，当前判断为 `{direction}`，阶段偏 `{stage}`"
            f"{timeline_text}{attachment_text}。"
        )

    def _build_timeline_summary(
        self,
        title: str,
        template: Dict[str, Any],
        timeline_points: List[str],
    ) -> str:
        title_year_range = re.search(r"\d{4}[—-]\d{4}年", title)
        parts: List[str] = []
        if title_year_range:
            parts.append(f"标题已给出 `{title_year_range.group(0)}` 的规划区间。")
        if timeline_points:
            parts.append(f"正文时间线抓手包括 `{timeline_points[0]}`。")
        if not parts:
            parts.append(str(template.get("timeline", "")))
        return " ".join(part for part in parts if part).strip()

    def _describe_context_kind(self, context: PolicyContext) -> str:
        if context.content_kind == "url":
            if context.source.lower().endswith(".pdf"):
                if "官方" in context.source_authority or "政府" in context.source_authority:
                    return "官方 PDF / URL"
                return "PDF / URL"
            if context.source.lower().endswith(".ofd"):
                if "官方" in context.source_authority or "政府" in context.source_authority:
                    return "官方 OFD / URL"
                return "OFD / URL"
            if "官方" in context.source_authority or "政府" in context.source_authority:
                return "官方页面 / URL"
            return "网页 / URL"
        if context.content_kind == "text":
            return "用户提供长正文"
        return "关键词"

    def _build_excerpt(self, text: str, limit: int = 220) -> str:
        cleaned = self._normalize_text(text).replace("\n", " ")
        return cleaned[:limit] + ("..." if len(cleaned) > limit else "")

    def _merge_unique(self, *groups: Sequence[str]) -> List[str]:
        merged: List[str] = []
        seen = set()
        for group in groups:
            for item in group:
                value = str(item).strip()
                if not value or value in seen:
                    continue
                seen.add(value)
                merged.append(value)
        return merged

    def _merge_unique_text(self, left: str, right: str) -> str:
        left_norm = self._normalize_text(left)
        right_norm = self._normalize_text(right)
        if not left_norm:
            return right_norm
        if not right_norm or right_norm in left_norm:
            return left_norm
        if left_norm in right_norm:
            return right_norm
        return f"{left_norm}\n{right_norm}".strip()

    def _merge_metadata(self, primary: Dict[str, str], secondary: Dict[str, str]) -> Dict[str, str]:
        merged = dict(primary)
        for key, value in secondary.items():
            normalized = self._normalize_text(value)
            if normalized and not merged.get(key):
                merged[key] = normalized
        return merged

    def _assess_source_authority(self, target: str, metadata: Dict[str, str]) -> str:
        domain = urlparse(target).netloc.lower()
        issuer = self._normalize_text(metadata.get("发文机关", ""))
        source = self._normalize_text(metadata.get("来源", ""))
        if domain.endswith(".gov.cn") or domain.endswith(".gov"):
            if issuer:
                return "官方政府站点，且页面含发文机关元信息"
            if source:
                return "官方政府站点，来源字段已抽到"
            return "官方政府站点"
        if issuer or source:
            return "机构页面，含来源/发文机关线索"
        if domain:
            return "普通网页来源，未确认是否官方原发"
        return "待确认"

    def _build_coverage_scope(
        self,
        title: str,
        metadata: Dict[str, str],
        text: str,
        attachment_titles: Sequence[str],
        *,
        pdf_text_extracted: bool = False,
        ofd_text_extracted: bool = False,
    ) -> List[str]:
        scope: List[str] = []
        if title:
            scope.append("标题")
        if metadata:
            scope.append(f"元信息（{min(len(metadata), 4)}项）")
        if text:
            scope.append("页面正文")
        if pdf_text_extracted:
            scope.append("PDF附件正文")
        if ofd_text_extracted:
            scope.append("OFD附件正文")
        if attachment_titles:
            scope.append(f"附件标题（{len(attachment_titles)}个）")
        return scope or ["覆盖范围待确认"]

    def _build_policy_taxonomy(
        self,
        *,
        context: PolicyContext,
        template: Dict[str, Any],
        direction: str,
        stage: str,
    ) -> Dict[str, str]:
        taxonomy = dict(template.get("taxonomy") or {})
        source_bucket = "中央/官方原发" if "官方" in context.source_authority or "政府" in context.source_authority else (
            "用户提供正文" if context.content_kind == "text" else "主题关键词/待确认"
        )
        taxonomy.setdefault("source_level", source_bucket)
        taxonomy.setdefault(
            "evidence_mode",
            "已覆盖正文"
            if any(item in {"页面正文", "PDF正文", "PDF附件正文", "OFD正文", "OFD附件正文"} for item in context.coverage_scope)
            else "主题推断",
        )
        taxonomy["policy_tone"] = direction
        taxonomy["policy_stage"] = stage
        return {key: value for key, value in taxonomy.items() if str(value).strip()}
