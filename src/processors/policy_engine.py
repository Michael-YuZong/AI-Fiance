"""Policy keyword matching and heuristic parsing."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

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


@dataclass
class PolicyTemplateMatch:
    template: Dict[str, Any]
    score: int
    matched_aliases: List[str]
    confidence_label: str


class PolicyEngine:
    """Heuristic policy analysis without requiring an LLM key."""

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
            )
        return PolicyContext(
            title=normalized_target,
            source="keyword",
            text=normalized_target,
            extraction_quality="仅关键词，无正文事实",
            extraction_notes=["当前输入为关键词，正文事实、时间线和发文机关都未确认。"],
            content_kind="keyword",
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
        if context.content_kind == "url" and context.extraction_quality != "正文抽取较完整":
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
            "inference_lines": inference_lines,
            "unconfirmed_lines": self._merge_unique(unconfirmed_lines),
            "extraction_status": context.extraction_quality,
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
        if "pdf" in content_type or target.lower().endswith(".pdf"):
            return PolicyContext(
                title=urlparse(target).path.rsplit("/", 1)[-1] or urlparse(target).netloc,
                source=target,
                text="",
                extraction_quality="仅识别到 PDF 链接，正文未抽取",
                extraction_notes=["当前链接是 PDF 或类 PDF 原件，项目内暂未接入稳定的 PDF 正文抽取。"],
                content_kind="url",
            )

        html = self._decode_response_text(response, notes)
        soup = BeautifulSoup(html, "html.parser")
        self._strip_noise_tags(soup)
        metadata = self._extract_metadata(soup)
        attachment_links = [
            href
            for href in (node.get("href", "") for node in soup.select("a[href]"))
            if str(href).lower().endswith((".pdf", ".ofd"))
        ]
        title = self._extract_html_title(soup, metadata, target)
        text, selector = self._extract_body_text(soup)
        if selector:
            notes.append(f"正文抽取来源 `{selector}`。")
        if not text and metadata:
            text = "\n".join(f"{label}：{value}" for label, value in metadata.items() if value)
        quality = "正文抽取较完整" if title and len(text) >= 120 else "正文抽取部分成功" if title or text else "仅抽到页面元信息"
        if attachment_links:
            quality = "正文抽取部分成功"
            notes.append("检测到 PDF/OFD 附件，当前只抽取了公告页正文，未展开附件原文。")
        if not text:
            notes.append("当前没有抽到足够正文，只能基于标题和元信息做弱判断。")
        return PolicyContext(
            title=title,
            source=target,
            text=text,
            metadata=metadata,
            extraction_quality=quality,
            extraction_notes=notes,
            content_kind="url",
            body_selector=selector,
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

    def _is_low_signal_fragment(self, text: str) -> bool:
        lowered = text.lower()
        if len(text) <= 4:
            return True
        if lowered.endswith((".pdf", ".ofd")):
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
        return f"原文主要围绕 `{support_text}` 展开，当前判断为 `{direction}`，阶段偏 `{stage}`{timeline_text}。"

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
            return "官方页面 / URL"
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
