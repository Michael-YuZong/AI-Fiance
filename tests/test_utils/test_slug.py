from src.utils.slug import ascii_slug


def test_ascii_slug_keeps_ascii_input_readable() -> None:
    assert ascii_slug("semiconductor ai", fallback_prefix="theme") == "semiconductor_ai"


def test_ascii_slug_turns_chinese_into_ascii_hash_slug() -> None:
    slug = ascii_slug("半导体", fallback_prefix="theme")

    assert slug.isascii()
    assert slug.startswith("theme_")
    assert len(slug) > len("theme_")


def test_ascii_slug_keeps_mixed_input_ascii_and_stable_hash() -> None:
    slug = ascii_slug("AI硬件链", fallback_prefix="theme")

    assert slug.isascii()
    assert slug.startswith("ai_")
