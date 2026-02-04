"""Tests for discord_archive.rag.chunking.tokenizer."""

from __future__ import annotations

from discord_archive.rag.chunking.tokenizer import estimate_tokens, get_tokenizer


class TestGetTokenizer:
    """Tests for the tokenizer getter."""

    def test_returns_tokenizer(self) -> None:
        tokenizer = get_tokenizer()
        assert tokenizer is not None

    def test_cached(self) -> None:
        t1 = get_tokenizer()
        t2 = get_tokenizer()
        assert t1 is t2


class TestEstimateTokens:
    """Tests for the token estimation function."""

    def test_empty_string_returns_zero(self) -> None:
        assert estimate_tokens("") == 0

    def test_hello_world(self) -> None:
        result = estimate_tokens("hello world")
        assert result > 0

    def test_longer_text_has_more_tokens(self) -> None:
        short = estimate_tokens("hi")
        long = estimate_tokens("hello world, this is a longer sentence")
        assert long > short
