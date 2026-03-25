"""Tests for src/advisor.py — Claude API wrapper for options advice."""

from unittest.mock import MagicMock, patch

import pytest

from src.advisor import (
    SYSTEM_PROMPT,
    ask_advisor,
    build_messages,
)


class TestSystemPrompt:
    def test_contains_key_instructions(self):
        assert "options" in SYSTEM_PROMPT.lower()
        assert "portfolio" in SYSTEM_PROMPT.lower() or "position" in SYSTEM_PROMPT.lower()

    def test_warns_not_financial_advice(self):
        assert "not financial advice" in SYSTEM_PROMPT.lower() or "educational" in SYSTEM_PROMPT.lower()

    def test_requires_profit_target_and_stop_loss(self):
        prompt_lower = SYSTEM_PROMPT.lower()
        assert "profit target" in prompt_lower
        assert "stop-loss" in prompt_lower


class TestBuildMessages:
    def test_first_message(self):
        context = "Portfolio: 5x AAPL calls"
        question = "Should I roll?"
        history = []

        messages = build_messages(context, question, history)

        # First message should include context
        assert len(messages) >= 1
        assert any("Portfolio" in m["content"] for m in messages if isinstance(m["content"], str))
        # Last message is the user question
        assert messages[-1]["role"] == "user"
        assert "roll" in messages[-1]["content"].lower()

    def test_with_history(self):
        context = "Portfolio: 5x AAPL calls"
        question = "What about puts instead?"
        history = [
            {"role": "user", "content": "Should I roll?"},
            {"role": "assistant", "content": "Consider the DTE..."},
        ]

        messages = build_messages(context, question, history)

        # Should include: context+first_q, assistant reply, new question
        assert len(messages) >= 3
        assert messages[-1]["content"] == "What about puts instead?"

    def test_context_only_in_first_message(self):
        context = "Portfolio: 5x AAPL calls"
        question = "Follow up"
        history = [
            {"role": "user", "content": "First question"},
            {"role": "assistant", "content": "First answer"},
        ]

        messages = build_messages(context, question, history)

        # Context should be in first user message only
        context_count = sum(
            1 for m in messages
            if isinstance(m["content"], str) and "Portfolio: 5x AAPL calls" in m["content"]
        )
        assert context_count == 1


class TestAskAdvisor:
    @patch("src.advisor.anthropic")
    def test_returns_response_text(self, mock_anthropic):
        mock_client = MagicMock()
        mock_anthropic.Anthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="Consider rolling to next month.")]
        mock_client.messages.create.return_value = mock_response

        result = ask_advisor(
            context="Portfolio: 5x AAPL calls",
            question="Should I roll?",
            history=[],
            api_key="test-key",
        )

        assert result == "Consider rolling to next month."
        mock_client.messages.create.assert_called_once()

    @patch("src.advisor.anthropic")
    def test_passes_system_prompt(self, mock_anthropic):
        mock_client = MagicMock()
        mock_anthropic.Anthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="Answer")]
        mock_client.messages.create.return_value = mock_response

        ask_advisor("ctx", "question", [], api_key="test-key")

        call_kwargs = mock_client.messages.create.call_args[1]
        assert call_kwargs["system"] == SYSTEM_PROMPT

    @patch("src.advisor.anthropic")
    def test_api_error_raises(self, mock_anthropic):
        mock_client = MagicMock()
        mock_anthropic.Anthropic.return_value = mock_client
        mock_client.messages.create.side_effect = Exception("API error")

        with pytest.raises(Exception, match="API error"):
            ask_advisor("ctx", "q", [], api_key="test-key")
