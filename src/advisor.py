"""Claude API wrapper for the Options Advisor.

Sends portfolio context + user questions to the Anthropic API
and returns strategy recommendations.
"""

import logging

import anthropic

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are an experienced options trading advisor integrated into an IBKR Trade Journal.
The user will share their current portfolio positions, fundamentals, and technical data.
Your role is to:

1. **Analyse positions**: Evaluate each options position considering DTE, moneyness, \
breakeven distance, and the underlying's technicals/fundamentals.
2. **Exit strategies**: For each position, recommend whether to hold, roll, close, \
or adjust — with clear reasoning.
3. **Strategy suggestions**: When asked, suggest options strategies (covered calls, \
spreads, cash-secured puts, etc.) that fit the user's existing portfolio.
4. **Risk awareness**: Flag positions with high risk (near expiry, deep OTM, \
large unrealized losses) and explain the risk.
5. **Volatility context**: Use the provided realized volatility (or user-overridden \
implied volatility) to inform strategy recommendations.

Important guidelines:
- This is for educational purposes only and is not financial advice.
- Always explain your reasoning.
- Be specific — reference actual position data, strikes, dates, and prices.
- When volatility override is provided, treat it as the user's implied volatility \
estimate and factor it into your analysis.
- If data is missing or insufficient, say so rather than guessing.
- Keep responses focused and actionable.
"""

MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 4096


def build_messages(
    context: str,
    question: str,
    history: list[dict],
) -> list[dict]:
    """Build the messages array for the API call.

    The portfolio context is prepended to the first user message.
    Subsequent messages in history are passed through as-is.
    """
    messages: list[dict] = []

    # First message always includes context
    context_message = (
        f"Here is my current portfolio:\n\n{context}\n\n"
        f"---\n\n{question}"
    )

    if not history:
        messages.append({"role": "user", "content": context_message})
    else:
        # Reconstruct: context + first question, then history, then new question
        first_q = history[0]["content"] if history else question
        messages.append({
            "role": "user",
            "content": f"Here is my current portfolio:\n\n{context}\n\n---\n\n{first_q}",
        })
        # Add remaining history
        for msg in history[1:]:
            messages.append({"role": msg["role"], "content": msg["content"]})
        # Add new question
        messages.append({"role": "user", "content": question})

    return messages


def ask_advisor(
    context: str,
    question: str,
    history: list[dict],
    api_key: str,
    model: str = MODEL,
    max_tokens: int = MAX_TOKENS,
) -> str:
    """Send a question to Claude with portfolio context.

    Args:
        context: Serialized portfolio context string.
        question: The user's question.
        history: Previous conversation messages [{"role": ..., "content": ...}].
        api_key: Anthropic API key.
        model: Model to use.
        max_tokens: Max response tokens.

    Returns:
        The assistant's response text.

    Raises:
        Exception on API errors (caller should handle via st.error).
    """
    client = anthropic.Anthropic(api_key=api_key)
    messages = build_messages(context, question, history)

    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=SYSTEM_PROMPT,
        messages=messages,
    )

    return response.content[0].text
