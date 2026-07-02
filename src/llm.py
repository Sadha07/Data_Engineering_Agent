"""LLM factory.

Default provider is Databricks Model Serving (Foundation Model APIs), which is
OpenAI-compatible and needs no external key inside a Databricks App. You can
switch to openai/anthropic via env for local development.
"""
from __future__ import annotations

from .config import config


def get_llm(temperature: float = 0.0):
    provider = config.llm_provider.lower()

    if provider == "databricks":
        # OpenAI-compatible endpoint exposed by Databricks serving.
        from langchain_openai import ChatOpenAI

        base_url = f"{config.host.rstrip('/')}/serving-endpoints" if config.host else None
        return ChatOpenAI(
            model=config.llm_model,
            temperature=temperature,
            base_url=base_url,
            api_key=config.token or "no-token-in-app",
        )

    if provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=config.llm_model, temperature=temperature)

    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=config.llm_model, temperature=temperature)

    raise ValueError(f"Unknown LLM_PROVIDER: {config.llm_provider}")
