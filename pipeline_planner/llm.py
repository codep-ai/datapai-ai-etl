"""
LLM transport for pipeline_planner.

Decision #6: the transport is datapai-platform-be/agents/llm_client.py:RouterChatClient
(9 vendors, routed by LLM_MODE / LLM_PRIMARY_PROVIDER env). We call it directly
from graph nodes; `ChatFn` is the only contract the planner depends on, so tests
can inject a stub and a LangChain BaseChatModel wrapper can be added later.

RouterChatClient.chat() returns {"role": ..., "content": str} for every provider;
older callers (dbt_agent.plan_with_llm) expect a bare str — `as_text` handles both.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Callable, Dict, List

import platform_init  # noqa: F401  (adds DATAPAI_PLATFORM_DIR to sys.path)

logger = logging.getLogger(__name__)

Message = Dict[str, str]
ChatFn = Callable[[List[Message]], str]


def as_text(resp: Any) -> str:
    if resp is None:
        return ""
    if isinstance(resp, str):
        return resp
    if isinstance(resp, dict):
        return str(resp.get("content") or resp.get("text") or "")
    return str(resp)


def get_router_chat(temperature: float = 0.1) -> ChatFn:
    """
    Return a `chat(messages) -> str` closure over RouterChatClient.
    Raises RuntimeError with a clear message if the platform checkout is missing
    or DATAPAI_LLM_ENABLED=false.
    """
    if not platform_init.platform_available():
        raise RuntimeError(
            f"datapai-platform-be not found at DATAPAI_PLATFORM_DIR={platform_init.platform_dir()} "
            "(agents/llm_client.py missing). On EC2 export DATAPAI_PLATFORM_DIR=/home/ec2-user/git/datapai-platform-be"
        )
    from agents.llm_client import RouterChatClient  # type: ignore

    # A whole-DAG plan is a long JSON document. platform-be's llm_policy caps the
    # ad-hoc call class at 8192 output tokens INCLUDING Gemini thinking tokens,
    # which truncated the first stock plan at ~8.8 KB (2026-09-09). Raise the
    # ceiling for this process only; operators can still override via env.
    os.environ.setdefault("DATAPAI_ADHOC_MAX_TOKENS", os.getenv("AI_ETL_PLANNER_MAX_TOKENS", "32768"))
    os.environ.setdefault("DATAPAI_ADHOC_THINKING", os.getenv("AI_ETL_PLANNER_THINKING", "2048"))

    client = RouterChatClient()
    logger.info("RouterChatClient mode=%s primary=%s dual_review=%s",
                client.mode, client.primary_provider, client.dual_review_enabled)

    def _chat(messages: List[Message]) -> str:
        return as_text(client.chat(messages=messages, temperature=temperature))

    return _chat


def llm_enabled() -> bool:
    return os.environ.get("DATAPAI_LLM_ENABLED", "true").lower() == "true"
