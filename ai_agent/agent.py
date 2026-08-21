"""
Optimized LangChain SQL agent wired to ClickHouse with real-time streaming support.

Features:
- Schema-injected 1-pass SQL generation (reduces 5 LLM passes to 1-2 passes).
- Async token streaming generator for real-time frontend rendering.
- Automatic fallback to SQL agent executor.
"""

import asyncio
import logging
import os
import re
from functools import lru_cache
from typing import AsyncGenerator

from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from db import get_db
from prompts import SYSTEM_PREFIX

logger = logging.getLogger("ai_agent.agent")

LLM_MODEL = os.getenv("LLM_MODEL", "openrouter/free")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://openrouter.ai/api/v1")


def get_llm():
    api_key = os.getenv("OPENAI_API_KEY")
    return ChatOpenAI(
        model=LLM_MODEL,
        openai_api_key=api_key,
        openai_api_base=OPENAI_API_BASE,
        base_url=OPENAI_API_BASE,
        temperature=0,
    )


@lru_cache(maxsize=1)
def get_agent_executor():
    db = get_db()
    llm = get_llm()

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    agent_executor = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        agent_type="openai-tools",
        prefix=SYSTEM_PREFIX,
        verbose=True,
    )
    return agent_executor


def ask_agent(question: str) -> str:
    executor = get_agent_executor()
    result = executor.invoke({"input": question})
    return result["output"]


async def ask_agent_stream(question: str) -> AsyncGenerator[str, None]:
    """High-speed 2-pass streaming agent generator.
    
    Yields text chunks directly to the client as they arrive from the LLM.
    """
    llm = get_llm()
    db = get_db()

    sql_system_prompt = (
        f"{SYSTEM_PREFIX}\n\n"
        "Your job right now is ONLY to write a single ClickHouse SQL query to answer the user's question.\n"
        "Return ONLY the SQL query inside ```sql ... ``` block or plain text without explanations."
    )

    try:
        # Step 1: Generate SQL query in 1 LLM pass
        sql_msg = await llm.ainvoke([
            SystemMessage(content=sql_system_prompt),
            HumanMessage(content=question)
        ])
        
        raw_sql = sql_msg.content.strip()
        # Extract SQL from code blocks if present
        match = re.search(r"```(?:sql)?\s*(.*?)\s*```", raw_sql, re.DOTALL | re.IGNORECASE)
        sql_query = match.group(1).strip() if match else raw_sql

        # Safety check: ensure read-only
        if not sql_query.upper().startswith("SELECT") and "WITH" not in sql_query.upper():
            # Fallback if LLM responded with direct text instead of SQL
            yield raw_sql
            return

        logger.info("Generated SQL: %s", sql_query)

        # Step 2: Execute query against ClickHouse
        loop = asyncio.get_running_loop()
        query_result = await loop.run_in_executor(None, db.run, sql_query)
        logger.info("Query Result: %s", str(query_result)[:200])

        # Step 3: Stream synthesis explanation from LLM
        synthesis_prompt = (
            f"User Question: {question}\n\n"
            f"ClickHouse SQL Query Executed:\n{sql_query}\n\n"
            f"Query Results:\n{query_result}\n\n"
            "Explain the answer to the user in a clear, professional, structured format. "
            "Include key numbers, insights, and bullet points where helpful."
        )

        async for chunk in llm.astream([
            SystemMessage(content=SYSTEM_PREFIX),
            HumanMessage(content=synthesis_prompt)
        ]):
            if chunk.content:
                yield chunk.content

    except Exception as e:
        logger.exception("Fast streaming path failed, invoking fallback agent: %s", e)
        # Fallback to standard agent executor if fast path fails
        try:
            loop = asyncio.get_running_loop()
            answer = await loop.run_in_executor(None, ask_agent, question)
            yield answer
        except Exception as fallback_err:
            yield f"Error processing query: {fallback_err}"
