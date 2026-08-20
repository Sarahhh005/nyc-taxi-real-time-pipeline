"""
LangChain SQL agent wired to ClickHouse.

get_agent_executor() is memoized so the (relatively expensive) schema
introspection only happens once per process, not once per request.
"""

import os
from functools import lru_cache

from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from langchain_openai import ChatOpenAI

from db import get_db
from prompts import SYSTEM_PREFIX

LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")


@lru_cache(maxsize=1)
def get_agent_executor():
    db = get_db()

    llm = ChatOpenAI(
        model=LLM_MODEL,
        temperature=0,  # deterministic SQL generation — you don't want a creative query planner
    )

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    agent_executor = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        agent_type="openai-tools",
        prefix=SYSTEM_PREFIX,
        verbose=True,  # prints the agent's SQL + reasoning to logs — useful to show during the demo
    )
    return agent_executor


def ask_agent(question: str) -> str:
    executor = get_agent_executor()
    result = executor.invoke({"input": question})
    return result["output"]


if __name__ == "__main__":
    # Quick manual smoke test: python agent.py "your question here"
    import sys

    q = " ".join(sys.argv[1:]) or "How many trips are in the database?"
    print(f"Q: {q}")
    print(f"A: {ask_agent(q)}")
