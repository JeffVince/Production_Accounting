#!/usr/bin/env python3
"""
Chain-of-Thought AI Agent Example with an Extra Step to Show SELECT Count
"""

import os
import re
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI
from dotenv import load_dotenv
load_dotenv("../.env")

###############################################################################
# Basic Infrastructure: DBExecutor, FSExecutor, etc.
###############################################################################
class DBExecutor:
    """Executes CRUD / schema changes in a SQLite DB."""
    def __init__(self, db_path=":memory:"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    last_login TEXT
                );
            """)

    def execute_query(self, sql: str):
        with self.conn:
            cur = self.conn.execute(sql)
            if sql.strip().lower().startswith("select"):
                return cur.fetchall()  # This is a list of rows
            else:
                return cur.rowcount

    def close(self):
        self.conn.close()


class FSExecutor:
    """Reads/writes files from a base directory."""
    def __init__(self, base_dir="."):
        self.base_dir = Path(base_dir).resolve()

    def _resolve_path(self, relative: str) -> Path:
        p = Path(relative)
        if not p.is_absolute():
            p = self.base_dir / p
        return p.resolve()

    def read_file(self, relative: str) -> str:
        path = self._resolve_path(relative)
        if not path.exists():
            raise FileNotFoundError(f"No file: {relative}")
        return path.read_text(encoding="utf-8")

    def write_file(self, relative: str, content: str):
        path = self._resolve_path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


###############################################################################
# ProjectContext
###############################################################################
class ProjectContext:
    """Maintains knowledge about DB tables, code files, ephemeral data."""
    def __init__(self, db_executor: DBExecutor, fs_executor: FSExecutor):
        self.db_executor = db_executor
        self.fs_executor = fs_executor
        self.table_schemas: Dict[str, List[str]] = {}
        self.files: Dict[str, Any] = {}
        self.memory: Dict[str, Any] = {}

    def refresh_schema_and_files(self):
        # DB schema
        self.table_schemas.clear()
        rows = self.db_executor.conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        for r in rows:
            tbl = r["name"]
            colrows = self.db_executor.conn.execute(f"PRAGMA table_info({tbl});").fetchall()
            self.table_schemas[tbl] = [c["name"] for c in colrows]

        # File Manifest
        base_dir = self.fs_executor.base_dir
        self.files.clear()
        for p in base_dir.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(base_dir))
                self.files[rel] = {"size": p.stat().st_size, "ext": p.suffix}


###############################################################################
# LLM "Roles"
###############################################################################
class ConversationLLM:
    SYSTEM_PROMPT = """\
You are a helpful conversational assistant. 
You are chatty and attempt to interpret the user's goal, 
but do NOT produce any structured plan or JSON. Just talk in natural language.
"""
    def __init__(self, client: OpenAI, model="gpt-4"):
        self.client = client
        self.model = model

    def converse(self, chat_history: List[Dict[str, str]]) -> str:
        messages = [{"role": "system", "content": self.SYSTEM_PROMPT}] + chat_history
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.7
        )
        return resp.choices[0].message.content.strip()


class PlannerLLM:
    SYSTEM_PROMPT = """\
You are a planning engine that takes the user's goal and figures out 
a multi-step approach in natural language. 
Include your chain-of-thought reasoning. 
Do NOT finalize with JSON, just plain text describing your plan.
"""
    def __init__(self, client: OpenAI, model="gpt-4"):
        self.client = client
        self.model = model

    def plan(self, user_goal: str, context_summary: str) -> str:
        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": f"User's goal: {user_goal}\n\nContext:\n{context_summary}\n\nPlan steps."}
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.7
        )
        return resp.choices[0].message.content.strip()


class CommandParserLLM:
    SYSTEM_PROMPT = """\
You are a command parser that reads a freeform plan 
and produces JSON commands. 
action_types: [db_operation, file_operation, respond_message]
Return JSON only, with "commands": [ ... ]
No extra commentary.
"""
    def __init__(self, client: OpenAI, model="gpt-3.5-turbo"):
        self.client = client
        self.model = model

    def parse_plan_to_commands(self, plan_text: str, context_summary: str) -> Dict[str, Any]:
        user_prompt = f"""\
Plan:
{plan_text}

Context:
{context_summary}

Now produce JSON "commands" for each step. 
If there's just a concluding note, use respond_message.
"""

        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0
            )
            text = resp.choices[0].message.content
            return json.loads(text)
        except Exception as e:
            return {"error": str(e), "commands": []}


class ReflectionLLM:
    SYSTEM_PROMPT = """\
You are a reflection engine. You read the results of the steps, 
and decide if we need more steps or if we are done.
"""
    def __init__(self, client: OpenAI, model="gpt-3.5-turbo"):
        self.client = client
        self.model = model

    def reflect(self, steps_executed: List[str]) -> str:
        msgs = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": "\n".join(steps_executed)}
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=msgs,
            temperature=0.2
        )
        return resp.choices[0].message.content.strip()


###############################################################################
# TaskOrchestrator (MODIFIED HERE)
###############################################################################
class TaskOrchestrator:
    """
    We run the commands.
    If there's a SELECT, we also produce a respond_message with the count of rows.
    """
    def __init__(self, db_executor: DBExecutor, fs_executor: FSExecutor):
        self.db_executor = db_executor
        self.fs_executor = fs_executor

    def run_commands(self, commands: List[Dict[str, Any]]) -> List[str]:
        results = []
        for cmd in commands:
            action_type = cmd.get("action_type")
            details = cmd.get("details", {})

            if action_type == "db_operation":
                sql = details.get("sql", "")
                try:
                    db_result = self.db_executor.execute_query(sql)
                    if isinstance(db_result, list):
                        # That means a SELECT statement
                        rowcount = len(db_result)
                        results.append(f"Executed SQL: {sql}\nReturned {rowcount} rows.")
                        # [MODIFIED HERE] Add an extra "respond_message" to show the user the count
                        msg = f"You have {rowcount} table(s) returned by that query."
                        results.append(f"Message to user: {msg}")
                    else:
                        # It's an int rowcount
                        results.append(f"Executed SQL: {sql}\nRows affected: {db_result}")
                except Exception as e:
                    results.append(f"DB error: {e}")

            elif action_type == "file_operation":
                mode = details.get("mode", "")
                path = details.get("path", "")
                if mode == "read":
                    try:
                        content = self.fs_executor.read_file(path)
                        results.append(f"Read file '{path}': {len(content)} chars")
                    except Exception as e:
                        results.append(f"File read error: {e}")
                elif mode == "write":
                    c = details.get("content", "")
                    try:
                        self.fs_executor.write_file(path, c)
                        results.append(f"Wrote file '{path}' with {len(c)} chars")
                    except Exception as e:
                        results.append(f"File write error: {e}")
                else:
                    results.append(f"Unknown file mode: {mode}")

            elif action_type == "respond_message":
                txt = details.get("text", "")
                results.append(f"Message to user: {txt}")

            else:
                results.append(f"Unknown action_type: {action_type}")

        return results


###############################################################################
# Main Agent
###############################################################################
class ChainOfThoughtAgent:
    """
    The same chain-of-thought approach, but now the orchestrator
    auto-displays the row count if the user did a SELECT.
    """
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)

        # Tools
        self.db_executor = DBExecutor()
        self.fs_executor = FSExecutor()
        self.proj_context = ProjectContext(self.db_executor, self.fs_executor)
        self.proj_context.refresh_schema_and_files()

        # LLM roles
        self.conversation_llm = ConversationLLM(self.client, model="gpt-3.5-turbo")
        self.planner_llm = PlannerLLM(self.client, model="gpt-4")
        self.cmd_parser_llm = CommandParserLLM(self.client, model="gpt-3.5-turbo")
        self.reflection_llm = ReflectionLLM(self.client, model="gpt-3.5-turbo")

        self.orchestrator = TaskOrchestrator(
            db_executor=self.db_executor,
            fs_executor=self.fs_executor
        )

        self.chat_history: List[Dict[str, str]] = []

    def run_shell(self):
        print("Chain-of-Thought Agent. Type 'exit' or 'quit' to stop.")
        while True:
            user_input = input("You> ").strip()
            if user_input.lower() in ["exit", "quit"]:
                print("Goodbye!")
                break

            self.chat_history.append({"role": "user", "content": user_input})

            # Heuristic: look for DB or file-related keywords
            if self._looks_like_action_request(user_input):
                # Summarize the user’s request
                summary = self._call_conversation_llm(user_input)
                print(f"\nAgent> (Conversation summary of your goal): {summary}")

                # Build chain-of-thought plan
                context_str = self._make_context_summary()
                plan_text = self.planner_llm.plan(user_input, context_str)
                print(f"\nAgent> Proposed Plan (chain-of-thought):\n{plan_text}\n")

                confirm_plan = input("Proceed with parsing this plan into commands? [yes/no] ").strip().lower()
                if confirm_plan.startswith("y"):
                    cmd_data = self.cmd_parser_llm.parse_plan_to_commands(plan_text, context_str)
                    if "error" in cmd_data and cmd_data["error"]:
                        print(f"\nAgent> Command parser error: {cmd_data['error']}\n")
                        continue
                    commands = cmd_data.get("commands", [])
                    if not commands:
                        print("\nAgent> No commands found.\n")
                        continue

                    print("\nAgent> Parsed commands:")
                    for i, c in enumerate(commands, 1):
                        print(f"  {i}) {c}")
                    confirm_execute = input("Execute these commands? [yes/no] ").strip().lower()
                    if confirm_execute.startswith("y"):
                        results = self.orchestrator.run_commands(commands)
                        print("\nAgent> Execution results:")
                        for r in results:
                            print(f" - {r}")
                        reflection = self.reflection_llm.reflect(results)
                        print(f"\nAgent> Reflection says:\n{reflection}\n")
                    else:
                        print("\nAgent> Plan execution cancelled.\n")

                else:
                    print("\nAgent> Understood. Not parsing.\n")
            else:
                # Normal conversation
                reply = self._call_conversation_llm(user_input)
                print(f"\nAgent> {reply}\n")

    def _call_conversation_llm(self, user_input: str) -> str:
        msgs = []
        for msg in self.chat_history[-5:]:
            msgs.append({"role": msg["role"], "content": msg["content"]})
        answer = self.conversation_llm.converse(msgs)
        self.chat_history.append({"role": "assistant", "content": answer})
        return answer

    def _looks_like_action_request(self, text: str) -> bool:
        triggers = ["select", "insert", "update", "delete", "drop", "create", "alter",
                    "read file", "write file", "code", "improve", "db ", "schema", "table", "tables"]
        lower = text.lower()
        return any(t in lower for t in triggers)

    def _make_context_summary(self) -> str:
        self.proj_context.refresh_schema_and_files()
        db_lines = []
        for t, cols in self.proj_context.table_schemas.items():
            db_lines.append(f"{t}: {', '.join(cols)}")
        file_lines = list(self.proj_context.files.keys())[:30]
        return f"DB Schema:\n" + "\n".join(db_lines) + "\n\nFiles:\n" + "\n".join(file_lines)

###############################################################################
# Entry
###############################################################################
if __name__ == "__main__":
    api_key = os.getenv("OPENAI_API_KEY", "YOUR_API_KEY")
    agent = ChainOfThoughtAgent(api_key=api_key)
    agent.run_shell()