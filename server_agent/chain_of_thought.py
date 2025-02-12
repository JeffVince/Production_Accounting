#!/usr/bin/env python3
"""
chain_of_thought_agent.py

A refined agent that:
- Hides chain-of-thought from the user.
- Provides direct answers for certain requests (like how many tables, table names, record counts).
- Uses a confirmable plan-based approach for more complex tasks, but doesn't print partial
  chain-of-thought or "Conversation summary" to the user.
- Can now read any files in the code base, discuss their code, and even edit them interactively.
- Also supports listing function definitions from Python files.
"""

import os
import re
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

# -------------------------------------------------------------------------
# 1) Import model.py to ensure your DB tables/metadata are loaded.
# -------------------------------------------------------------------------
import database.models

from openai import OpenAI
from sqlalchemy import text

from utilities.config import Config
from db_util import initialize_database, get_db_session
from dotenv import load_dotenv

load_dotenv("../.env")  # Load .env if present for OPENAI_API_KEY

from document_ingestor import DocumentIngestor
from retriever import Retriever

###############################################################################
# Basic Tools
###############################################################################
class DBExecutor:
    def __init__(self):
        # If get_db_session() is a context manager,
        # we forcibly __enter__() it once
        self._ctx = get_db_session()      # This is a GeneratorContextManager
        self.session = self._ctx.__enter__()  # The actual Session object

    def execute_query(self, sql: str):
        result = self.session.execute(text(sql))
        if result.returns_rows:
            rows = result.mappings().all()
            return [dict(r) for r in rows]
        else:
            return result.rowcount

    def close(self):
        if self._ctx:
            self._ctx.__exit__(None, None, None)
            self._ctx = None

    def list_tables(self) -> List[str]:
        """
        Returns a list of table names from sys_table (adjust query if needed).
        """
        rows = self.execute_query("SELECT name FROM sys_table WHERE type='table';")
        return [r["name"] for r in rows]

    def count_tables(self) -> int:
        """
        Returns the number of tables in sys_table (adjust query if needed).
        """
        rows = self.session.execute(text("SELECT COUNT(*) AS c FROM sys_table WHERE type='table';"))
        count = rows.mappings().all()[0]["c"]
        return count

    def count_in_table(self, table_name: str) -> int:
        """
        Returns the number of records in a given table_name.
        Adjust SQL if your environment differs.
        """
        query = f"SELECT COUNT(*) AS c FROM {table_name}"
        rows = self.session.execute(text(query)).mappings().all()
        if rows:
            return rows[0]["c"]
        return 0


class FSExecutor:
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

    def list_dir(self) -> List[str]:
        """
        Returns a list of files in the base_dir. If 'tree.txt' exists,
        we attempt to use its contents as the canonical file listing.
        """
        tree_path = self._resolve_path("tree.txt")
        if tree_path.exists():
            txt = tree_path.read_text(encoding="utf-8").strip()
            listing = [line.strip() for line in txt.splitlines() if line.strip()]
            return listing

        items = []
        for p in self.base_dir.iterdir():
            if p.is_file():
                items.append(str(p.name))
        return items

###############################################################################
# ProjectContext
###############################################################################
class ProjectContext:
    def __init__(self, db_executor: DBExecutor, fs_executor: FSExecutor):
        self.db_executor = db_executor
        self.fs_executor = fs_executor
        self.table_schemas: Dict[str, List[str]] = {}
        self.files: Dict[str, Any] = {}
        self.memory: Dict[str, Any] = {}

    def refresh_schema_and_files(self):
        self.table_schemas.clear()
        rows = self.db_executor.execute_query("SELECT name FROM sys_table WHERE type='table';")
        for r in rows:
            tbl = r["name"]
            colrows = self.db_executor.execute_query(f"PRAGMA table_info({tbl});")
            col_names = [c["name"] for c in colrows]
            self.table_schemas[tbl] = col_names

        base_dir = self.fs_executor.base_dir
        self.files.clear()
        for p in base_dir.rglob("*"):
            if p.is_file():
                rel = str(p.relative_to(base_dir))
                self.files[rel] = {"size": p.stat().st_size, "ext": p.suffix}

###############################################################################
# LLM Roles
###############################################################################
class ConversationLLM:
    SYSTEM_PROMPT = """\
You are a helpful conversational assistant.
You can chat about the user's request in natural language.
Only produce direct answers, no chain-of-thought or JSON.
"""
    def __init__(self, client: OpenAI, model="gpt-3.5-turbo"):
        self.client = client
        self.model = model

    def converse(self, messages: List[Dict[str, str]]) -> str:
        all_msgs = [{"role": "system", "content": self.SYSTEM_PROMPT}] + messages
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=all_msgs,
            temperature=0.7
        )
        return resp.choices[0].message.content.strip()


class PlannerLLM:
    SYSTEM_PROMPT = """\
You are a hidden planning engine.
You should produce a plan in plain text (chain-of-thought).
However, the user must NOT see your chain-of-thought.
We'll store your plan internally.
Respond with your chain-of-thought, but the user won't see it.
"""
    def __init__(self, client: OpenAI, model="gpt-4"):
        self.client = client
        self.model = model

    def plan(self, user_goal: str, context_summary: str) -> str:
        msgs = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"UserGoal: {user_goal}\nContext:\n{context_summary}\nPlan your steps internally."
            }
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=msgs,
            temperature=0.7
        )
        return resp.choices[0].message.content.strip()


class CommandParserLLM:
    SYSTEM_PROMPT = """\
You read an internal chain-of-thought plan and produce a JSON object with "commands".
Possible actions: db_operation, file_operation, respond_message.
No chain-of-thought or commentary. Just valid JSON.
Don't include triple backticks in your response. Output raw JSON only.
Ensure that file operations are wrapped using the key "file_operation" with a sub-object that includes an "action" key (e.g., "read_file", "write_file") and any necessary parameters.
"""
    def __init__(self, client: OpenAI, model="gpt-3.5-turbo"):
        self.client = client
        self.model = model

    def parse_plan_to_commands(self, plan_text: str, context_summary: str) -> Dict[str, Any]:
        user_prompt = f"""\
Chain-of-thought plan (not shown to user):
{plan_text}

Context:
{context_summary}

Output valid JSON with "commands": [...]
No chain-of-thought or commentary.
"""
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1
        )
        text = resp.choices[0].message.content

        text_cleaned = re.sub(r"```(?:json)?|```", "", text).strip()
        try:
            return json.loads(text_cleaned)
        except Exception:
            return {"error": f"Invalid JSON: {text}", "commands": []}


class ReflectionLLM:
    SYSTEM_PROMPT = """\
You read the step results and decide if we are done or need more steps.
Only produce a direct statement like 'All done!' or 'We need more steps...'
No chain-of-thought or commentary.
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
# Task Orchestrator
###############################################################################
class TaskOrchestrator:
    def __init__(self, db_executor: DBExecutor, fs_executor: FSExecutor):
        self.db_executor = db_executor
        self.fs_executor = fs_executor

    def run_commands(self, commands: List[Dict[str, Any]]) -> List[str]:
        results = []
        for cmd in commands:
            # Allow commands to be wrapped with keys like "db_operation", "file_operation", or "respond_message"
            if "db_operation" in cmd:
                atype = "db_operation"
                details = cmd["db_operation"]
            elif "file_operation" in cmd:
                atype = "file_operation"
                details = cmd["file_operation"]
            elif "respond_message" in cmd:
                atype = "respond_message"
                details = cmd["respond_message"]
            else:
                atype = cmd.get("action_type")
                details = cmd.get("details", {})

            if atype == "db_operation":
                sql = details.get("sql", "")
                try:
                    db_result = self.db_executor.execute_query(sql)
                    if isinstance(db_result, list):
                        rowcount = len(db_result)
                        results.append(f"Executed SELECT: {sql}\nFound {rowcount} rows.")
                        table_names = [row["name"] for row in db_result if "name" in row]
                        if table_names:
                            results.append(f"Message to user: Table names => {table_names}")
                        else:
                            results.append("Message to user: No 'name' column found.")
                    else:
                        results.append(f"Executed SQL: {sql}\nRows affected: {db_result}")
                except Exception as e:
                    results.append(f"DB error: {e}")

            elif atype == "file_operation":
                operation = details.get("operation", "")
                if operation in ["list_files", "filter_files_by_extension", "count_files"]:
                    res = self._handle_custom_file_ops(operation, details)
                    results.append(res)
                elif operation == "read_file" or details.get("mode") == "read":
                    path = details.get("path", "")
                    try:
                        content = self.fs_executor.read_file(path)
                        if len(content) > 1000:
                            displayed = content[:1000] + "\n...[Content truncated]"
                        else:
                            displayed = content
                        results.append(f"Content of file '{path}':\n{displayed}")
                    except Exception as e:
                        results.append(f"File read error: {e}")
                elif operation == "write_file" or details.get("mode") == "write":
                    path = details.get("path", "")
                    content = details.get("content", "")
                    try:
                        self.fs_executor.write_file(path, content)
                        results.append(f"Wrote file '{path}' with {len(content)} characters.")
                    except Exception as e:
                        results.append(f"File write error: {e}")
                else:
                    mode = details.get("mode", "")
                    path = details.get("path", "")
                    if mode == "read":
                        try:
                            content = self.fs_executor.read_file(path)
                            if len(content) > 1000:
                                displayed = content[:1000] + "\n...[Content truncated]"
                            else:
                                displayed = content
                            results.append(f"Content of file '{path}':\n{displayed}")
                        except Exception as e:
                            results.append(f"File read error: {e}")
                    elif mode == "write":
                        c = details.get("content", "")
                        try:
                            self.fs_executor.write_file(path, c)
                            results.append(f"Wrote file '{path}' with {len(c)} characters.")
                        except Exception as e:
                            results.append(f"File write error: {e}")
                    else:
                        results.append("Unknown file mode or operation in file_operation.")

            elif atype == "respond_message":
                msg = details.get("text") or details.get("message") or ""
                results.append(f"Message to user: {msg}")

            else:
                results.append(f"Unknown action_type: {atype}")
        return results

    def _handle_custom_file_ops(self, operation: str, details: Dict[str, Any]) -> str:
        if operation == "list_files":
            listing = self.fs_executor.list_dir()
            return f"FileOperation: list_files => {listing}"
        elif operation in ["filter_files_by_extension", "identify_files_by_extension"]:
            ext = details.get("extension", "")
            all_files = self.fs_executor.list_dir()
            filtered = [f for f in all_files if f.endswith(ext)]
            return f"FileOperation: filter_files_by_extension => {filtered}"
        elif operation == "count_files":
            listing = self.fs_executor.list_dir()
            count = len(listing)
            return f"FileOperation: count_files => {count}"
        return "FileOperation: unknown operation"

###############################################################################
# The Agent
###############################################################################
class ChainOfThoughtAgent:
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)

        # Tools
        self.db_executor = DBExecutor()
        self.fs_executor = FSExecutor()
        self.proj_context = ProjectContext(self.db_executor, self.fs_executor)
        self.proj_context.refresh_schema_and_files()

        # LLM Roles
        self.conversation_llm = ConversationLLM(self.client, "gpt-3.5-turbo")
        self.planner_llm = PlannerLLM(self.client, "gpt-4")
        self.cmd_parser_llm = CommandParserLLM(self.client, "gpt-3.5-turbo")
        self.reflection_llm = ReflectionLLM(self.client, "gpt-3.5-turbo")

        # Orchestrator
        self.orchestrator = TaskOrchestrator(self.db_executor, self.fs_executor)

        # Vector-based knowledge
        self.retriever = Retriever(api_key)

        self.chat_history: List[Dict[str, str]] = []

    def run_shell(self):
        print("Chain-of-Thought Agent. Type 'exit' or 'quit' to stop.")
        while True:
            user_input = input("You> ").strip()
            if user_input.lower() in ["exit", "quit"]:
                print("Goodbye!")
                break

            # --- Direct file edit queries ---
            file_edit_match = re.search(r"(?:edit|modify|update)\s+(?:the\s+)?(?:code\s+in\s+)?([\w\.\-_/\\]+\.\w+)", user_input, re.IGNORECASE)
            if file_edit_match:
                filename = file_edit_match.group(1)
                try:
                    content = self.fs_executor.read_file(filename)
                    preview = content[:500] + ("\n...[Content truncated]" if len(content) > 500 else "")
                    print(f"\nAgent> Current content of '{filename}':\n{preview}\n")
                    print("Agent> Please provide the modifications you would like to make (describe your changes):")
                    user_mods = input("You (modifications)> ")
                    prompt = (f"I have the following code from '{filename}':\n\n{content}\n\n"
                              f"User requested modifications: {user_mods}\n\nPlease produce the updated code for the file.")
                    updated_code = self.conversation_llm.converse([{"role": "user", "content": prompt}])
                    print(f"\nAgent> Here is the updated code for '{filename}':\n{updated_code}\n")
                    print("Agent> Would you like to save these changes? [yes/no]")
                    confirm_save = input("You> ").strip().lower()
                    if confirm_save.startswith("y"):
                        self.fs_executor.write_file(filename, updated_code)
                        print(f"\nAgent> Changes saved to '{filename}'.\n")
                    else:
                        print("\nAgent> Changes discarded.\n")
                except Exception as e:
                    print(f"\nAgent> Error editing file '{filename}': {e}\n")
                continue

            # --- Direct file explanation queries ---
            file_explain_match = re.search(r"(?:explain|describe|discuss)\s+(?:the\s+)?(?:code\s+in\s+)?([\w\.\-_/\\]+\.\w+)", user_input, re.IGNORECASE)
            if file_explain_match:
                filename = file_explain_match.group(1)
                try:
                    content = self.fs_executor.read_file(filename)
                    prompt = (f"I have the following code from '{filename}':\n\n{content}\n\n"
                              "Please explain what this code does, its structure, and any noteworthy details.")
                    explanation = self.conversation_llm.converse([{"role": "user", "content": prompt}])
                    print(f"\nAgent> Explanation of '{filename}':\n{explanation}\n")
                except Exception as e:
                    print(f"\nAgent> Error reading file '{filename}': {e}\n")
                continue

            # --- Direct file opinion queries ---
            file_opinion_match = re.search(r"what\s+do\s+you\s+think\s+about\s+(?:the\s+)?(?:code\s+in\s+)?([\w\.\-_/\\]+\.\w+)", user_input, re.IGNORECASE)
            if file_opinion_match:
                filename = file_opinion_match.group(1)
                try:
                    content = self.fs_executor.read_file(filename)
                    prompt = (f"I have the content of the file '{filename}':\n\n{content}\n\n"
                              "Can you provide your thoughts or a summary about this file?")
                    answer = self.conversation_llm.converse([{"role": "user", "content": prompt}])
                    print(f"\nAgent> {answer}\n")
                except Exception as e:
                    print(f"\nAgent> Error reading file '{filename}': {e}\n")
                continue

            # --- Direct file read queries ---
            file_read_match = re.search(r"read(?:\s+to\s+me)?\s+(?:the\s+)?([\w\.\-_/\\]+\.\w+)", user_input, re.IGNORECASE)
            if file_read_match:
                filename = file_read_match.group(1)
                try:
                    content = self.fs_executor.read_file(filename)
                    if len(content) > 1000:
                        displayed = content[:1000] + "\n...[Content truncated]"
                    else:
                        displayed = content
                    print(f"\nAgent> Content of '{filename}':\n{displayed}\n")
                except Exception as e:
                    print(f"\nAgent> Error reading file '{filename}': {e}\n")
                continue

            # --- Direct function definition extraction ---
            func_defs_match = re.search(r"(?:list|show|display|listen)\s+(?:out\s+)?(?:the\s+)?function(?:\s+definitions)?\s+in\s+(?:the\s+)?([\w\.\-_/\\]+\.\w+)", user_input, re.IGNORECASE)
            if func_defs_match:
                filename = func_defs_match.group(1)
                try:
                    content = self.fs_executor.read_file(filename)
                    function_pattern = re.compile(r"^\s*def\s+([\w_]+)\s*\(.*\):", re.MULTILINE)
                    functions = function_pattern.findall(content)
                    if functions:
                        functions_list = "\n".join(functions)
                        print(f"\nAgent> Function definitions in '{filename}':\n{functions_list}\n")
                    else:
                        print(f"\nAgent> No function definitions found in '{filename}'.\n")
                except Exception as e:
                    print(f"\nAgent> Error reading file '{filename}': {e}\n")
                continue

            # --- Direct DB requests ---
            if self._direct_table_count_request(user_input):
                table_count = self.db_executor.count_tables()
                print(f"\nAgent> Let me check for you... There are {table_count} tables in 'virtual_pm'.\n")
                continue

            if self._direct_table_list_request(user_input):
                table_names = self.db_executor.list_tables()
                print(f"\nAgent> The tables are: {', '.join(table_names)}\n")
                continue

            if self._direct_count_in_table(user_input):
                table_name = self._extract_table_name(user_input)
                if table_name:
                    try:
                        count = self.db_executor.count_in_table(table_name)
                        print(f"\nAgent> Let me check... There are {count} records in the '{table_name}' table.\n")
                    except Exception as e:
                        print(f"\nAgent> Error: {e}\n")
                else:
                    print("\nAgent> I couldn't figure out which table you meant.\n")
                continue

            # --- Possibly retrieval if user references DB or code ---
            clean_input = re.sub(r'[^\w\s]', '', user_input.lower()).strip()
            if "table" in clean_input or "schema" in clean_input or "model" in clean_input:
                relevant_chunks = self.retriever.retrieve(user_input, top_k=4)
                context_str = "\n\n".join(relevant_chunks)
                messages = [
                    {
                        "role": "system",
                        "content": f"You have these code/DB references:\n{context_str}\nUse them to answer the question below in plain English."
                    },
                    {
                        "role": "user",
                        "content": user_input
                    }
                ]
                answer = self.conversation_llm.converse(messages)
                print(f"\nAgent> {answer}\n")
                continue

            # --- Multi-step action requests ---
            if self._looks_like_action_request(clean_input):
                plan_text = self.planner_llm.plan(user_input, self._make_context_summary())
                cmd_data = self.cmd_parser_llm.parse_plan_to_commands(plan_text, self._make_context_summary())
                if "error" in cmd_data and cmd_data["error"]:
                    print(f"\nAgent> JSON parse error: {cmd_data['error']}\n")
                    continue
                commands = cmd_data.get("commands", [])
                if not commands:
                    print("\nAgent> Sorry, I couldn't form a valid plan.\n")
                    continue

                print("\nAgent> I have a plan to do multiple steps. Would you like to see and run them? [yes/no]")
                confirm = input().strip().lower()
                if confirm.startswith("y"):
                    for i, cc in enumerate(commands, 1):
                        print(f" {i}) {cc}")

                    print("\nExecute these commands? [yes/no]")
                    conf2 = input().strip().lower()
                    if conf2.startswith("y"):
                        results = self.orchestrator.run_commands(commands)
                        print("\nAgent> Execution results:")
                        for r in results:
                            print(f"- {r}")
                        reflection = self.reflection_llm.reflect(results)
                        print(f"\nAgent> {reflection}\n")
                    else:
                        print("\nAgent> Understood, not executing.\n")
                else:
                    print("\nAgent> Ok, not executing any plan.\n")
            else:
                # --- Normal conversation ---
                answer = self._call_conversation_llm(user_input)
                print(f"\nAgent> {answer}\n")

    ############################################################################
    # Direct Handlers
    ############################################################################
    def _direct_table_count_request(self, text: str) -> bool:
        text_lower = text.lower()
        triggers = ["how many tables are in the db", "how many tables in the db"]
        return any(t in text_lower for t in triggers)

    def _direct_table_list_request(self, text: str) -> bool:
        text_lower = text.lower()
        triggers = ["what are their names", "what are the table names", "list of tables"]
        return any(t in text_lower for t in triggers)

    def _direct_count_in_table(self, text: str) -> bool:
        text_lower = text.lower()
        return ("how many records" in text_lower and "table" in text_lower)

    def _extract_table_name(self, text: str) -> Optional[str]:
        text_no_table = re.sub(r"\btable\b", "", text.lower())
        text_no_punct = re.sub(r"[^\w\s]", "", text_no_table)
        words = text_no_punct.split()

        if "in" in words:
            idx = words.index("in")
            if idx < len(words) - 1:
                candidate = words[idx+1:]
                candidate = [w for w in candidate if w not in ("the", "a", "an")]
                candidate_str = "_".join(candidate).strip()
                if candidate_str:
                    return candidate_str
        return None

    ############################################################################
    # Utility
    ############################################################################
    def _call_conversation_llm(self, user_input: str) -> str:
        self.chat_history.append({"role": "user", "content": user_input})
        msgs = self.chat_history[-5:]
        answer = self.conversation_llm.converse(msgs)
        self.chat_history.append({"role": "assistant", "content": answer})
        return answer

    def _looks_like_action_request(self, text: str) -> bool:
        triggers = [
            "select", "insert", "update", "delete", "drop",
            "create", "alter", "read file", "write file",
            "code", "improve", "db", "schema", "table", "tables"
        ]
        return any(t in text for t in triggers)

    def _make_context_summary(self) -> str:
        self.proj_context.refresh_schema_and_files()
        lines = []
        for t, cols in self.proj_context.table_schemas.items():
            lines.append(f"{t}: {', '.join(cols)}")
        file_list = list(self.proj_context.files.keys())[:30]
        return "DB Schema:\n" + "\n".join(lines) + "\n\nFiles:\n" + "\n".join(file_list)

###############################################################################
# Entry
###############################################################################
if __name__ == "__main__":
    config = Config()
    db_settings = config.get_database_settings(config.USE_LOCAL)
    initialize_database(db_settings['url'])

    api_key = os.getenv("OPENAI_API_KEY", "YOUR_OPENAI_KEY")
    agent = ChainOfThoughtAgent(api_key=api_key)
    agent.run_shell()