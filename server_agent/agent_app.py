#!/usr/bin/env python3
# chat_agent_improved.py

import os
import re
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from difflib import get_close_matches
from datetime import datetime
from time import sleep
from dotenv import load_dotenv
from sqlalchemy import text
from openai import OpenAI

# Adjust these imports to your structure
from database.db_util import get_db_session, initialize_database
from database.database_util import DatabaseOperations
from utilities.config import Config

# Import the Base from your models so we can extract table names.
from database.models import Base

load_dotenv('../.env')


class ChatAgent:
    """
    An improved chat agent for Jeff that:
      - Uses the OpenAI API for conversational responses and code generation.
      - Interacts with a MySQL database via DatabaseOperations and raw SQL.
      - Performs CRUD operations on your codebase files (read, write/create, update, delete).
      - Provides detailed file system summaries.
      - Uses an LLM to handle any tasks beyond basic CRUD (e.g., debugging, code recommendations,
        or answering broader questions).
    """

    def __init__(self, model: str = "gpt-4o"):
        self.model = model

        # Initialize the database (ensuring SessionLocal is set up)
        config = Config()
        db_settings = config.get_database_settings(config.USE_LOCAL)
        initialize_database(db_settings['url'])
        self.db_ops = DatabaseOperations()

        # Build a list of available table names from the Base metadata.
        self.available_tables = list(Base.metadata.tables.keys())
        self.last_table_names = None  # For conversational context

        # Load and store the models file content for context.
        self.models_file_content = self._get_models_file_content()

        # Create an OpenAI client.
        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", "YOUR_OPENAI_KEY_HERE"))

        # System prompt.
        self.system_prompt = (
            "You are a helpful chat agent for Jeff. "
            "Maintain a formal yet approachable tone. "
            "Ask clarifying questions if needed. "
            "Always address the user as 'Jeff'. "
            "When code is given, reply with the full code."
        )
        self.conversation_history = [{"role": "system", "content": self.system_prompt}]

        # Set the codebase root to be the Dropbox Listener directory.
        # (Assuming this file is in <codebase_root>/server_agent, codebase_root is one level up.)
        self.codebase_root = Path(__file__).resolve().parent.parent

        # Store the last error encountered (if any) for debugging.
        self.last_error: Optional[str] = None

    def run_chat(self):
        print("ChatAgent is now running. Type 'exit' or 'quit' to stop.")
        while True:
            user_input = input("\nYou: ")
            if user_input.lower() in ["exit", "quit"]:
                print("ChatAgent: Goodbye, Jeff!")
                break
            print("ChatAgent (status): Processing your request, please wait...")
            response = self.handle_user_input(user_input)
            print(f"ChatAgent: {response}")

    # ------------------
    # HELPER: ASCII TABLE FORMATTER
    # ------------------
    def _format_as_ascii_table(self, headers: List[str], rows: List[List[Any]]) -> str:
        col_widths = [len(header) for header in headers]
        for row in rows:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell)))
        sep = "+".join("-" * (w + 2) for w in col_widths)
        sep = f"+{sep}+"
        header_row = "| " + " | ".join(header.ljust(col_widths[i]) for i, header in enumerate(headers)) + " |"
        row_lines = []
        for row in rows:
            row_line = "| " + " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)) + " |"
            row_lines.append(row_line)
        table = "\n".join([sep, header_row, sep] + row_lines + [sep])
        return table

    # ------------------
    # FILE SYSTEM OPS
    # ------------------
    def _resolve_path(self, path_str: str) -> Path:
        p = Path(path_str)
        if not p.is_absolute():
            p = self.codebase_root / p
        return p.resolve()

    def _find_file_by_name(self, filename: str) -> Optional[Path]:
        for root, dirs, files in os.walk(self.codebase_root):
            if filename in files:
                return Path(root) / filename
        return None

    def _read_file(self, filename: str) -> str:
        filepath = self._resolve_path(filename)
        if not filepath.exists():
            filepath = self._find_file_by_name(filename)
            if not filepath:
                return f"Jeff, the file '{filename}' was not found in your codebase."
        if filepath.is_dir():
            return f"Jeff, '{filepath}' is a directory, not a file."
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
            return f"Contents of {filepath}:\n\n{content}"
        except Exception as e:
            return f"Jeff, I encountered an error reading {filepath}: {str(e)}"

    def _write_file(self, filename: str, new_content: str) -> str:
        filepath = self._resolve_path(filename)
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(new_content)
            return f"Jeff, I successfully wrote to {filepath}."
        except Exception as e:
            return f"Jeff, I encountered an error writing to {filepath}: {str(e)}"

    def _edit_file(self, filename: str, new_content: str) -> str:
        return self._write_file(filename, new_content)

    # ------------------
    # FILE SYSTEM CRUD: DELETE FILE
    # ------------------
    def _delete_file(self, filename: str) -> str:
        filepath = self._resolve_path(filename)
        if not filepath.exists():
            return f"Jeff, the file '{filename}' does not exist."
        if filepath.is_dir():
            return f"Jeff, '{filepath}' is a directory and cannot be deleted as a file."
        try:
            os.remove(filepath)
            return f"Jeff, the file '{filepath}' has been successfully deleted."
        except Exception as e:
            return f"Jeff, I encountered an error deleting '{filepath}': {str(e)}"

    def _count_python_files(self) -> str:
        count = 0
        for root, dirs, files in os.walk(self.codebase_root):
            for file in files:
                if file.endswith('.py'):
                    count += 1
        return f"Jeff, I found {count} Python files in your codebase."

    def _confirm_file_access(self) -> str:
        try:
            items = os.listdir(self.codebase_root)
            count = len(items)
            table = self._format_as_ascii_table(["Name"], [[item] for item in items])
            return f"Jeff, I can see {count} items (files/directories) in your codebase root:\n{table}"
        except Exception as e:
            return f"Jeff, I encountered an error accessing your codebase: {str(e)}"

    def _get_file_info(self, filename: str) -> str:
        filepath = self._resolve_path(filename)
        if not filepath.exists():
            filepath = self._find_file_by_name(filename)
            if not filepath:
                return f"Jeff, I couldn't find any file named '{filename}' in your codebase."
        try:
            stats = filepath.stat()
            size = stats.st_size
            mod_time = datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            with open(filepath, "r", encoding="utf-8") as f:
                lines = f.readlines()
            snippet = "".join(lines[:10])
            return (f"Jeff, I found the file '{filepath}'.\n"
                    f"Size: {size} bytes\nLast Modified: {mod_time}\n"
                    f"First few lines:\n{snippet}")
        except Exception as e:
            return f"Jeff, I encountered an error accessing '{filepath}': {str(e)}"

    # ------------------
    # AMBIGUITY RESOLUTION VIA LLM
    # ------------------
    def disambiguate_query(self, query: str) -> str:
        """
        Use the LLM to ask for clarification if the query is ambiguous.
        Expected return values are "file" or "db".
        """
        prompt = (
            f"Jeff, the query '{query}' is ambiguous. Please clarify: do you want to refer to files on disk or database tables?\n"
            "Respond with just 'file' or 'db'."
        )
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=10,
                temperature=0.0,
            )
            clarification = response.choices[0].message.content.strip().lower()
            if clarification in ["file", "db"]:
                return clarification
            return "file"  # default to file if ambiguous
        except Exception as e:
            return "file"  # default if error occurs

    # ------------------
    # DEBUGGING & CODE RECOMMENDATIONS
    # ------------------
    def _debug_error(self, error: Exception, code: str, request: str) -> str:
        prompt = (
            f"I encountered the following error while executing code for a database request:\n\n"
            f"Error: {str(error)}\n\n"
            f"Code:\n{code}\n\n"
            f"Original request: {request}\n\n"
            "Please explain what might be causing this error and offer debugging suggestions."
        )
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert debugging assistant."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=200,
                temperature=0.0,
            )
            return response.choices[0].message.content.strip()
        except Exception as e2:
            return f"Jeff, I encountered an error while debugging: {str(e2)}"

    def _recommend_code(self, filename: str) -> str:
        file_content_response = self._read_file(filename)
        if file_content_response.startswith("Contents of"):
            parts = file_content_response.split("\n\n", 1)
            file_text = parts[1] if len(parts) > 1 else file_content_response
        else:
            file_text = file_content_response
        # Optionally, you can also include a brief summary of the codebase to give more context.
        codebase_summary = self._confirm_file_access()
        if len(file_text) > 4000:
            file_text = file_text[:4000] + "\n...[truncated]"
        prompt = (
            f"I have the following file content from {filename}:\n\n"
            f"{file_text}\n\n"
            f"Here is a summary of the codebase for additional context:\n{codebase_summary}\n\n"
            "Please provide recommendations to improve this code in terms of performance, maintainability, and best practices. "
            "Include specific suggestions where applicable."
        )
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an expert software engineer providing code recommendations."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=300,
                temperature=0.0,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"Jeff, I encountered an error while generating recommendations: {str(e)}"

    # ------------------
    # GENERAL TASK DECOMPOSITION
    # ------------------
    def decompose_request(self, request: str) -> List[Dict[str, Any]]:
        prompt = (
            "Break down the following request into a list of actionable tasks that can be performed using your tools. "
            "For each task, output a JSON object with keys 'action' and 'parameters'. "
            "The available actions are:\n"
            "  - read_file (parameters: filename)\n"
            "  - write_file (parameters: filename, content)\n"
            "  - edit_file (parameters: filename, new_content)\n"
            "  - delete_file (parameters: filename)\n"
            "  - list_files (parameters: directory)\n"
            "  - db_query (parameters: query)  -- the query should be a complete SQL statement\n"
            "  - general_query (parameters: question)\n"
            "Return the output as a JSON array. If no decomposition is possible, return an empty array.\n"
            f"Request: \"{request}\""
        )
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a task decomposer. Output only JSON."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=200,
                temperature=0.0,
            )
            json_text = response.choices[0].message.content.strip()
            tasks = json.loads(json_text)
            if isinstance(tasks, list):
                return tasks
            return []
        except Exception as e:
            return []

    def process_tasks(self, tasks: List[Dict[str, Any]]) -> str:
        results = []
        for i, task in enumerate(tasks, start=1):
            action = task.get("action", "")
            params = task.get("parameters", {})
            results.append(f"Task {i}:")
            if action == "list_files":
                directory = params.get("directory", ".")
                results.append(f"Listing files in {directory}...")
                res = self._confirm_file_access() if directory == "." else self._list_files(directory)
            elif action == "read_file":
                filename = params.get("filename", "")
                results.append(f"Reading file {filename}...")
                res = self._read_file(filename)
            elif action == "write_file":
                filename = params.get("filename", "")
                content = params.get("content", "")
                results.append(f"Writing to file {filename}...")
                res = self._write_file(filename, content)
            elif action == "edit_file":
                filename = params.get("filename", "")
                new_content = params.get("new_content", "")
                results.append(f"Editing file {filename}...")
                res = self._edit_file(filename, new_content)
            elif action == "delete_file":
                filename = params.get("filename", "")
                results.append(f"Deleting file {filename}...")
                res = self._delete_file(filename)
            elif action == "db_query":
                query = params.get("query", "")
                results.append(f"Executing DB query: {query}...")
                res = self._auto_execute_task(query)
            elif action == "general_query":
                question = params.get("question", "")
                results.append(f"Processing general query: {question}...")
                res = self._call_llm(question)
            else:
                res = f"Action '{action}' not recognized."
            results.append(f"Result: {res}\n")
        return "\n".join(results)

    # ------------------
    # DB INTENT & PARSING
    # ------------------
    def classify_intent(self, user_input: str) -> str:
        prompt = (
            "Classify the following user input as either DB_TASK or GENERAL_CHAT. "
            "Return only the label (DB_TASK or GENERAL_CHAT) with no additional text.\n"
            f"User input: \"{user_input}\""
        )
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an intent classifier."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=10,
                temperature=0.0,
            )
            intent = response.choices[0].message.content.strip().upper()
            if intent not in ["DB_TASK", "GENERAL_CHAT"]:
                intent = "GENERAL_CHAT"
            return intent
        except Exception as e:
            return "GENERAL_CHAT"

    def parse_db_request(self, user_input: str) -> dict:
        prompt = (
            "You are a Python code generator that parses a database request and outputs a JSON object "
            "with two keys: 'function' and 'arguments'. The 'function' key must be one of the following: "
            "COUNT_TABLES, LIST_TABLE_NAMES, DESCRIBE_TABLE, COUNT_ROWS, LIST_ROWS, SEARCH_ROWS, SUM_COLUMN, "
            "AVG_COLUMN, MAX_COLUMN, MIN_COLUMN, INSERT_RECORD, UPDATE_RECORD, DELETE_RECORD. "
            "The 'arguments' key should be a JSON object mapping parameter names to values. "
            "Here is the content of the models file that defines available tables and columns:\n\n"
            f"{self.models_file_content}\n\n"
            "For example, for the request 'Describe table Receipt', output: "
            '{"function": "DESCRIBE_TABLE", "arguments": {"table": "Receipt"}}. '
            "If the request does not match any known database operation, output an empty JSON object {}.\n"
            f"User input: \"{user_input}\""
        )
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a database request parser. Output only JSON."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=100,
                temperature=0.0,
            )
            json_text = response.choices[0].message.content.strip()
            parsed = json.loads(json_text)
            if isinstance(parsed, dict):
                return parsed
            return {}
        except Exception as e:
            return {}

    # ------------------
    # CODEBASE CONTEXT & TABLE RESOLUTION
    # ------------------
    def _get_models_file_content(self) -> str:
        try:
            with open("../database/models.py", "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            return ""

    def resolve_table_name(self, input_name: str) -> str:
        prompt = (
            f"Below is the content of a models file that defines database table names:\n\n"
            f"{self.models_file_content}\n\n"
            f"Given the user-supplied table name or phrase: \"{input_name}\", "
            "return the exact table name (exactly as defined in the models file). "
            "Output only the table name with no extra commentary."
        )
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a table name resolver. Output only the exact table name."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=20,
                temperature=0.0,
            )
            resolved_name = response.choices[0].message.content.strip()
            if any(resolved_name.lower() == t.lower() for t in self.available_tables):
                return next(t for t in self.available_tables if t.lower() == resolved_name.lower())
            else:
                fallback = get_close_matches(
                    input_name.lower().replace(" ", "_"),
                    [t.lower() for t in self.available_tables],
                    n=1,
                    cutoff=0.5
                )
                if fallback:
                    return next(t for t in self.available_tables if t.lower() == fallback[0])
                else:
                    return input_name
        except Exception as e:
            return input_name

    # ------------------
    # RESULT FORMATTING VIA GPT-4o
    # ------------------
    def _format_result(self, raw_result: Any, request: str) -> str:
        prompt = (
            "Based on the following raw result and the original request, produce a friendly and contextualized answer. "
            "The answer should be a complete sentence that includes the context from the request. For example, "
            "if the raw result is 20 and the request was 'how many records in detail items?', output: "
            "'There are 20 records in the detail_item table. Anything else?'\n"
            f"Original request: {request}\n"
            f"Raw result: {raw_result}\n"
            "Output only the final answer."
        )
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a friendly assistant that formats raw results with context."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=100,
                temperature=0.0,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"There are {raw_result} (raw result)."

    # ------------------
    # HANDLING USER INPUT
    # ------------------
    def handle_user_input(self, user_input: str) -> str:
        lower_input = user_input.lower().strip()

        # === Check for ambiguous queries first ===
        if lower_input in ["what can you see?", "what do you see?", "what are you seeing?"]:
            # Use the LLM to clarify whether the user means files or database tables.
            clarification = self.disambiguate_query(user_input)
            if clarification == "file":
                return self._confirm_file_access()
            elif clarification == "db":
                return self._count_tables_in_db()  # or _list_table_names_in_db() as desired

        # === File query handling via keywords (overrides DB logic) ===
        file_query = re.search(r'\b(?:show|display|read|open|contents of)\s+(?:the\s+file\s+)?([\w\-.]+\.py)', lower_input)
        if file_query:
            filename = file_query.group(1)
            return self._read_file(filename)

        # === Explicit file operations ===
        if "list files" in lower_input:
            m = re.search(r"list files(?: in)? (.+)", lower_input)
            directory = m.group(1).strip() if m else "."
            return self._confirm_file_access() if directory == "." else self._list_files(directory)
        if "read file" in lower_input:
            m = re.search(r"read file (.+)", lower_input)
            filename = m.group(1).strip() if m else ""
            return self._read_file(filename)
        if "write file" in lower_input:
            m = re.search(r"write file ([^\s]+)\s*\{(.+)\}", user_input, re.DOTALL)
            if m:
                filename = m.group(1).strip()
                content = m.group(2).strip()
                return self._write_file(filename, content)
            else:
                return "Jeff, please provide a filename and content in the format: write file <filename> { <content> }"
        if "edit file" in lower_input:
            m = re.search(r"edit file ([^\s]+)\s*\{(.+)\}", user_input, re.DOTALL)
            if m:
                filename = m.group(1).strip()
                new_content = m.group(2).strip()
                return self._edit_file(filename, new_content)
            else:
                return "Jeff, please provide a filename and new content in the format: edit file <filename> { <new content> }"
        if "delete file" in lower_input:
            m = re.search(r"delete file\s+([^\s]+)", lower_input)
            if m:
                filename = m.group(1).strip()
                return self._delete_file(filename)
            else:
                return "Jeff, please provide a filename in the format: delete file <filename>"

        # === Auto infer file-related queries for recommendations ===
        if "recommendation" in lower_input and ".py" in lower_input:
            m = re.search(r'(agent_[\w\d\-]+\.py)', lower_input)
            if m:
                return self._recommend_code(m.group(1))

        # === Debugging request ===
        if "where was the error" in lower_input:
            if self.last_error:
                return f"Jeff, here are the debugging details from the last error:\n{self.last_error}"
            else:
                return "Jeff, I don't have any stored error details at the moment. Could you please specify what you're referring to?"

        # === Database-related tasks ===
        intent = self.classify_intent(user_input)
        if intent == "DB_TASK":
            parsed_request = self.parse_db_request(user_input)
            if parsed_request and "function" in parsed_request:
                func_name = parsed_request["function"]
                args = parsed_request.get("arguments", {})
                if "table" in args:
                    args["table"] = self.resolve_table_name(args["table"])
                mapping = {
                    "COUNT_TABLES": self._count_tables_in_db,
                    "LIST_TABLE_NAMES": self._list_table_names_in_db,
                    "DESCRIBE_TABLE": lambda: self._describe_table(args.get("table", "")),
                    "COUNT_ROWS": lambda: self._count_rows_in_table(args.get("table", "")),
                    "LIST_ROWS": lambda: self._list_all_rows_in_table(args.get("table", "")),
                    "SEARCH_ROWS": lambda: self._search_table_by_column(
                        args.get("table", ""), args.get("column", ""), args.get("value", "")
                    ),
                    "SUM_COLUMN": lambda: self._sum_column_in_table(args.get("table", ""), args.get("column", "")),
                    "AVG_COLUMN": lambda: self._avg_column_in_table(args.get("table", ""), args.get("column", "")),
                    "MAX_COLUMN": lambda: self._max_column_in_table(args.get("table", ""), args.get("column", "")),
                    "MIN_COLUMN": lambda: self._min_column_in_table(args.get("table", ""), args.get("column", "")),
                    "INSERT_RECORD": lambda: self._insert_new_record(args.get("table", ""), args.get("assignments", "")),
                    "UPDATE_RECORD": lambda: self._update_record(args.get("table", ""), args.get("set", ""), args.get("where", "")),
                    "DELETE_RECORD": lambda: self._delete_record(args.get("table", ""), args.get("where", "")),
                }
                if func_name in mapping:
                    try:
                        raw_result = mapping[func_name]()
                        return self._format_result(raw_result, user_input)
                    except Exception as e:
                        self.last_error = self._debug_error(e, "N/A (mapping call)", user_input)
                        return f"Jeff, I encountered an error while executing the command: {str(e)}. {self.last_error}"
            raw_result = self._auto_execute_task(user_input)
            return self._format_result(raw_result, user_input)

        # === General query decomposition ===
        tasks = self.decompose_request(user_input)
        if tasks:
            print("ChatAgent (status): Decomposing your request into sub-tasks...")
            for idx, task in enumerate(tasks, 1):
                print(f"  Task {idx}: Action = {task.get('action')}, Parameters = {task.get('parameters')}")
            combined_result = self.process_tasks(tasks)
            return self._format_result(combined_result, user_input)

        # Otherwise, use a general LLM call.
        return self._call_llm(user_input)

    # ------------------
    # LLM CALLS FOR GENERAL CHAT & CODE GENERATION
    # ------------------
    def _call_llm(self, message: str) -> str:
        self.conversation_history.append({"role": "user", "content": message})
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=self.conversation_history,
                max_tokens=512,
                temperature=0.7,
            )
            llm_reply = response.choices[0].message.content.strip()
            self.conversation_history.append({"role": "assistant", "content": llm_reply})
            return llm_reply
        except Exception as e:
            return f"Jeff, I encountered an error calling OpenAI API: {str(e)}"

    def _auto_execute_task(self, request: str) -> str:
        prompt = (
            "You are a Python code generator that produces only a Python snippet. "
            "The snippet MUST use a 'with get_db_session() as session:' block to connect to the database and execute SQL to fulfill the following database request. "
            "All SQL strings must be wrapped in sqlalchemy.text(...). "
            "Do not use any undefined variables; output complete SQL statements as literals. "
            "The code MUST assign its final output to a variable named 'result' and must not include any print statements or additional commentary. "
            "Do not include any import statements. Assume get_db_session() is already defined.\n\n"
            "Here is the content of the models file (defining available tables):\n"
            f"{self.models_file_content}\n\n"
            f"Database request: {request}"
        )
        code = self._call_llm_for_code(prompt)
        if "with get_db_session() as session:" not in code:
            code = "with get_db_session() as session:\n    " + "\n    ".join(code.splitlines())
            print("ChatAgent (debug): Wrapped code snippet for execution.")
        exec_globals = globals().copy()
        exec_globals['get_db_session'] = get_db_session
        try:
            import sqlalchemy
        except ImportError:
            pass
        exec_globals['sqlalchemy'] = __import__('sqlalchemy')
        local_vars = {}
        try:
            exec(code, exec_globals, local_vars)
            result = local_vars.get("result", None)
            if result is None:
                return "Jeff, the generated code did not produce any result."
            return result
        except SyntaxError as se:
            self.last_error = self._debug_error(se, code, request)
            fix_prompt = (
                "The following Python code produced a SyntaxError: " + str(se) + "\n"
                "Please fix the code so that it executes properly. Output only valid Python code that assigns "
                "the final output to a variable named 'result', without any import statements or markdown formatting.\n"
                "Code:\n" + code
            )
            fixed_code = self._call_llm_for_code(fix_prompt)
            print("ChatAgent (debug): Attempting to fix the code snippet.")
            try:
                local_vars = {}
                exec(fixed_code, exec_globals, local_vars)
                result = local_vars.get("result", None)
                if result is None:
                    return "Jeff, even after fixing, the code did not produce any result."
                return result
            except Exception as e:
                self.last_error = self._debug_error(e, fixed_code, request)
                return f"Jeff, I encountered an error executing the fixed code: {str(e)}. {self.last_error}"
        except Exception as e:
            self.last_error = self._debug_error(e, code, request)
            return f"Jeff, I encountered an error executing the generated code: {str(e)}. {self.last_error}"

    def _call_llm_for_code(self, prompt: str) -> str:
        messages = self.conversation_history.copy()
        messages.append({
            "role": "system",
            "content": "You are a Python code generator. Provide only Python code without any explanation or markdown formatting (no triple backticks or language tags)."
        })
        messages.append({"role": "user", "content": prompt})
        try:
            code_response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=512,
                temperature=0.0,
            )
            code = code_response.choices[0].message.content.strip()
            if code.startswith("```") and code.endswith("```"):
                code = code.strip("`").strip()
                lines = code.splitlines()
                if lines and re.match(r"^\s*python\s*$", lines[0], re.IGNORECASE):
                    lines = lines[1:]
                code = "\n".join(lines).strip()
            return code
        except Exception as e:
            return f"# Error generating code: {str(e)}"

    # ------------------
    # DATABASE (SCHEMA) OPS
    # ------------------
    def _count_tables_in_db(self) -> str:
        try:
            with get_db_session() as session:
                result = session.execute(text("SHOW TABLES;"))
                rows = result.fetchall()
                table_count = len(rows)
                self.last_table_names = [str(r[0]) for r in rows]
            return f"Jeff, there are {table_count} tables in your MySQL database."
        except Exception as e:
            return f"Jeff, I encountered an error counting tables: {str(e)}"

    def _list_table_names_in_db(self) -> str:
        try:
            with get_db_session() as session:
                result = session.execute(text("SHOW TABLES;"))
                rows = result.fetchall()
            if not rows:
                return "Jeff, it appears there are no tables in your database."
            table_names = [str(r[0]) for r in rows]
            self.last_table_names = table_names
            table = self._format_as_ascii_table(["Table Name"], [[name] for name in table_names])
            return f"Jeff, here are your tables:\n{table}"
        except Exception as e:
            return f"Jeff, I encountered an error listing the tables: {str(e)}"

    def _describe_table(self, table_name: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        if not table_name_clean:
            return "Jeff, I couldn't parse a valid table name."
        try:
            with get_db_session() as session:
                rows = session.execute(text(f"DESCRIBE {table_name_clean};")).fetchall()
            if not rows:
                return f"Jeff, I found no columns for '{table_name_clean}'."
            headers = ["Field", "Type", "Null", "Key"]
            data = [[r[0], r[1], r[2], r[3]] for r in rows]
            table = self._format_as_ascii_table(headers, data)
            return f"Jeff, here are the columns for '{table_name_clean}':\n{table}"
        except Exception as e:
            return f"Jeff, I had an error describing '{table_name_clean}': {str(e)}"

    def _add_column_to_table(self, table_name: str, column_def: str) -> str:
        t_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        if not t_clean:
            return "Jeff, I couldn't parse a valid table name."
        try:
            sql = f"ALTER TABLE {t_clean} ADD COLUMN {column_def}"
            with get_db_session() as session:
                session.execute(text(sql))
                session.commit()
            return f"Jeff, I added column '{column_def}' to table {t_clean}."
        except Exception as e:
            return f"Jeff, I encountered an error adding column to {t_clean}: {str(e)}"

    def _drop_column_from_table(self, table_name: str, column_name: str) -> str:
        t_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        c_clean = re.sub(r"[^A-Za-z0-9_]", "", column_name)
        if not t_clean or not c_clean:
            return "Jeff, I couldn't parse the table or column name."
        try:
            sql = f"ALTER TABLE {t_clean} DROP COLUMN {c_clean}"
            with get_db_session() as session:
                session.execute(text(sql))
                session.commit()
            return f"Jeff, I dropped column '{c_clean}' from table {t_clean}."
        except Exception as e:
            return f"Jeff, I encountered an error dropping the column: {str(e)}"

    def _create_table(self, table_name: str, column_info: str) -> str:
        t_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        if not t_clean:
            return "Jeff, I couldn't parse a valid new table name."
        try:
            col_part = column_info.replace("columns", "").replace(":", "").strip()
            sql = f"CREATE TABLE {t_clean} ({col_part})"
            with get_db_session() as session:
                session.execute(text(sql))
                session.commit()
            return f"Jeff, I created new table '{t_clean}' with columns: {col_part}"
        except Exception as e:
            return f"Jeff, I encountered an error creating table '{t_clean}': {str(e)}"

    def _drop_table(self, table_name: str) -> str:
        t_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        if not t_clean:
            return "Jeff, I couldn't parse a valid table name."
        try:
            with get_db_session() as session:
                session.execute(text(f"DROP TABLE IF EXISTS {t_clean};"))
                session.commit()
            return f"Jeff, table '{t_clean}' has been dropped (if it existed)."
        except Exception as e:
            return f"Jeff, I encountered an error dropping table '{t_clean}': {str(e)}"

    def _count_rows_in_table(self, table_name: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        if not table_name_clean:
            return "Jeff, I couldn't parse a valid table name from your request."
        try:
            with get_db_session() as session:
                result = session.execute(text(f"SELECT COUNT(*) FROM {table_name_clean}"))
                row = result.fetchone()
            if row is None:
                return f"Jeff, I couldn't fetch row count from table '{table_name_clean}'."
            count = row[0]
            return f"Jeff, there are {count} records in your {table_name_clean} table."
        except Exception as e:
            return f"Jeff, I encountered an error counting rows in '{table_name_clean}': {str(e)}"

    def _list_all_rows_in_table(self, table_name: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        if not table_name_clean:
            return "Jeff, I couldn't parse the table name."
        try:
            with get_db_session() as session:
                rows = session.execute(text(f"SELECT * FROM {table_name_clean}")).fetchall()
            if not rows:
                return f"Jeff, no rows found in '{table_name_clean}'."
            json_rows = [dict(r) for r in rows]
            headers = list(json_rows[0].keys())
            rows_data = [[row.get(h, "") for h in headers] for row in json_rows[:5]]
            table = self._format_as_ascii_table(headers, rows_data)
            return f"Jeff, here are up to 5 rows from '{table_name_clean}':\n{table}"
        except Exception as e:
            return f"Jeff, I encountered an error listing rows from '{table_name_clean}': {str(e)}"

    def _search_table_by_column(self, table_name: str, column_name: str, value: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        column_name_clean = re.sub(r"[^A-Za-z0-9_]", "", column_name)
        if not table_name_clean or not column_name_clean:
            return "Jeff, I couldn't parse table or column name."
        try:
            with get_db_session() as session:
                sql = text(f"SELECT * FROM {table_name_clean} WHERE {column_name_clean} = :val")
                rows = session.execute(sql, {"val": value}).fetchall()
            if not rows:
                return f"Jeff, no matching rows found in '{table_name_clean}' where {column_name_clean}={value}."
            json_rows = [dict(r) for r in rows]
            headers = list(json_rows[0].keys())
            rows_data = [[row.get(h, "") for h in headers] for row in json_rows[:5]]
            table = self._format_as_ascii_table(headers, rows_data)
            return f"Jeff, found {len(rows)} rows. Showing up to 5:\n{table}"
        except Exception as e:
            return f"Jeff, I hit an error searching in '{table_name_clean}': {str(e)}"

    def _sum_column_in_table(self, table_name: str, column_name: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        column_name_clean = re.sub(r"[^A-Za-z0-9_]", "", column_name)
        if not table_name_clean or not column_name_clean:
            return "Jeff, I couldn't parse the table or column name."
        try:
            with get_db_session() as session:
                row = session.execute(text(f"SELECT SUM({column_name_clean}) as total_sum FROM {table_name_clean}")).fetchone()
            return f"Jeff, the total sum of {column_name_clean} in {table_name_clean} is {row[0] or 0}."
        except Exception as e:
            return f"Jeff, I encountered an error summing column '{column_name_clean}': {str(e)}"

    def _avg_column_in_table(self, table_name: str, column_name: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        column_name_clean = re.sub(r"[^A-Za-z0-9_]", "", column_name)
        if not table_name_clean or not column_name_clean:
            return "Jeff, I couldn't parse the table or column name."
        try:
            with get_db_session() as session:
                row = session.execute(text(f"SELECT AVG({column_name_clean}) as avg_val FROM {table_name_clean}")).fetchone()
            return f"Jeff, the average of {column_name_clean} in {table_name_clean} is {row[0] or 0}."
        except Exception as e:
            return f"Jeff, I encountered an error computing avg of '{column_name_clean}': {str(e)}"

    def _max_column_in_table(self, table_name: str, column_name: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        column_name_clean = re.sub(r"[^A-Za-z0-9_]", "", column_name)
        if not table_name_clean or not column_name_clean:
            return "Jeff, I couldn't parse the table or column name."
        try:
            with get_db_session() as session:
                row = session.execute(text(f"SELECT MAX({column_name_clean}) as max_val FROM {table_name_clean}")).fetchone()
            return f"Jeff, the maximum {column_name_clean} in {table_name_clean} is {row[0]}."
        except Exception as e:
            return f"Jeff, I encountered an error finding max of '{column_name_clean}': {str(e)}"

    def _min_column_in_table(self, table_name: str, column_name: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        column_name_clean = re.sub(r"[^A-Za-z0-9_]", "", column_name)
        if not table_name_clean or not column_name_clean:
            return "Jeff, I couldn't parse the table or column name."
        try:
            with get_db_session() as session:
                row = session.execute(text(f"SELECT MIN({column_name_clean}) as min_val FROM {table_name_clean}")).fetchone()
            return f"Jeff, the minimum {column_name_clean} in {table_name_clean} is {row[0]}."
        except Exception as e:
            return f"Jeff, I encountered an error finding min of '{column_name_clean}': {str(e)}"

    def _insert_new_record(self, table_name: str, assignments: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        if not table_name_clean:
            return "Jeff, I couldn't parse the table name."
        pairs = [p.strip() for p in assignments.split(',')]
        colvals = {}
        for p in pairs:
            match = re.search(r"(\w+)\s*=\s*(.*)", p)
            if match:
                col = match.group(1)
                val = match.group(2).strip().strip('"').strip("'")
                colvals[col] = val
        if not colvals:
            return "Jeff, I couldn't parse any column=values in your statement."
        cols = ", ".join(colvals.keys())
        placeholders = ", ".join([f":{c}" for c in colvals.keys()])
        sql = f"INSERT INTO {table_name_clean} ({cols}) VALUES ({placeholders})"
        try:
            with get_db_session() as session:
                session.execute(text(sql), colvals)
                session.commit()
            return f"Jeff, I inserted a new record into {table_name_clean} with {colvals}."
        except Exception as e:
            return f"Jeff, I had an error inserting into '{table_name_clean}': {str(e)}"

    def _update_record(self, table_name: str, set_part: str, where_part: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        if not table_name_clean:
            return "Jeff, I couldn't parse the table name."
        pairs = [p.strip() for p in set_part.split(',')]
        colvals = {}
        for p in pairs:
            match = re.search(r"(\w+)\s*=\s*(.*)", p)
            if match:
                col = match.group(1)
                val = match.group(2).strip().strip('"').strip("'")
                colvals[col] = val
        if not colvals:
            return "Jeff, I couldn't parse the SET part for your UPDATE."
        wh_match = re.search(r"(\w+)\s*=\s*(['\"]?)(.*)\2", where_part)
        if not wh_match:
            return "Jeff, I couldn't parse your WHERE clause properly. Please specify something like 'where id=15'."
        where_col = wh_match.group(1)
        where_val = wh_match.group(3)
        set_clause = ", ".join([f"{c}=:{c}" for c in colvals.keys()])
        sql = f"UPDATE {table_name_clean} SET {set_clause} WHERE {where_col} = :whereval"
        params = colvals.copy()
        params["whereval"] = where_val
        try:
            with get_db_session() as session:
                res = session.execute(text(sql), params)
                session.commit()
            rowcount = res.rowcount
            return f"Jeff, I updated {rowcount} row(s) in {table_name_clean}."
        except Exception as e:
            return f"Jeff, I had an error updating '{table_name_clean}': {str(e)}"

    def _delete_record(self, table_name: str, where_part: str) -> str:
        table_name_clean = re.sub(r"[^A-Za-z0-9_]", "", table_name)
        if not table_name_clean:
            return "Jeff, invalid table name."
        wh_match = re.search(r"(\w+)\s*=\s*(['\"]?)(.*)\2", where_part)
        if not wh_match:
            return "Jeff, I couldn't parse your WHERE clause. Example: 'delete from X where id=123'."
        where_col = wh_match.group(1)
        where_val = wh_match.group(3)
        sql = f"DELETE FROM {table_name_clean} WHERE {where_col} = :val"
        try:
            with get_db_session() as session:
                res = session.execute(text(sql), {"val": where_val})
                session.commit()
            rowcount = res.rowcount
            return f"Jeff, I deleted {rowcount} record(s) from {table_name_clean}."
        except Exception as e:
            return f"Jeff, I had an error deleting from '{table_name_clean}': {str(e)}"


if __name__ == "__main__":
    agent = ChatAgent(model="gpt-4o")
    agent.run_chat()