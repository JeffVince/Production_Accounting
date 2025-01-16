#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os
import re
import time

import dotenv
from utilities.logger import setup_logging
from openai import OpenAI

from chat_service import ChatService
from codegen_service import _ai_log_handler  # Access the memory-based log handler

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = setup_logging("agent_logger")

dotenv.load_dotenv()  # load environment variables for OPENAI_API_KEY, etc.


def load_schema_instructions(models_file_path="database/models.py"):
    """
    Reads database/models.py to gather table names and generate a textual summary
    for GPT's system prompt. Also includes synonyms or references if needed.
    """
    if not os.path.exists(models_file_path):
        return "Unable to locate models file for schema instructions."

    with open(models_file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Basic pattern: "class SomeTable(Base):"
    pattern = re.compile(r"class\s+(\w+)\(Base\)")
    matches = pattern.findall(content)
    if not matches:
        return "No SQLAlchemy models found in database/models.py."

    instructions = ["I found these SQLAlchemy models:\n"]
    for model_name in matches:
        instructions.append(f"- {model_name}")

    # (Optional) you can add synonyms, e.g. "purchase orders" => "purchase_order"
    instructions.append(
        "\nIf user says 'purchase orders', interpret as 'purchase_order'. "
        "If user says 'parent of detail_item', interpret as 'purchase_order'.\n"
        "For best practices or coding strategy, respond with action='other'."
    )
    return "\n".join(instructions)


class AgentApp:
    """
    Main Agent that interacts with the user via console:
      - Interprets user input with GPT
      - Decides what DB or code updates to do
      - Summarizes logs in near real time
    """

    def __init__(self):
        self.logger = logging.getLogger("agent_logger")
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.chat_service = ChatService()
        self.schema_docs = load_schema_instructions()

        # Track the most recently used table name for references like "that table"
        self.current_table_name = None

    def chat_loop(self):
        print("AI Agent: Hi Jeff, how can I help you today? Type 'quit' or Ctrl+C to exit.")
        while True:
            user_input = input("\nYou: ").strip()
            if user_input.lower() in ["quit", "exit"]:
                print("AI Agent: Take care, Jeff!")
                break

            # 1) Interpret user input -> JSON
            intent = self._interpret_intent(user_input)

            # 2) Handle/execute
            self._handle_intent(intent, user_input)

            # 3) Summarize logs from memory
            self._stream_and_clear_recent_logs()

    def _interpret_intent(self, user_text: str) -> dict:
        """
        Use GPT to parse user input into a structured JSON, or fallback to 'other'.
        """
        system_prompt = f"""\
You are an AI that handles DB requests or best practices.
Return JSON with:
  action in [create_table, update_table, delete_table, insert_data, update_data, query_data, other]
  table_name
  columns_or_updates
  where
  filters

Below is the current schema context:
{self.schema_docs}
"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text}
        ]
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=messages,
                max_tokens=512,
                temperature=0
            )
            content = response.choices[0].message.content.strip()
            parsed = json.loads(content)
            return parsed
        except Exception as e:
            self.logger.error(f"Error parsing user intent: {e}", exc_info=True)
            return {"action": "other", "info": user_text}

    def _handle_intent(self, intent: dict, user_text: str):
        action = intent.get("action", "other")
        table_name = intent.get("table_name", "")
        columns_or_updates = intent.get("columns_or_updates", {})
        where_clause = intent.get("where", {})
        filters = intent.get("filters", {})

        # Possibly handle "that table" references, etc.
        table_name = self._resolve_table_name(table_name)

        if action == "create_table":
            self.chat_service.create_table(table_name, columns_or_updates)
            self.current_table_name = table_name
            print(f"AI Agent: Created table '{table_name}' successfully!")
        elif action == "update_table":
            self.chat_service.update_table(table_name, columns_or_updates)
            self.current_table_name = table_name
            print(f"AI Agent: Updated table '{table_name}' successfully!")
        elif action == "delete_table":
            self.chat_service.delete_table(table_name)
            print(f"AI Agent: Deleted table '{table_name}'.")
        elif action == "insert_data":
            self.chat_service.insert_data(table_name, columns_or_updates)
            print(f"AI Agent: Inserted data into '{table_name}'.")
        elif action == "update_data":
            self.chat_service.update_data(table_name, columns_or_updates, where_clause)
            print(f"AI Agent: Updated data in '{table_name}'.")
        elif action == "query_data":
            results = self.chat_service.query_data(table_name, filters)
            print(f"AI Agent: Here are your query results: {results}")
        else:
            self.logger.info("No direct DB action recognized; treating as general question.")
            self.chat_service.answer_general_question(user_text)

    def _resolve_table_name(self, table_name: str) -> str:
        if not table_name and self.current_table_name:
            return self.current_table_name
        return table_name

    def _stream_and_clear_recent_logs(self):
        """
        Reads new logs from the AiLogCaptureHandler, prints them line by line as a summary.
        This helps the user see success/failure in near real time.
        """
        logs = _ai_log_handler.get_logs()
        if logs:
            for line in logs:
                # You can parse line content to display only relevant data.
                if "ERROR" in line or "WARNING" in line:
                    print(f"AI Agent (log): {line}")
                elif "INFO" in line:
                    print(f"AI Agent (log): {line}")
                else:
                    # or hide debug lines if you'd like
                    pass
            _ai_log_handler.clear_logs()

if __name__ == "__main__":
    app = AgentApp()
    app.chat_loop()