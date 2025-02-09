import os
import json
import logging
import pathlib
import datetime
from dotenv import load_dotenv

from config import Config
from db_util import initialize_database, get_db_session

load_dotenv('../.env')
from flask import Flask, request, jsonify, render_template_string
from sqlalchemy import create_engine, Column, Integer, String, DateTime, text
from sqlalchemy.orm import sessionmaker, declarative_base

# KEY CHANGE: import the new v1 class from openai
from openai import OpenAI

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
logging.basicConfig(
    filename="agent.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logging.info("Starting chat agent...")

# -----------------------------------------------------------------------------
# Database Setup (using SQLAlchemy)
# -----------------------------------------------------------------------------
class DatabaseInterface:
    def __init__(self):
        config = Config()
        db_settings = config.get_database_settings(config.USE_LOCAL)
        initialize_database(db_settings['url'])
        self.session = get_db_session()

    def execute_query(self, sql_query):
        logging.info(f"Executing SQL: {sql_query}")
        try:
            result = self.session.execute(text(sql_query))
            self.session.commit()
            fetched = result.fetchall()
            logging.info(f"Query executed successfully. Result: {fetched}")
            return fetched
        except Exception as e:
            logging.error(f"Database error: {e}")
            self.session.rollback()
            return f"Database error: {e}"

# -----------------------------------------------------------------------------
# File System Interface
# -----------------------------------------------------------------------------
class FileInterface:
    def __init__(self, base_dir="Dropbox Listener"):
        self.base_dir = pathlib.Path(base_dir).resolve()
        if not self.base_dir.exists():
            self.base_dir.mkdir(parents=True)
            logging.info(f"Created base directory: {self.base_dir}")
        else:
            logging.info(f"Using existing base directory: {self.base_dir}")

    def is_safe_path(self, path):
        try:
            resolved_path = pathlib.Path(path).resolve()
            return self.base_dir in resolved_path.parents or resolved_path == self.base_dir
        except Exception as e:
            logging.error(f"Error resolving path {path}: {e}")
            return False

    def read_file(self, filename):
        file_path = self.base_dir / filename
        if not self.is_safe_path(file_path):
            logging.warning(f"Attempt to read file outside allowed directory: {file_path}")
            return "Unauthorized file access attempt."
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            logging.info(f"Read file: {file_path}")
            return content
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            return f"Error reading file: {e}"

    def write_file(self, filename, content):
        file_path = self.base_dir / filename
        if not self.is_safe_path(file_path):
            logging.warning(f"Attempt to write file outside allowed directory: {file_path}")
            return "Unauthorized file write attempt."
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            logging.info(f"Wrote file: {file_path}")
            return f"File '{filename}' written successfully."
        except Exception as e:
            logging.error(f"Error writing file {file_path}: {e}")
            return f"Error writing file: {e}"

# -----------------------------------------------------------------------------
# LLM Integration (OpenAI API Client) - UPDATED for the new library style
# -----------------------------------------------------------------------------
class LLMClient:
    def __init__(self, api_key: str):
        # Instead of using openai.api_key = ...
        # we instantiate the new v1 client from openai import OpenAI
        self.client = OpenAI(api_key=api_key)

    def get_response(self, conversation_history):
        """
        conversation_history is a list of {role: "...", content: "..."} dicts.
        We'll feed that directly into client.chat.completions.create(...)
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4",  # or "gpt-3.5-turbo", "gpt-4o", etc.
                messages=conversation_history,
                temperature=0.2,
                max_tokens=150
            )
            # The new library uses Pydantic models. We can still do:
            reply = response.choices[0].message.content
            logging.info(f"LLM response: {reply}")
            return reply
        except Exception as e:
            logging.error(f"LLM API error: {e}")
            return f"LLM API error: {e}"

# -----------------------------------------------------------------------------
# Chat Agent Orchestrator
# -----------------------------------------------------------------------------
class ChatAgent:
    def __init__(self, llm_client, db_interface, file_interface):
        self.llm_client = llm_client
        self.db_interface = db_interface
        self.file_interface = file_interface
        self.conversation_history = [
            {
                "role": "system",
                "content": (
                    "You are an assistant that can execute database queries and file operations. "
                    "For each user request, return a JSON object with the following keys: "
                    '"action", "sql", "filename", "content", "message".\n'
                    "Allowed actions:\n"
                    "- query_db: Provide an SQL query to be executed.\n"
                    "- file_read: Specify a filename to read.\n"
                    "- file_write: Specify a filename and content to write.\n"
                    "- general: No tool operation needed, just a message.\n"
                    "Example JSON response:\n"
                    '{"action": "query_db", "sql": "SELECT * FROM users;", "message": ""}\n'
                    "Output valid JSON."
                )
            }
        ]

    def process_message(self, user_message):
        logging.info(f"Processing user message: {user_message}")
        self.conversation_history.append({"role": "user", "content": user_message})
        llm_response = self.llm_client.get_response(self.conversation_history)

        try:
            response_json = json.loads(llm_response)
        except Exception as e:
            logging.error(f"Error parsing LLM response as JSON: {e}")
            return f"Error parsing LLM response: {e}. Raw response: {llm_response}"

        action = response_json.get("action", "general")
        if action == "query_db":
            sql_query = response_json.get("sql", "")
            if not sql_query:
                return "No SQL query provided in the response."
            result = self.db_interface.execute_query(sql_query)
            response_json["message"] = f"Database query executed. Result: {result}"
        elif action == "file_read":
            filename = response_json.get("filename", "")
            if not filename:
                return "No filename provided for file read."
            result = self.file_interface.read_file(filename)
            response_json["message"] = f"File read completed. Content: {result}"
        elif action == "file_write":
            filename = response_json.get("filename", "")
            content = response_json.get("content", "")
            if not filename or not content:
                return "Filename or content missing for file write."
            result = self.file_interface.write_file(filename, content)
            response_json["message"] = f"File write completed. Result: {result}"
        elif action == "general":
            response_json["message"] = response_json.get("message", "No additional action performed.")
        else:
            logging.error(f"Unknown action: {action}")
            return f"Unknown action specified: {action}"

        logging.info(f"Final response: {response_json}")
        self.conversation_history.append({"role": "assistant", "content": json.dumps(response_json)})
        return json.dumps(response_json, indent=2)

# -----------------------------------------------------------------------------
# Flask Web Interface
# -----------------------------------------------------------------------------
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "your-api-key-here")
llm_client = LLMClient(api_key=OPENAI_API_KEY)
db_interface = DatabaseInterface()
file_interface = FileInterface()
chat_agent = ChatAgent(llm_client, db_interface, file_interface)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Python-based Chat Agent</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        #chat-box { width: 100%; height: 400px; border: 1px solid #ccc; overflow-y: scroll; padding: 10px; }
        #user-input { width: 80%; padding: 10px; }
        #send-btn { padding: 10px; }
        .message { margin: 5px 0; }
        .user { color: blue; }
        .agent { color: green; }
    </style>
</head>
<body>
    <h1>Python-based Chat Agent</h1>
    <div id="chat-box"></div>
    <br>
    <input type="text" id="user-input" placeholder="Type your message here..." />
    <button id="send-btn">Send</button>
    <script>
        const chatBox = document.getElementById("chat-box");
        const userInput = document.getElementById("user-input");
        const sendBtn = document.getElementById("send-btn");

        function appendMessage(sender, text) {
            const msgDiv = document.createElement("div");
            msgDiv.classList.add("message", sender);
            msgDiv.textContent = sender.toUpperCase() + ": " + text;
            chatBox.appendChild(msgDiv);
            chatBox.scrollTop = chatBox.scrollHeight;
        }

        sendBtn.addEventListener("click", () => {
            const message = userInput.value;
            if (message.trim() === "") return;
            appendMessage("user", message);
            fetch("/chat", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ message: message })
            })
            .then(response => response.json())
            .then(data => {
                appendMessage("agent", data.reply);
            })
            .catch(error => {
                appendMessage("agent", "Error: " + error);
            });
            userInput.value = "";
        });

        userInput.addEventListener("keypress", (e) => {
            if (e.key === "Enter") {
                sendBtn.click();
            }
        });
    </script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json()
    user_msg = data.get("message", "")
    reply = chat_agent.process_message(user_msg)
    return jsonify({"reply": reply})

if __name__ == "__main__":
    app.run(debug=True)
