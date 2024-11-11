![Project Logo](https://i.ibb.co/nr00FPw/Logo.png)
# Dropbox Production Accounting Assistant

An automated system that listens to Dropbox webhook events, processes files with OCR and AI-powered text extraction, and updates Monday.com boards to streamline project management workflows.

---

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Folder Structure](#folder-structure)
- [Contributing](#contributing)
- [License](#license)

---

## Features

- **Real-Time Event Handling:** Automatically detects and processes file and folder events from Dropbox.
- **OCR and AI Integration:** Extracts text from files using OCR and processes it with OpenAI's GPT models for data extraction.
- **Monday.com Automation:** Creates and updates items and subitems in Monday.com boards based on processed data.
- **Robust Error Handling:** Comprehensive logging and error management ensure reliability and ease of debugging.
- **Extensible Architecture:** Modular code structure allows for easy enhancements and integration with other services.

---

## Prerequisites

- **Python 3.7+**
- **Dropbox Account:** With the appropriate API credentials and permissions.
- **Monday.com Account:** API token with access to modify boards and items.
- **OpenAI API Key:** For AI-powered text extraction.
- **SQLite:** For the local database (comes pre-installed with Python).
- **Tesseract OCR:** Required for OCR capabilities.

---

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/dropbox-listener.git
   cd dropbox-listener
   ```

2. **Create a Virtual Environment:**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Install Tesseract OCR:**

   - **MacOS:** `brew install tesseract`
   - **Ubuntu:** `sudo apt-get install tesseract-ocr`
   - **Windows:** Download the installer from [here](https://github.com/UB-Mannheim/tesseract/wiki).

---

## Configuration

1. **Environment Variables:**

   Create a `.env` file in the root directory with the following content:

   ```ini
   DROPBOX_APP_KEY=your_dropbox_app_key
   DROPBOX_APP_SECRET=your_dropbox_app_secret
   DROPBOX_REFRESH_TOKEN=your_dropbox_refresh_token
   MY_EMAIL=your_email@example.com
   NAMESPACE_NAME=your_namespace_name
   OPENAI_API_KEY=your_openai_api_key
   MONDAY_API_TOKEN=your_monday_api_token
   ```

2. **Set Up Dropbox Webhook:**

   - Deploy the Flask app to a server accessible via HTTPS.
   - Register the webhook endpoint (`https://yourdomain.com/dropbox-webhook`) in your Dropbox App Console.

3. **Initialize the Database:**

   The database will be automatically initialized when you first run the application.

---

## Usage

1. **Run the Flask Server:**

   ```bash
   python webhook/main.py
   ```

   The server will start listening on `http://0.0.0.0:5001`.

2. **Process Events:**

   The `file_processor.py` script processes pending events. It is designed to run periodically.

   ```bash
   python processors/file_processor.py
   ```

   For continuous processing, consider running it as a background service or using a process manager like `supervisord`.

---

## Folder Structure

```plaintext
Dropbox Listener/
│
├── cursors/
│   └── cursor_<member_id>.json
│
├── processors/
│   ├── event_router.py          # Handles Dropbox event processing
│   ├── file_processor.py        # Processes pending events
│   ├── file_util.py             # Utilities for file handling and OCR
│   ├── monday_util.py           # Interfaces with Monday.com API
│   └── openai_util.py           # Handles OpenAI API interactions
│
├── threading/
│   ├── celeryapp.py             # (Optional) Celery configuration
│   └── tasks.py                 # (Optional) Celery tasks
│
├── webhook/
│   ├── database_util.py         # Database operations
│   ├── dropbox_client.py        # Dropbox client singleton
│   └── main.py                  # Flask app entry point
│
├── .env                         # Environment variables
├── .gitignore                   # Git ignore file
├── automation.log               # Log files
├── dropbox_server.log
├── event_processor.log
├── notification_util.py         # (Optional) Notifications
├── processed_files.db           # SQLite database
├── README.md                    # Project documentation
├── requirements.txt             # Python dependencies
└── token.json                   # Dropbox access token (handled securely)
```

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or feature requests.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

*Embarking on this project was an exciting journey, blending automation with AI to streamline workflows. We hope it brings efficiency and ease to your operations!*

