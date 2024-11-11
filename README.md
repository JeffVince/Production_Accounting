# Dropbox Event Processor

![Project Logo](https://i.ibb.co/nr00FPw/Logo.png)

**Dropbox Event Processor** is a robust Python application designed to handle and process events from Dropbox webhooks. It intelligently detects various event types such as file/folder additions, deletions, renames, and moves. The application extracts detailed information from file and folder paths, integrates with a database for event logging, and leverages OpenAI for data extraction from financial documents. Future enhancements include asynchronous data enrichment using Celery.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Features

- **Event Detection**: Accurately identifies and categorizes Dropbox webhook events including:
  - File and Folder Additions
  - File and Folder Deletions (excluded from database)
  - File and Folder Renames
  - File and Folder Moves

- **Data Extraction**:
  - Parses file and folder paths to extract critical information such as `project_name`, `po_number`, `vendor_name`, `vendor_type`, `file_type`, and `file_number`.
  - Utilizes OpenAI to extract structured data from financial documents like invoices and receipts.

- **Database Integration**:
  - Logs relevant events into a database with comprehensive details.
  - Ensures no duplication by accurately detecting and processing rename events.

- **Asynchronous Processing** (Future Enhancement):
  - Integrates Celery for background tasks such as generating Dropbox share links and performing OCR.

- **Robust Error Handling & Logging**:
  - Comprehensive logging for monitoring and debugging.
  - Graceful error handling to ensure continuous operation.

## Architecture

The application is modular, consisting of several key components:

- **Webhook Listener**: Receives and processes Dropbox webhook events.
- **Event Router (`event_router.py`)**: Determines the type of event and routes it accordingly.
- **File Utilities (`file_util.py`)**: Handles parsing and data extraction from file and folder paths.
- **OpenAI Utilities (`openai_util.py`)**: Interfaces with OpenAI API to extract structured information from documents.
- **Celery Tasks (`tasks.py`)**: Manages asynchronous tasks for data enrichment (to be implemented).
- **Database Utilities (`database_util.py`)**: Facilitates interactions with the database for logging events.
- **Dropbox Client (`dropbox_client.py`)**: Manages Dropbox API interactions.

## Technologies Used

- **Programming Language**: Python 3.10+
- **APIs & Services**:
  - [Dropbox API](https://www.dropbox.com/developers/documentation)
  - [OpenAI API](https://openai.com/api/)
- **Libraries & Frameworks**:
  - [Flask](https://flask.palletsprojects.com/) - Web framework for handling webhook events.
  - [Celery](https://docs.celeryproject.org/) - Asynchronous task queue (future implementation).
  - [PyPDF2](https://pypi.org/project/PyPDF2/) - PDF processing.
  - [Pillow](https://pillow.readthedocs.io/) - Image processing.
  - [pdf2image](https://pypi.org/project/pdf2image/) - Convert PDF to images for OCR.
  - [pytesseract](https://pypi.org/project/pytesseract/) - OCR tool.
  - [dotenv](https://pypi.org/project/python-dotenv/) - Environment variable management.
  - [logging](https://docs.python.org/3/library/logging.html) - Logging framework.
- **Database**: (Specify your database, e.g., PostgreSQL, MySQL, SQLite)
- **Version Control**: Git

## Installation

### Prerequisites

- **Python 3.10+**: Ensure Python is installed on your system.
- **pip**: Python package manager.
- **Virtual Environment**: Recommended to create an isolated environment.

### Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/yourusername/dropbox-event-processor.git
   cd dropbox-event-processor
   ```

2. **Create a Virtual Environment**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set Up Environment Variables**

   Create a `.env` file in the project root directory and add the following variables:

   ```env
   DROPBOX_API_TOKEN=your_dropbox_api_token
   OPENAI_API_KEY=your_openai_api_key
   MONDAY_API_TOKEN=your_monday_com_api_token
   DATABASE_URL=your_database_url
   ```

   **Note**: Replace `your_dropbox_api_token`, `your_openai_api_key`, `your_monday_com_api_token`, and `your_database_url` with your actual credentials.

5. **Initialize the Database**

   (Provide specific instructions based on the database being used, e.g., running migrations.)

   ```bash
   # Example for Django ORM
   python manage.py migrate
   ```

## Configuration

- **Dropbox Webhook Setup**: Configure Dropbox to send webhook events to your application's endpoint (e.g., `/dropbox-webhook`).

- **Logging**: Adjust logging configurations in your code as needed to suit your monitoring preferences.

## Usage

1. **Run the Flask Application**

   ```bash
   export FLASK_APP=main.py
   export FLASK_ENV=production  # Or development
   flask run --host=0.0.0.0 --port=5000
   ```

   **Note**: Ensure that your application is accessible to Dropbox's webhook by deploying it to a server or using a tunneling service like [ngrok](https://ngrok.com/) during development.

2. **Webhook Endpoint**

   The application listens for POST requests at `/dropbox-webhook`. Ensure Dropbox is configured to send events to this endpoint.

3. **Processing Events**

   - **Additions**: New files or folders are processed, details are extracted, and relevant information is added to the database.
   - **Renames**: Detected by matching deletion and addition events with the same parent directory and timestamps. Details are updated in the database.
   - **Moves**: Files or folders moved to different directories are logged with their new paths.
   - **Deletions**: Logged but not added to the database as per requirements.

## Project Structure

```
dropbox-event-processor/
├── .env
├── README.md
├── requirements.txt
├── main.py
├── event_router.py
├── file_util.py
├── openai_util.py
├── tasks.py
├── dropbox_client.py
├── database_util.py
├── monday_util.py
├── celery_app.py
└── ... (other modules and files)
```

### Description of Key Files

- **main.py**: Entry point of the Flask application handling webhook events.
- **event_router.py**: Processes and categorizes Dropbox events, adding relevant data to the database.
- **file_util.py**: Contains utility functions for parsing paths, extracting data, and processing files/folders.
- **openai_util.py**: Interfaces with the OpenAI API to extract structured information from documents.
- **tasks.py**: Defines Celery tasks for asynchronous data enrichment (pending implementation).
- **dropbox_client.py**: Manages Dropbox API interactions and cursor management.
- **database_util.py**: Handles database operations for logging events.
- **monday_util.py**: Integrates with Monday.com API for project and task management.
- **celery_app.py**: Configures the Celery application for background task processing.

## Contributing

Contributions are welcome! Please follow these steps to contribute:

1. **Fork the Repository**

2. **Create a Feature Branch**

   ```bash
   git checkout -b feature/YourFeatureName
   ```

3. **Commit Your Changes**

   ```bash
   git commit -m "Add your message"
   ```

4. **Push to the Branch**

   ```bash
   git push origin feature/YourFeatureName
   ```

5. **Open a Pull Request**

   Provide a clear description of the changes and the problem they solve.

## License

This project is licensed under the [MIT License](LICENSE).

## Contact

For any inquiries or

