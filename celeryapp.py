# celery_app.py

from celery import Celery
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Initialize Celery
celery_app = Celery(
    'tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)


# Optional: Configure Celery settings
celery_app.conf.update(
    result_expires=3600,
)
