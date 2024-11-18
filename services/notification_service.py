# notification_service.py

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
import logging

# Initialize Slack client
client = WebClient(token=os.getenv('SLACK_TOKEN'))
SLACK_CHANNEL = '#server'  # Replace with your Slack channel name

def send_slack_message(message):
    """
    Sends a message to the configured Slack channel.

    Args:
        message (str): The message text to send.
    """
    try:
        response = client.chat_postMessage(channel=SLACK_CHANNEL, text=message)
        logging.info("Sent Slack notification.")
    except SlackApiError as e:
        logging.error(f"Slack API Error: {e.response['error']}")