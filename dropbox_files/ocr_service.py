# services/ocr_service.py
import json
import os

import pytesseract
from PIL import Image
import io
import logging

from openai import OpenAI

from singleton import SingletonMeta

logger = logging.getLogger("app_logger")

class OCRService(metaclass=SingletonMeta):

    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Set up logging
            self.logger = logging.getLogger("app_logger")
            self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

            self.logger.info("OCR Service initialized")
            self._initialized = True


    def extract_text_from_file(self, file_data: bytes) -> str:
        """Extract text from a file (invoice, receipt, or W-9)."""
        try:
            image = Image.open(io.BytesIO(file_data))
            text = pytesseract.image_to_string(image)
            return text
        except Exception as e:
            logger.error(f"OCR extraction failed: {e}")
            return ""

    def extract_text_from_invoice(self, file_data: bytes) -> str:
        """Extract text specifically from an invoice file."""
        return self.extract_text_from_file(file_data)

    def parse_invoice_details(self, text_data: str) -> dict:
        """Parse invoice details from extracted text."""
        details = {}
        lines = text_data.split('\n')
        for line in lines:
            if 'Invoice Number:' in line:
                details['invoice_number'] = line.split(':')[1].strip()
            elif 'Total Amount:' in line:
                details['total_amount'] = float(line.split(':')[1].strip().replace('$', ''))
        return details

    def extract_text_from_w9(self, file_data: bytes) -> str:
        """Extract text from a W-9 form."""
        return self.extract_text_from_file(file_data)

    def parse_w9_details(self, text_data: str) -> dict:
        """Parse details from a W-9 form."""
        details = {}
        lines = text_data.split('\n')
        for i, line in enumerate(lines):
            if 'Name' in line:
                details['name'] = lines[i + 1].strip()
            if 'Tax ID' in line:
                details['tax_id'] = lines[i + 1].strip()
        return details

    def extract_text_from_receipt(self, file_data: bytes) -> str:
        """Extract text from a receipt."""
        return self.extract_text_from_file(file_data)

    def parse_receipt_details(self, text_data: str) -> dict:
        """Parse receipt details from extracted text."""
        details = {}
        # Implement parsing logic specific to receipts
        return details

    def extract_info_with_openai(self, text):
        messages = [
            {
                "role": "system",
                "content": """You are an AI assistant that extracts information from financial documents for a production company / digital creative studio. Extract the following details from the text:
                   Invoice Date (Formatted as YYYY-MM-DD),  Quantity (consider multipliers like days, weeks, hours, x, X, and any other units that may have separate columns to quantity but need to be considered for the total), Rate, Date (You must format the date  as YYYY-MM-DD and if no date found then leave empty), Item Description (summarize to 30 characters maximum, only include roles or item names, exclude project names or other fluff), Account Number (where 5300 is US Labor, 5000 is Cost of Goods Sold, and 5330 is Foreign Contractor). 
                   Invoice Description (use all of the line items you gather to generate a description of the vendor like Location Rental, Gaffer, Director of Photography, Rental House, etc. Use concise descriptions that are common in the creative industry)
                   Some invoices have a separate line for tax, for these situations quantity is 1, description is Tax, rate is the tax amount. Make sure to add this as if it was just another line in the invoice. Some invoices have separate line for discount. for these situations quantity is 1, description is Discount, rate is the discount amount in dollars(if it's only marked as a % use context clues to figure out the dollar amount). Make sure to add this as if it was just another line in the invoice.
                   Respond with pure, parsable, JSON (no leading or trailing apostrophes) with keys: 'invoice_date', 'due_date', 'description' and 'line_items' (where 'line items' is an array of objects where each object in the array has 'quantity', 'rate', 'date', 'item_description', and 'account_number'. Ensure that the total amount for all line items (quantity * rate) matches the invoice's total amount'. If any fields are empty do not include them in the JSON"""
            },
            {"role": "user", "content": text}
        ]

        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",  # or 'gpt-4' if you have access
            messages=messages,
            max_tokens=1000,
            temperature=0
        )

        extracted_info = response.choices[0].message.content.strip()
        # print("OPEN AI INFO", extracted_info)
        # Parse the JSON response

        try:
            info = json.loads(extracted_info)

            return info, None  # No error
        except json.JSONDecodeError:
            logging.error("Failed to parse JSON from OpenAI response")
            return None, 'json_decode_error'
        except Exception as e:
            logging.error(f"An error occurred in extract_info_with_openai: {e}")
            return None, 'unknown_error'

    def extract_receipt_info_with_openai(self, text):

        messages = [
            {
                "role": "system",
                "content": """You are an AI assistant that extracts information from receipts.
                    Extract the following details from the text: 
                    Total Amount (numbers only, no symbols), 
                    Date of purchase (format YYYY-MM-DD), and 
                    generate a description (summarize to 20 characters maximum). 
                    If the total is a refund then the value should be negative. 
                    Provide the information in JSON format with keys: 'total_amount', 'description', 'date'."""
            },
            {"role": "user", "content": text}
        ]

        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",  # or 'gpt-4' if you have access
            messages=messages,
            max_tokens=1000,
            temperature=0
        )

        extracted_info = response.choices[0].message.content.strip()
        # print("RAW DATA", extracted_info)
        extracted_info_clean = extracted_info.replace("```json", "").replace("```", "").strip()

        # Parse the JSON response
        try:
            info = json.loads(extracted_info_clean)
            return info
        except json.JSONDecodeError:
            logging.error("Failed to parse JSON from OpenAI response")
            return None