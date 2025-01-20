import json
import os
import pdfplumber
import pytesseract
from PIL import Image
import io
import logging
from openai import OpenAI
from utilities.singleton import SingletonMeta
logger = logging.getLogger('dropbox')


class OCRService():

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.logger = logger
            self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            self.logger.info('OCR Service initialized')
            self._initialized = True

    def extract_text_from_file(self, file_data: bytes) -> str:
        """Extract text from a file (invoice, receipt, or W-9)."""
        try:
            image = Image.open(io.BytesIO(file_data))
            text = pytesseract.image_to_string(image)
            return text
        except Exception as e:
            logger.error(f'OCR extraction failed: {e}')
            return ''

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
        for (i, line) in enumerate(lines):
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
        return details

    def extract_info_with_openai(self, text):
        messages = [{'role': 'system', 'content': "You are an AI assistant that extracts information from financial documents for a production company / digital creative studio. Extract the following details from the text:\n                   Invoice Date (Formatted as YYYY-MM-DD),  Total Amount, Payment Term.\n                   Respond with pure, parsable, JSON (no leading or trailing apostrophes) with keys: 'invoice_date', 'total_amount', 'payment_term' If any fields are empty make their value None"}, {'role': 'user', 'content': text}]
        response = self.client.chat.completions.create(model='gpt-3.5-turbo', messages=messages, max_tokens=1000, temperature=0)
        extracted_info = response.choices[0].message.content.strip()
        try:
            info = json.loads(extracted_info)
            return (info, None)
        except json.JSONDecodeError:
            logging.error('Failed to parse JSON from OpenAI response')
            return (None, 'json_decode_error')
        except Exception as e:
            logging.error(f'An error occurred in extract_info_with_openai: {e}')
            return (None, 'unknown_error')

    def extract_receipt_info_with_openai(self, text):
        messages = [{'role': 'system', 'content': "You are an AI assistant that extracts information from receipts.\n                    Extract the following details from the text: \n                    Total Amount (numbers only, no symbols), \n                    Date of purchase (format YYYY-MM-DD), and \n                    generate a description (summarize to 20 characters maximum). \n                    If the total is a refund then the value should be negative. \n                    Provide the information in JSON format with keys: 'total_amount', 'description', 'date'."}, {'role': 'user', 'content': text}]
        response = self.client.chat.completions.create(model='gpt-3.5-turbo', messages=messages, max_tokens=1000, temperature=0)
        extracted_info = response.choices[0].message.content.strip()
        extracted_info_clean = extracted_info.replace('```json', '').replace('```', '').strip()
        try:
            info = json.loads(extracted_info_clean)
            return info
        except json.JSONDecodeError:
            logging.error('Failed to parse JSON from OpenAI response')
            return None

    def extract_text(self, local_file_path: str) -> str:
        """
        Extracts text from a PDF or image file using OCR or direct PDF text extraction.

        :param local_file_path: The local file path of the document or image.
        :return: A string containing all text extracted from the file.
        """
        (_, ext) = os.path.splitext(local_file_path.lower())
        text_content = ''
        try:
            if ext == '.pdf':
                logging.info(f'🔎 [OCRService] Processing PDF with pdfplumber: {local_file_path}')
                with pdfplumber.open(local_file_path) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text() or ''
                        text_content += page_text + '\n'
            else:
                logging.info(f'🔎 [OCRService] Processing image with pytesseract: {local_file_path}')
                image = Image.open(local_file_path)
                text_content = pytesseract.image_to_string(image)
        except Exception as e:
            logging.error(f'❌ [OCRService] Failed to extract text from file {local_file_path}: {e}', exc_info=True)
        return text_content.strip()