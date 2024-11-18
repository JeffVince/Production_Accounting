from flask import Flask, request, redirect, jsonify
import os
import logging
import requests
import json
import sys
from dotenv import load_dotenv, set_key

# ===============================
# Configuration and Setup
# ===============================

# Determine the path to the .env file one directory up
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(dotenv_path=dotenv_path)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set up Flask app
app = Flask(__name__)

# Xero app credentials from environment variables
CLIENT_ID = os.getenv('XERO_CLIENT_ID')
CLIENT_SECRET = os.getenv('XERO_CLIENT_SECRET')

# Validate that CLIENT_ID and CLIENT_SECRET are set
if not CLIENT_ID or not CLIENT_SECRET:
    raise EnvironmentError(
        "Please set the XERO_CLIENT_ID and XERO_CLIENT_SECRET environment variables in the .env file.")

# Redirect URI must match the one registered in Xero app settings
REDIRECT_URI = "http://localhost:5022/callback"

# Xero OAuth and API endpoints
TOKEN_URL = "https://identity.xero.com/connect/token"
AUTH_URL = "https://login.xero.com/identity/connect/authorize"
API_URL = "https://api.xero.com/api.xro/2.0/Invoices"

# Required OAuth 2.0 scopes
REQUIRED_SCOPES = "accounting.transactions offline_access"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Logs to console
    ]
)

# Sample bill data for testing
sample_bill_data = {
    "Type": "ACCPAY",
    "Contact": {"Name": "Sample Supplier"},
    "Date": "2024-11-16",
    "DueDate": "2024-12-01",
    "LineItems": [
        {
            "Description": "Sample Product",
            "Quantity": 1.0,
            "UnitAmount": 100.0,
            "AccountCode": "5000",
        }
    ],
}


# ===============================
# Token Management Functions
# ===============================

def load_tokens():
    """
    Loads access tokens, refresh tokens, and tenant ID from environment variables.
    Returns:
        Tuple of (access_token, refresh_token, tenant_id)
    """
    access_token = os.getenv('XERO_ACCESS_TOKEN')
    refresh_token = os.getenv('XERO_REFRESH_TOKEN')
    tenant_id = os.getenv('XERO_TENANT_ID')

    if access_token and refresh_token and tenant_id:
        logging.info("Tokens and Tenant ID loaded successfully from .env file.")
        return access_token, refresh_token, tenant_id
    else:
        logging.warning("Tokens and/or Tenant ID not found in .env file.")
        return None, None, None


def save_tokens(access_token, refresh_token, tenant_id):
    """
    Saves access tokens, refresh tokens, and tenant ID to the .env file.
    """
    set_key(dotenv_path, 'XERO_ACCESS_TOKEN', access_token)
    set_key(dotenv_path, 'XERO_REFRESH_TOKEN', refresh_token)
    set_key(dotenv_path, 'XERO_TENANT_ID', tenant_id)
    logging.info("Tokens and Tenant ID saved successfully to .env file.")


def get_tenant_id(access_token):
    """
    Retrieves the Tenant ID by making a GET request to the Xero Connections endpoint.
    Args:
        access_token (str): The current access token.
    Returns:
        str or None: The Tenant ID if found, else None.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    response = requests.get("https://api.xero.com/connections", headers=headers)
    if response.status_code == 200:
        connections = response.json()
        if connections:
            tenant_id = connections[0]['tenantId']  # Assuming single organization
            logging.info(f"Retrieved Tenant ID: {tenant_id}")
            return tenant_id
        else:
            logging.error("No connections found for the access token.")
    else:
        logging.error(f"Failed to retrieve connections: {response.text}")
    return None


# Load tokens and tenant ID at startup
access_token, refresh_token, tenant_id = load_tokens()


# ===============================
# OAuth 2.0 Authorization Flow
# ===============================

@app.route('/')
def authorize():
    """
    Initiates the Xero authorization flow.
    Redirects the user to Xero's authorization URL.
    """
    logging.info("Starting the authorization process...")
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": REQUIRED_SCOPES,
    }
    auth_request_url = f"{AUTH_URL}?{requests.compat.urlencode(params)}"
    logging.info(f"Redirecting to Xero authorization URL: {auth_request_url}")
    return redirect(auth_request_url)


@app.route('/callback')
def callback():
    """
    Handles the Xero callback and exchanges the authorization code for tokens.
    Retrieves and stores the Tenant ID.
    """
    global access_token, refresh_token, tenant_id
    code = request.args.get('code')
    if not code:
        logging.error("Authorization code not provided in callback request.")
        return "Authorization code not provided", 400

    logging.info(f"Authorization code received: {code}")
    token_data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    logging.info("Exchanging authorization code for tokens...")
    response = requests.post(TOKEN_URL, data=token_data)
    if response.status_code == 200:
        tokens = response.json()
        access_token = tokens['access_token']
        refresh_token = tokens['refresh_token']
        tenant_id = get_tenant_id(access_token)
        if tenant_id:
            save_tokens(access_token, refresh_token, tenant_id)
            logging.info("Tokens and Tenant ID successfully obtained and stored.")
            return "Authorization successful! You can now create bills via /create-bill or /create-sample-bill."
        else:
            logging.error("Failed to retrieve Tenant ID after obtaining tokens.")
            return "Failed to retrieve Tenant ID.", 500
    else:
        logging.error(f"Error exchanging code for tokens: {response.text}")
        return f"Error exchanging code: {response.text}", 400


# ===============================
# Token Refresh Function
# ===============================

def refresh_access_token():
    """
    Refreshes the Xero access token using the refresh token.
    Also retrieves and updates the Tenant ID if necessary.
    """
    global access_token, refresh_token, tenant_id
    logging.info("Refreshing access token...")
    token_data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    response = requests.post(TOKEN_URL, data=token_data)
    if response.status_code == 200:
        tokens = response.json()
        access_token = tokens['access_token']
        refresh_token = tokens['refresh_token']
        # Optionally, retrieve tenant ID again if it might change
        tenant_id = get_tenant_id(access_token)
        if tenant_id:
            save_tokens(access_token, refresh_token, tenant_id)
            logging.info("Access token successfully refreshed.")
        else:
            logging.error("Failed to retrieve Tenant ID after refreshing tokens.")
            raise Exception("Failed to retrieve Tenant ID after refreshing tokens.")
    else:
        logging.error(f"Error refreshing token: {response.text}")
        raise Exception(f"Error refreshing token: {response.text}")


# ===============================
# API Endpoints
# ===============================

@app.route('/create-bill', methods=['POST'])
def create_bill():
    """
    Creates a bill in Xero using the provided bill data.
    Expects JSON data in the request body.
    """
    global access_token, tenant_id
    if not access_token or not tenant_id:
        logging.error("Authorization required. Access token or Tenant ID is missing.")
        return "Authorization required. Please visit the home page to authorize.", 401

    bill_data = request.json
    if not bill_data:
        logging.error("No bill data provided in the request.")
        return "No bill data provided", 400

    logging.info("Sending a request to create a bill in Xero...")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Xero-Tenant-Id": tenant_id
    }
    response = requests.post(API_URL, headers=headers, json=bill_data)
    if response.status_code in [200, 201]:
        logging.info("Bill created successfully in Xero.")
        return jsonify(response.json()), response.status_code
    elif response.status_code == 401:
        logging.warning("Access token expired. Attempting to refresh token...")
        try:
            refresh_access_token()
            headers["Authorization"] = f"Bearer {access_token}"
            retry_response = requests.post(API_URL, headers=headers, json=bill_data)
            if retry_response.status_code in [200, 201]:
                logging.info("Bill created successfully after refreshing token.")
                return jsonify(retry_response.json()), retry_response.status_code
            else:
                logging.error(f"Error creating bill after retry: {retry_response.text}")
                return jsonify({"error": retry_response.text,
                                "status_code": retry_response.status_code}), retry_response.status_code
        except Exception as e:
            logging.error(f"Failed to refresh token: {str(e)}")
            return jsonify({"error": "Failed to refresh access token.", "message": str(e)}), 500
    else:
        logging.error(f"Error creating bill: {response.text}")
        return jsonify({"error": response.text, "status_code": response.status_code}), response.status_code


@app.route('/create-sample-bill', methods=['GET'])
def create_sample_bill():
    """
    Creates a bill in Xero using predefined sample data.
    Useful for testing purposes.
    """
    global access_token, tenant_id
    if not access_token or not tenant_id:
        logging.error("Authorization required. Access token or Tenant ID is missing.")
        return jsonify({"error": "Authorization required", "message": "Please visit the home page to authorize."}), 401

    logging.info("Sending a request to create a sample bill in Xero...")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Xero-Tenant-Id": tenant_id
    }
    response = requests.post(API_URL, headers=headers, json=sample_bill_data)

    if response.status_code in [200, 201]:
        try:
            data = response.json()
            logging.info("Sample bill created successfully in Xero.")
            return jsonify(data), response.status_code
        except json.JSONDecodeError:
            logging.error("Received empty or invalid JSON response from Xero.")
            return jsonify({
                "message": "Sample bill created successfully, but received no data from Xero.",
                "raw_response": response.text
            }), response.status_code
    elif response.status_code == 401:
        logging.warning("Access token expired. Attempting to refresh token...")
        try:
            refresh_access_token()
            headers["Authorization"] = f"Bearer {access_token}"
            retry_response = requests.post(API_URL, headers=headers, json=sample_bill_data)
            if retry_response.status_code in [200, 201]:
                try:
                    retry_data = retry_response.json()
                    logging.info("Sample bill created successfully after refreshing token.")
                    return jsonify(retry_data), retry_response.status_code
                except json.JSONDecodeError:
                    logging.error("Received empty or invalid JSON response from Xero after token refresh.")
                    return jsonify({
                        "message": "Sample bill created successfully after refreshing token, but received no data from Xero.",
                        "raw_response": retry_response.text
                    }), retry_response.status_code
            else:
                logging.error(f"Error creating sample bill after retry: {retry_response.text}")
                return jsonify({"error": retry_response.text,
                                "status_code": retry_response.status_code}), retry_response.status_code
        except Exception as e:
            logging.error(f"Failed to refresh token: {str(e)}")
            return jsonify({"error": "Failed to refresh access token.", "message": str(e)}), 500
    else:
        logging.error(f"Error creating sample bill: {response.text}")
        return jsonify({"error": response.text, "status_code": response.status_code}), response.status_code


@app.route('/test', methods=['GET'])
def test_service():
    """
    A test endpoint to validate that the service is running and tokens are accessible.
    """
    logging.info("Test endpoint called.")
    if access_token and tenant_id:
        logging.info("Access token and Tenant ID are available.")
        return jsonify({"status": "Service is running", "access_token_available": True, "tenant_id_available": True})
    else:
        logging.warning("Access token or Tenant ID is not available.")
        return jsonify(
            {"status": "Service is running", "access_token_available": False, "tenant_id_available": False}), 401


# ===============================
# Run the Flask Application
# ===============================

if __name__ == '__main__':
    logging.info("Starting Xero Bill Service...")
    app.run(debug=True, host='0.0.0.0', port=5022)