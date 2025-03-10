"""
xero_auth.py

A minimal Flask script for local dev, allowing HTTP callbacks by setting
OAUTHLIB_INSECURE_TRANSPORT=1, plus saving tokens to .env after successful auth.

Not for production use!
"""
import os
import logging
from flask import Flask, session, redirect, request, make_response
from flask_session import Session
from xero import Xero
from xero.auth import OAuth2Credentials
from xero.constants import XeroScopes
from dotenv import load_dotenv, set_key
load_dotenv('../.env')
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('xero_logger')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'supersecret'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)
CLIENT_ID = os.getenv('XERO_CLIENT_ID', 'YOUR_CLIENT_ID')
CLIENT_SECRET = os.getenv('XERO_CLIENT_SECRET', 'YOUR_CLIENT_SECRET')
CALLBACK_URI = 'http://localhost:5002/xero_callback'
DEFAULT_SCOPES = [XeroScopes.OFFLINE_ACCESS, XeroScopes.ACCOUNTING_TRANSACTIONS, XeroScopes.ACCOUNTING_SETTINGS, XeroScopes.ACCOUNTING_CONTACTS]

@app.route('/')
def index():
    return "<h2>Welcome to the Xero Auth Demo!</h2><p><a href='/start_xero_auth'>Click here to reauthorize Xero tokens</a></p>"

@app.route('/start_xero_auth')
def start_xero_auth():
    credentials = OAuth2Credentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, callback_uri=CALLBACK_URI, scope=DEFAULT_SCOPES)
    authorization_url = credentials.generate_url()
    session['xero_creds_state'] = credentials.state
    logger.info('Redirecting user to Xero authorization URL.')
    return redirect(authorization_url)

@app.route('/xero_callback')
def xero_callback():
    cred_state = session.get('xero_creds_state')
    if not cred_state:
        return make_response('No Xero credential state found. Please start auth again.', 400)
    credentials = OAuth2Credentials(**cred_state)
    full_request_uri = request.url
    credentials.verify(full_request_uri)
    logger.info('Successfully verified tokens with Xero.')
    credentials.set_default_tenant()
    session['xero_creds_state'] = credentials.state
    token_data = credentials.token
    logger.info('Saving tokens to .env file...')
    set_key('../.env', 'XERO_ACCESS_TOKEN', token_data.get('access_token', ''))
    set_key('../.env', 'XERO_REFRESH_TOKEN', token_data.get('refresh_token', ''))
    logger.info('Tenant set. Tokens verified, stored in .env, and ready to use!')
    return '<h2>Authorization Successful!</h2><p>Your tokens are now saved in .env. You can now make Xero API calls with fresh tokens.</p>'

@app.route('/xero_api')
def xero_api():
    cred_state = session.get('xero_creds_state')
    if not cred_state:
        return make_response('No Xero creds. Please reauthorize first!', 400)
    credentials = OAuth2Credentials(**cred_state)
    if credentials.expired():
        logger.debug('Token expired; refreshing now...')
        credentials.refresh()
        session['xero_creds_state'] = credentials.state
        token_data = credentials.token
        logger.info('Refreshed tokens. Saving to .env file...')
        set_key('../.env', 'XERO_ACCESS_TOKEN', token_data.get('access_token', ''))
        set_key('../.env', 'XERO_REFRESH_TOKEN', token_data.get('refresh_token', ''))
    else:
        logger.info('CREDENTIALS ARE FINE')
    xero_client = Xero(credentials)
    return '<h2>Authorization Successful!</h2><p>Your tokens are now saved in .env. You can now make Xero API calls with fresh tokens.</p>'
if __name__ == '__main__':
    app.run(host='localhost', port=5002, debug=True, use_reloader=False)