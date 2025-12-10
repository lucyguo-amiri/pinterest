import os
import urllib.parse as urlparse
from flask import Flask, request, redirect
import requests
from dotenv import load_dotenv
import base64
import json
import time

TOKEN_FILE = "pinterest_tokens.json"

load_dotenv()

PINTEREST_CLIENT_ID = os.environ["PINTEREST_CLIENT_ID"]
PINTEREST_CLIENT_SECRET = os.environ["PINTEREST_CLIENT_SECRET"]
PINTEREST_REDIRECT_URI = os.environ["PINTEREST_REDIRECT_URI"]
PINTEREST_SCOPES = os.environ.get("PINTEREST_SCOPES", "ads:read,ads:write")

AUTH_URL = "https://www.pinterest.com/oauth/"
TOKEN_URL = "https://api.pinterest.com/v5/oauth/token"

app = Flask(__name__)


def build_auth_url() -> str:
    params = {
        "client_id": PINTEREST_CLIENT_ID,
        "redirect_uri": PINTEREST_REDIRECT_URI,
        "response_type": "code",
        "scope": PINTEREST_SCOPES,
    }
    return AUTH_URL + "?" + urlparse.urlencode(params)


def _basic_auth_header() -> str:
    """
    Build Pinterest Basic Auth header from client_id:client_secret
    """
    raw = f"{PINTEREST_CLIENT_ID}:{PINTEREST_CLIENT_SECRET}".encode("utf-8")
    b64 = base64.b64encode(raw).decode("utf-8")
    return f"Basic {b64}"


def exchange_code_for_token(code: str) -> dict:
    """
    Exchange the authorization code for the access token + refresh token
    (Pinterest OAuth 2.0)
    """
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": PINTEREST_REDIRECT_URI,
        "client_id": PINTEREST_CLIENT_ID,
        "client_secret": PINTEREST_CLIENT_SECRET,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": _basic_auth_header(),
    }

    resp = requests.post(TOKEN_URL, headers=headers, data=data)

    # Always print Pinterest's error JSON when something goes wrong
    print("Token endpoint status:", resp.status_code)
    print("Token endpoint body:", resp.text)

    resp.raise_for_status()
    return resp.json()


def refresh_access_token(refresh_token: str) -> dict:
    """
    Refresh an access token using the refresh_token.
    """
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": PINTEREST_CLIENT_ID,
        "client_secret": PINTEREST_CLIENT_SECRET,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": _basic_auth_header(),
    }

    resp = requests.post(TOKEN_URL, headers=headers, data=data)

    print("Refresh endpoint status:", resp.status_code)
    print("Refresh endpoint body:", resp.text)

    resp.raise_for_status()
    return resp.json()


def save_tokens(token_data: dict):
    """Save tokens + timestamp to a local JSON file."""
    token_data = dict(token_data)
    token_data["obtained_at"] = int(time.time())
    with open(TOKEN_FILE, "w") as f:
        json.dump(token_data, f, indent=2)


def load_tokens() -> dict:
    """Load tokens from local JSON file."""
    with open(TOKEN_FILE, "r") as f:
        return json.load(f)


def get_pinterest_token() -> str:
    """
    Return a valid Pinterest access token.

    - Loads tokens from TOKEN_FILE.
    - If near expiry, uses refresh_access_token() to get a new one and saves it.
    - No browser / redirect needed, as long as TOKEN_FILE already exists.
    """
    try:
        tokens = load_tokens()
    except FileNotFoundError:
        raise RuntimeError(
            f"No {TOKEN_FILE} found. You must run the OAuth flow once to create it "
            "(start the Flask app and log in to Pinterest)."
        )

    access_token = tokens.get("access_token")
    refresh_token_val = tokens.get("refresh_token")
    expires_in = tokens.get("expires_in")  # seconds
    obtained_at = tokens.get("obtained_at")

    # If we don't have expiry info, just return what we have
    if not expires_in or not obtained_at:
        return access_token

    now = int(time.time())
    # Refresh 5 minutes before expiry
    if now >= obtained_at + expires_in - 300 and refresh_token_val:
        print("Access token is expired/expiring soon — refreshing...")
        new_tokens = refresh_access_token(refresh_token_val)
        save_tokens(new_tokens)
        access_token = new_tokens.get("access_token")

    return access_token


@app.route("/")
def index():
    """
    Manual OAuth entrypoint for re-authorization.
    """
    return f"""
    <h1>Pinterest OAuth Login</h1>
    <p>Click below to authorize your Pinterest app:</p>
    <a href="{build_auth_url()}" style="display: inline-block; padding: 10px 20px; background: #E60023; color: white; text-decoration: none; border-radius: 5px;">Connect Pinterest</a>
    <br><br>
    <p style="color: #666; font-size: 14px;">
        After clicking, you'll be redirected to Pinterest to authorize the app.<br>
        Make sure your redirect URI matches: <code>{PINTEREST_REDIRECT_URI}</code>
    </p>
    """


@app.route("/callback")
def callback():
    if "error" in request.args:
        return f"Pinterest error: {request.args['error']}", 400

    code = request.args.get("code")
    if not code:
        return "Missing code", 400

    try:
        # Exchange code for an access token
        token_data = exchange_code_for_token(code)

        # Persist token data (access + refresh + obtained_at)
        save_tokens(token_data)

        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        expires_in = token_data.get("expires_in")

        return f"""
        <h2>✅ SUCCESS — Pinterest Token Generated</h2>
        <p><b>Access Token (first 20 chars):</b><br><code>{access_token[:20]}...</code></p>
        <p><b>Refresh Token (first 20 chars):</b><br><code>{refresh_token[:20]}...</code></p>
        <p><b>Expires In:</b> {expires_in} seconds ({expires_in/3600:.1f} hours)</p>
        <p>✅ Tokens saved to <code>{TOKEN_FILE}</code>.</p>
        <p><b>Next step:</b> Run <code>python3 pinterest_reports.py</code> to fetch your data.</p>
        <p style="margin-top: 30px;">
            <a href="/" style="display: inline-block; padding: 10px 20px; background: #666; color: white; text-decoration: none; border-radius: 5px;">Back to Home</a>
        </p>
        """
    except Exception as e:
        return f"""
        <h2>❌ Error exchanging code for token</h2>
        <p><b>Error:</b> {str(e)}</p>
        <p>Check your console output for more details.</p>
        <p style="margin-top: 30px;">
            <a href="/" style="display: inline-block; padding: 10px 20px; background: #666; color: white; text-decoration: none; border-radius: 5px;">Try Again</a>
        </p>
        """, 500


if __name__ == "__main__":
    print("="*60)
    print("🚀 PINTEREST OAUTH SERVER")
    print("="*60)
    print(f"✅ Client ID: {PINTEREST_CLIENT_ID}")
    print(f"✅ Redirect URI: {PINTEREST_REDIRECT_URI}")
    print(f"✅ Scopes: {PINTEREST_SCOPES}")
    print("="*60)
    print("\n👉 Open your browser and go to: http://127.0.0.1:5000")
    print("👉 Click 'Connect Pinterest' to authorize the app")
    print("\n⚠️  Make sure this redirect URI is configured in your Pinterest app settings:")
    print(f"   {PINTEREST_REDIRECT_URI}")
    print("\n")
    
    # Run the Flask server
    app.run(debug=True, port=5000)