#!/usr/bin/env python3
##################################################################################
##### This script authenticates with the Whoop API and gets a refresh token. #####
##### Run this once to get your refresh token.                               #####
##################################################################################

import requests
import webbrowser
from urllib.parse import urlencode, urlparse, parse_qs
import sys
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import time
import secrets

# Add project root to path to import config_loader
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config_loader import Config


def get_authorization_url(client_id, redirect_url, state):
    """Generate the authorization URL for WHOOP OAuth."""
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_url,
        "response_type": "code",
        "scope": "read:recovery read:workout read:sleep read:profile offline",  # offline scope needed for refresh tokens
        "state": state,  # WHOOP requires state parameter (min 8 characters)
    }
    # WHOOP uses /oauth/oauth2/auth endpoint
    auth_url = f"https://api.prod.whoop.com/oauth/oauth2/auth?{urlencode(params)}"
    return auth_url


class CallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler to catch OAuth callback"""

    code = None
    error = None
    state = None
    expected_state = None

    def do_GET(self):
        """Handle GET request from OAuth redirect"""
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)

        # Check state parameter for security
        if "state" in query_params:
            CallbackHandler.state = query_params["state"][0]
            if (
                CallbackHandler.expected_state
                and CallbackHandler.state != CallbackHandler.expected_state
            ):
                CallbackHandler.error = "invalid_state_mismatch"
                self.send_response(400)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h1>State mismatch error</h1></body></html>"
                )
                return

        if "code" in query_params:
            CallbackHandler.code = query_params["code"][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(
                """
            <html>
            <head><title>Authorization Successful</title></head>
            <body>
                <h1>✅ Authorization Successful!</h1>
                <p>You can close this window and return to the terminal.</p>
            </body>
            </html>
            """.encode(
                    "utf-8"
                )
            )
        elif "error" in query_params:
            CallbackHandler.error = query_params["error"][0]
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(
                f"""
            <html>
            <head><title>Authorization Error</title></head>
            <body>
                <h1>❌ Authorization Error</h1>
                <p>Error: {CallbackHandler.error}</p>
                <p>Please check your redirect URL in WHOOP app settings.</p>
            </body>
            </html>
            """.encode()
            )
        else:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"""
            <html>
            <head><title>Waiting for Authorization</title></head>
            <body>
                <h1>Waiting for authorization...</h1>
            </body>
            </html>
            """
            )

    def log_message(self, format, *args):
        """Suppress default logging"""
        pass


def start_callback_server(port=8000):
    """Start a local HTTP server to catch OAuth callback"""
    server = HTTPServer(("localhost", port), CallbackHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server, thread


def exchange_code_for_token(client_id, client_secret, code, redirect_url):
    """Exchange authorization code for access and refresh tokens."""
    # WHOOP uses /oauth/oauth2/token endpoint
    token_url = "https://api.prod.whoop.com/oauth/oauth2/token"

    # WHOOP API expects form-encoded data, not JSON
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_url,  # WHOOP uses "redirect_uri" not "redirect_url"
    }

    # Try with form-encoded data (application/x-www-form-urlencoded)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(token_url, data=payload, headers=headers)

    if response.status_code == 200:
        token_data = response.json()
        return token_data
    else:
        print(f"❌ Error: {response.status_code}")
        print(f"   URL: {token_url}")
        print(f"   Response: {response.text}")

        # Try alternative: JSON format
        print("\n   Trying with JSON format...")
        response_json = requests.post(token_url, json=payload)
        if response_json.status_code == 200:
            print("   ✅ JSON format worked!")
            return response_json.json()
        else:
            print(f"   ❌ JSON also failed: {response_json.status_code}")
            print(f"   Response: {response_json.text}")

        return None


def main():
    print("🚀 WHOOP OAuth Authentication")
    print("=" * 50)

    # Load config
    try:
        config = Config()
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return

    client_id = config.whoop_client_id
    client_secret = config.whoop_client_secret
    redirect_url = config.whoop_redirect_url

    if not client_id or not client_secret:
        print("❌ Error: Please set client_id and client_secret in config.yml")
        return

    # Use localhost redirect URL if not set or if it's the example URL
    if (
        not redirect_url
        or redirect_url == "your_redirect_url"
        or "example" in redirect_url.lower()
    ):
        redirect_url = "http://localhost:8000/callback"
        print(f"⚠️  Using default localhost redirect URL: {redirect_url}")
        print("\n   ⚠️  IMPORTANT: Before continuing, you MUST:")
        print("   1. Go to https://developer.whoop.com/")
        print("   2. Navigate to your app settings")
        print("   3. Add this EXACT redirect URL: http://localhost:8000/callback")
        print("   4. Save the settings")
        print("\n   If localhost doesn't work, try using:")
        print(
            "   - https://whoop.com/example/redirect (register this in WHOOP dashboard)"
        )
        print("   - Or a custom URL scheme like: whoop://example/redirect")
        print("\n   Press Enter to continue after registering the URL...")
        input()
    else:
        print(f"   Using redirect URL from config: {redirect_url}")
        print(
            "   ⚠️  Make sure this URL is registered in your WHOOP Developer Dashboard!"
        )

    # Step 1: Start local callback server
    print("\n1. Starting local callback server...")
    try:
        server, server_thread = start_callback_server(8000)
        print("   ✅ Server started on http://localhost:8000")
    except OSError as e:
        if "Address already in use" in str(e):
            print("   ⚠️  Port 8000 is already in use. Trying port 8001...")
            try:
                server, server_thread = start_callback_server(8001)
                redirect_url = "http://localhost:8001/callback"
                print("   ✅ Server started on http://localhost:8001")
                print("   ⚠️  Make sure to update redirect_url in config.yml to match!")
            except OSError:
                print(
                    "   ❌ Could not start server. Please free up a port or use manual method."
                )
                return
        else:
            print(f"   ❌ Error starting server: {e}")
            return

    # Step 2: Generate state parameter (WHOOP requires min 8 characters)
    state = secrets.token_urlsafe(16)  # Generate a secure random state
    CallbackHandler.expected_state = state  # Store for validation

    # Step 3: Get authorization URL and open browser
    auth_url = get_authorization_url(client_id, redirect_url, state)
    print(f"\n2. Opening browser for authorization...")
    print(f"   URL: {auth_url}\n")
    print("   Please authorize the app in your browser.")
    print(
        "   If you see a 404 error, the redirect URL is not registered in WHOOP settings."
    )
    print("   Waiting for callback...")

    webbrowser.open(auth_url)

    # Step 4: Wait for callback (with timeout)
    timeout = 300  # 5 minutes
    start_time = time.time()
    code = None

    while time.time() - start_time < timeout:
        if CallbackHandler.code:
            code = CallbackHandler.code
            break
        if CallbackHandler.error:
            print(f"\n❌ Authorization error: {CallbackHandler.error}")
            print("   Please check:")
            print("      - Is your redirect_url registered in WHOOP app settings?")
            print("      - Does the redirect_url match exactly (including port)?")
            server.shutdown()
            return
        time.sleep(0.5)

    # Shutdown server
    server.shutdown()
    server.server_close()

    if not code:
        print("\n❌ Timeout: No authorization code received")
        print(
            "\n   Manual method: If you see a redirect URL in your browser with 'code=' in it,"
        )
        print("   you can manually paste it here.")
        print("   Example: http://localhost:8000/callback?code=XXXXXXXXXXXXX")

        manual_url = input(
            "\n   Paste the redirect URL with code here (or press Enter to exit): "
        ).strip()

        if manual_url and "code=" in manual_url:
            # Extract code from manually pasted URL
            try:
                parsed = urlparse(manual_url)
                query_params = parse_qs(parsed.query)
                if "code" in query_params:
                    code = query_params["code"][0]
                    print(f"\n✅ Authorization code extracted: {code[:10]}...")
                else:
                    print("❌ No code found in the URL")
                    return
            except Exception as e:
                print(f"❌ Error parsing URL: {e}")
                return
        else:
            print("❌ No authorization code provided")
            return

    print(f"\n✅ Authorization code received: {code[:10]}...")

    # Step 5: Exchange code for tokens
    print("\n3. Exchanging code for tokens...")
    token_data = exchange_code_for_token(client_id, client_secret, code, redirect_url)

    if token_data:
        print("\n✅ Success! Tokens received:")
        print(f"   Access Token: {token_data.get('access_token', '')[:]}")
        print(f"   Refresh Token: {token_data.get('refresh_token', '')[:]}")
        if "expires_in" in token_data:
            print(f"   Expires In: {token_data['expires_in']} seconds")

        # Update config.yml with refresh token and redirect URL
        if "refresh_token" in token_data:
            config.update("whoop", "refresh_token", value=token_data["refresh_token"])
            config.update("whoop", "redirect_url", value=redirect_url)
            print("\n✅ config.yml updated with refresh_token and redirect_url!")
            print(
                "\n🚀 You can now run: python 1_elt/0_extract/whoop/extract_whoop_data.py"
            )
        else:
            print("\n⚠️  Warning: No refresh_token in response")
            print("   Response:", token_data)
    else:
        print("❌ Failed to get tokens")


if __name__ == "__main__":
    main()
