import os
import json
from fastapi import FastAPI, Request, BackgroundTasks
import gspread
from google.oauth2.service_account import Credentials
import requests

app = FastAPI(title="Nico Mercer Core")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_sheets_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        return None
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

@app.get("/")
def read_root():
    return {"status": "Nico Core Live & Connected"}

@app.post("/webhooks/upbank")
async def handle_upbank(request: Request, background_tasks: BackgroundTasks):
    payload = await request.json()
    print("Received Webhook Payload:", json.dumps(payload))
    
    event_type = payload.get("data", {}).get("attributes", {}).get("eventType")
    
    if event_type == "TRANSACTION_CREATED":
        # Extract the transaction ID from the webhook relationship
        tx_data = payload.get("data", {}).get("relationships", {}).get("transaction", {}).get("data", {})
        tx_id = tx_data.get("id")
        
        if tx_id:
            background_tasks.add_task(fetch_and_log_up_transaction, tx_id)
        
    return {"status": "received"}

def fetch_and_log_up_transaction(transaction_id: str):
    # Optional: If you want to fetch full details using Up API, or parse directly if available.
    # For now, let's log the event safely to the sheet.
    client = get_sheets_client()
    if not client:
        print("Google Sheets client not configured.")
        return
    
    try:
        sheet = client.open("Nico Mercer - Finance Ledger").worksheet("Transaction Ledger")
        sheet.append_row([transaction_id, "Up Bank Card Swipe", "Logged via Live Webhook"])
        print(f"Successfully logged transaction {transaction_id} to Google Sheets.")
    except Exception as e:
        print(f"Error writing to Google Sheets: {e}")
