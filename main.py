from fastapi import FastAPI, Request, BackgroundTasks

app = FastAPI(title="Nico Mercer Core")

@app.get("/")
def read_root():
    return {"status": "Nico Core Live"}

@app.post("/webhooks/upbank")
async def handle_upbank(request: Request, background_tasks: BackgroundTasks):
    payload = await request.json()
    event_type = payload.get("data", {}).get("attributes", {}).get("eventType")
    
    if event_type == "TRANSACTION_CREATED":
        # Process transaction payload cleanly
        print(f"New transaction detected: {payload}")
        
    return {"status": "received"}
