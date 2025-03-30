import time
import schedule
from fastapi import FastAPI
from cf1400_excel import CF1400Excel
import yaml

app = FastAPI(title="CF1400 Excel Microservice")

def load_config():
    with open("configuration.yaml", "r") as f:
        return yaml.safe_load(f)
    
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/process")
def process_files():
    converter = CF1400Excel()
    converter.process_unconverted_pdfs()
    return {"message": "Processing completed"}

def job():
    print("[Job] Running PDF check...")
    converter = CF1400Excel()
    converter.process_unconverted_pdfs()

def main():
    config = load_config()
    scheduler_enabled = config.get("scheduler", {}).get("enabled", True)
    interval = config.get("scheduler", {}).get("interval_minutes", 10)

    if scheduler_enabled:
        print(f"[Scheduler] Enabled. Checking for new PDFs every {interval} minutes.")
        schedule.every(interval).minutes.do(job)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        print("[Run-Once] Scheduler disabled. Running one-time PDF check...")
        job()

if __name__ == "__main__":
    main()

