from fastapi import FastAPI
from consumer import start_consumer
from cf1400_excel import CF1400Excel

app = FastAPI(title="CF1400 Excel Microservice")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/process")
def process_files():
    converter = CF1400Excel()
    converter.process_unconverted_pdfs()
    return {"message": "Processing completed"}

if __name__ == "__main__":
    start_consumer()
