# CF1400 Excel Microservice

This microservice processes U.S. Customs CF 1400 PDF files, converts them to Microsoft Excel format, and stores structured trade data in a PostgreSQL database. It avoids reprocessing previously converted files and is built for integration with a broader data pipeline.

---

## 📄 What Is a CF 1400 File?

A CF 1400 (U.S. Customs Form 1400) is an official document used to record vessel arrivals at U.S. ports in foreign trade. These forms include key data such as:

- Vessel name and operator
- Port and date of filing
- Manifest number
- Originating foreign port
- IMO and call sign numbers

They are published as monthly PDF reports by U.S. Customs and are essential for trade analysis.
Learn more by visiting: https://www.cbp.gov/document/report/cf-1400-record-vessel-foreign-trade-entrances

---

## ⚙️ Features

- 🧠 Converts CF 1400 PDFs to structured Excel files
- 📥 Reads from a configured downloads folder
- 🧾 Inserts data into a `foreign_trade_entrances` table
- ✅ Tracks which files have been processed
- 🔁 Supports scheduled or on-demand operation
- 🌐 Exposes a FastAPI-based REST API

---

## 🚀 API Endpoints

| Method | Endpoint     | Description                         |
|--------|--------------|-------------------------------------|
| GET    | `/health`    | Health check                        |
| POST   | `/process`   | Triggers processing of unconverted CF 1400 files |

---

## 📁 Folder Structure

