# This file is part of CF1400 Excel.
# Copyright (C) 2025 Ray Stiegler
# Licensed under the GNU General Public License v3.0
# See LICENSE file for details.

import pdfplumber
import pandas as pd
import psycopg2
import yaml
import logging
import datetime
import pika
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CF1400Excel:

    """
    CF1400Excel handles the processing of CF-1400 PDF files into structured Excel spreadsheets
    and inserts extracted data into a Postgres database.

    Responsibilities:
    - Load and manage configuration settings
    - Query and update metadata from the `cf1400_files` and `cf1400_excel_files` tables
    - Convert tabular data from CF-1400 PDFs into Excel files using `pdfplumber` and `pandas`
    - Insert cleaned data into the `foreign_trade_entrances` table
    - Track which files have already been processed

    Attributes:
        config (dict): Configuration loaded from a YAML file.
        db_config (dict): Subset of config with Postgres connection details.
        downloads_dir (Path): Path to directory containing downloaded CF-1400 PDFs.
        converted_dir (Path): Path to directory where Excel files will be saved.
        pdf_path (Path): Path to the current PDF file being processed.
        excel_path (Path): Path to the Excel file being generated.

    Example usage:
        converter = CF1400Excel()
        converter.process_pdf_file("2024-03_CF1400 - Record of Vessel in Foreign Trade - Entrances.pdf")

    Dependencies:
        - pdfplumber
        - pandas
        - psycopg2
        - PyYAML
    """

    
    INSERT_FOREIGN_TRADE_ENTRANCE_SQL = """
        INSERT INTO foreign_trade_entrances (
            filing_port_code, filing_port_name, manifest_number, filing_date, last_domestic_port,
            vessel_name, last_foreign_port, call_sign_number, imo_number, last_foreign_country,
            trade_code, official_number, voyage_number, vessel_flag, vessel_type_code,
            agent_name, pax, total_crew, operator_name, draft, tonnage,
            owner_name, dock_name, dock_intrans,
            cf1400_excel_file_id
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s
        )
    """

    
    INSERT_EXCEL_FILES_SQL = """
            INSERT INTO cf1400_excel_files (cf1400_file_id, excel_filename, converted_at, notes)
            VALUES (%s, %s, %s, %s)
        """
        
    UPDATE_CF1400_FILE_PROCESSED_SQL = """
        UPDATE cf1400_files
        SET processed_to_excel = TRUE
        WHERE id = %s
    """
    
    SELECT_CF1400_FILE_ID_BY_FILENAME_SQL = """
    SELECT id FROM cf1400_files
    WHERE pdf_filename = %s
    """

    
    def __init__(self, config_path: str = "configuration.yaml"):
        self.config = self.load_config(config_path)
        self.pdf_path = None
        self.excel_path = None
        self.db_config = self.config.get("database", {})
        self.downloads_dir = Path(self.config.get("downloads_dir")).resolve()
        self.converted_dir = Path(self.config.get("converted_dir", "./converted")).resolve()
        self.converted_dir.mkdir(exist_ok=True)

    def load_config(self, path: str) -> dict:
        """Loads configuration from a YAML file."""
        with open(path, 'r') as f:
            return yaml.safe_load(f)
        
    def get_cf1400_file_record(self, pdf_filename: str) -> Optional[int]:
        """
        Queries the cf1400_files table to get the ID for a given PDF filename.
        Returns the ID if found, otherwise None.
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            cur.execute(self.SELECT_CF1400_FILE_ID_BY_FILENAME_SQL, (pdf_filename,))

            result = cur.fetchone()
            cur.close()
            conn.close()

            if result:
                return result[0]
            else:
                logger.warning(f"No cf1400_files record found for PDF: {pdf_filename}")
                return None
        except Exception as e:
            logger.error(f"Failed to query cf1400_files: {e}")
            return None
    
    def get_processed_pdf_filenames(self) -> set:
        """
        Returns a set of PDF filenames that have already been processed into Excel.
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            cur.execute("SELECT pdf_filename FROM cf1400_files WHERE processed_to_excel = TRUE")
            results = cur.fetchall()
            cur.close()
            conn.close()
            return set(row[0] for row in results)
        except Exception as e:
            logger.error(f"Failed to query processed PDFs: {e}")
            return set()
        
    def mark_cf1400_file_processed(self, file_id: int):
        """
        Sets processed_to_excel = TRUE for a given cf1400_files ID.
        """
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            cur.execute(self.UPDATE_CF1400_FILE_PROCESSED_SQL, (file_id,))

            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"Marked cf1400_file ID {file_id} as processed_to_excel = TRUE")
        except Exception as e:
            logger.error(f"Failed to update cf1400_file processed status: {e}")
        
    def log_excel_conversion(self, cf1400_file_id: int, excel_filename: str, notes: str = "") -> Optional[int]:
        """
        Logs the conversion and returns the new ID of the inserted Excel file record.
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            cur.execute(self.INSERT_EXCEL_FILES_SQL + " RETURNING id", (
                cf1400_file_id,
                excel_filename,
                datetime.datetime.now(),
                notes
            ))
            inserted_id = cur.fetchone()[0]

            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"Logged Excel conversion: {excel_filename} (ID: {inserted_id})")
            return inserted_id
        except Exception as e:
            logger.error(f"Failed to log Excel conversion: {e}")
            return None

    def pdf_to_excel(self, pdf_path: Optional[Path] = None, excel_path: Optional[Path] = None) -> Optional[pd.DataFrame]:
        """Extracts tables from a PDF and writes to an Excel file."""
        pdf_path = pdf_path or self.pdf_path
        excel_path = excel_path or self.excel_path
        all_tables = []
        
        if not pdf_path or not pdf_path.exists():
            logger.error(f"PDF file does not exist: {pdf_path}")
            return None

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    table = page.extract_table()
                    if table:
                        if i == 0:
                            headers = table[0]
                            data_rows = table[1:]

                            if len(headers) > len(data_rows[0]):
                                headers = headers[:len(data_rows[0])]

                            df = pd.DataFrame(data_rows, columns=headers)
                        else:
                            df = pd.DataFrame(table[1:], columns=headers)

                        all_tables.append(df)

            if all_tables:
                full_df = pd.concat(all_tables, ignore_index=True)
                full_df.to_excel(excel_path, index=False)
                logger.info(f"Excel file saved to {excel_path}")
                return full_df
            else:
                logger.warning("No tables found in PDF.")
                return None
        except Exception as e:
            logger.error(f"Failed to convert PDF to Excel: {e}")
            return None

    def process_unconverted_pdfs(self):
        """
        Loops through all PDFs in the downloads folder, processes those not marked as processed.
        """
        processed_files = self.get_processed_pdf_filenames()
        logger.info(f"Already processed files: {processed_files}")

        for pdf_file in self.downloads_dir.glob("*.pdf"):
            pdf_filename = pdf_file.name

            if pdf_filename in processed_files:
                logger.info(f"Skipping already processed file: {pdf_filename}")
                continue

            logger.info(f"Processing new file: {pdf_filename}")

            # Find cf1400_file_id
            file_id = self.get_cf1400_file_record(pdf_filename)
            if not file_id:
                logger.warning(f"No DB record found for {pdf_filename}. Skipping.")
                continue

            # Set paths
            self.pdf_path = pdf_file
            self.excel_path = self.converted_dir / (pdf_file.stem + "_Converted.xlsx")

            # Convert and insert
            df = self.pdf_to_excel(self.pdf_path, self.excel_path)
            if df is not None:
                excel_file_id = self.log_excel_conversion(file_id, self.excel_path.name)
                if excel_file_id:
                    self.insert_to_database(df, excel_file_id)
                    self.mark_cf1400_file_processed(file_id)


    def insert_to_database(self, df: pd.DataFrame, excel_file_id: int):
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            for index, row in df.iterrows():
                try:
                    first_col = str(row[0]).strip()
                    if '-' in first_col:
                        continue
                    cur.execute(self.INSERT_FOREIGN_TRADE_ENTRANCE_SQL, tuple(row) + (excel_file_id,))
                except Exception as row_e:
                    logger.warning(f"Error inserting row {index}: {row_e}")

            conn.commit()
            cur.close()
            conn.close()
            logger.info("Data inserted into database.")
        except Exception as e:
            logger.error(f"Database connection or insertion error: {e}")
            
    def process_pdf_file(self, filename: str):
        pdf_path = self.downloads_dir / filename
        if not pdf_path.exists():
            logger.warning(f"PDF does not exist: {pdf_path}")
            return

        file_id = self.get_cf1400_file_record(filename)
        if not file_id:
            logger.warning(f"No DB record for {filename}")
            return

        self.pdf_path = pdf_path
        self.excel_path = self.converted_dir / (pdf_path.stem + "_Converted.xlsx")

        df = self.pdf_to_excel(self.pdf_path, self.excel_path)
        if df is not None:
            excel_file_id = self.log_excel_conversion(file_id, self.excel_path.name)
            if excel_file_id:
                self.insert_to_database(df, excel_file_id)
                self.mark_cf1400_file_processed(file_id)
            


if __name__ == "__main__":
    converter = CF1400Excel()
    converter.process_unconverted_pdfs()
