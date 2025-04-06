import pytest
from unittest.mock import patch, MagicMock
from cf1400_excel import CF1400Excel
from pathlib import Path

@pytest.fixture
def excel_processor():
    return CF1400Excel(config_path="tests/test_config.yaml")

@patch("cf1400_excel.psycopg2.connect")
def test_get_cf1400_file_record(mock_connect, excel_processor):
    mock_cursor = MagicMock()
    mock_connect.return_value.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (123,)

    file_id = excel_processor.get_cf1400_file_record("some_file.pdf")

    assert file_id == 123
    mock_cursor.execute.assert_called_once()

@patch("cf1400_excel.psycopg2.connect")
def test_get_processed_pdf_filenames(mock_connect, excel_processor):
    mock_cursor = MagicMock()
    mock_connect.return_value.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("file1.pdf",), ("file2.pdf",)]

    processed = excel_processor.get_processed_pdf_filenames()

    assert "file1.pdf" in processed
    assert "file2.pdf" in processed
    assert len(processed) == 2

@patch("cf1400_excel.pdfplumber.open")
def test_pdf_to_excel_extracts_data(mock_pdfplumber, excel_processor):
    # Mock PDF with simple table structure
    mock_pdf = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_table.return_value = [["Header1", "Header2"], ["Data1", "Data2"]]
    mock_pdf.pages = [mock_page]
    mock_pdfplumber.return_value.__enter__.return_value = mock_pdf

    excel_processor.pdf_path = Path("tests/fake.pdf")
    excel_processor.excel_path = Path("tests/fake.xlsx")

    with patch("pandas.DataFrame.to_excel"):
        df = excel_processor.pdf_to_excel()

    assert df is not None
    assert not df.empty
    assert "Header1" in df.columns

@patch("cf1400_excel.psycopg2.connect")
def test_mark_cf1400_file_processed(mock_connect, excel_processor):
    mock_cursor = MagicMock()
    mock_connect.return_value.cursor.return_value = mock_cursor

    excel_processor.mark_cf1400_file_processed(123)

    mock_cursor.execute.assert_called_once()
    mock_connect.return_value.commit.assert_called_once()
