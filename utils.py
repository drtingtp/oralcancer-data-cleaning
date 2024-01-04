import polars as pl

from pathlib import Path


def _get_conn_str(file_path: Path) -> str:
  driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
  conn_str = f"DRIVER={driver};DBQ={file_path};"
  return conn_str


def get_df(path: Path) -> pl.DataFrame:
  df = pl.read_database(
    query="SELECT * FROM [DATA SHEET];",
    connection=_get_conn_str(path),
    execute_options={"max_text_size": 100},  # for long text fields / varchar(max)
    schema_overrides={
      "DATESCREEN": pl.Date,
      "DATEBIRTH": pl.Date,
      "DATE REFERRED": pl.Date,
      "SPECIALIST APPT DATE": pl.Date,
      "DATE SEEN BY SPECIALIST": pl.Date,
      "DATE REFERRED QUIT SER": pl.Date,
      "TARIKH TEMUJANJI QUIT SERVICE": pl.Date,
    },
  )

  return df
