import logging
import logging.config
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

import polars as pl
import yaml

import utils
from validate.general import ValidationGeneral
from validate.lesion import ValidationLesion

with open("config_log.yaml", "r") as f:
  config = yaml.safe_load(f.read())
  logging.config.dictConfig(config)

logger = logging.getLogger("mainLogger")
handler: RotatingFileHandler = logging.getHandlerByName("file")

handler.doRollover()

PATH_INPUT = os.getenv("PATH_INPUT")
PATH_STORE = os.getenv("PATH_STORE")
PATH_OUTPUT = os.getenv("PATH_OUTPUT")


def _log_critical(msg: str, e: Exception):
  print(msg)
  logger.critical(msg)
  logger.exception(e)


def _compile_output():
  """Compiles parquet files in store into excel file in output."""
  list_store = ["general", "lesion"]

  for store_item in list_store:
    list_df = []
    output_file = Path(PATH_OUTPUT).joinpath(f"validation_{store_item}.xlsx")

    for file_path in Path(PATH_STORE).glob(f"{store_item}/*.parquet"):
      list_df.append(pl.scan_parquet(file_path))

    if len(list_df) == 0:
      continue

    df: pl.LazyFrame = pl.concat(list_df)

    # unnest and stringify data to be written into excel
    df.unnest("fail").with_columns(
      pl.col("data").list.join("; ")
    ).collect().write_excel(output_file, autofit=True)

    print(f"Output saved as {output_file}")

    # clean up store
    for file_path in Path(PATH_STORE).glob(f"{store_item}/*.parquet"):
      os.unlink(file_path)


def main():
  # loop through all *.accdb files and invoke validation classes
  for path in Path(PATH_INPUT).glob("*.accdb"):
    print(f"Validating '{path.stem}'")
    try:
      lf = utils.get_df(path).lazy()
    except Exception as e:
      _log_critical(f"Unhandled exception in utils.get_df(), path: {path}", e)

    try:
      ValidationGeneral(lf, path.stem).run_all()
    except Exception as e:
      _log_critical(f"Unhandled exception in ValidationGeneral object: {path.stem}", e)

    try:
      ValidationLesion(lf, path.stem).run_all()
    except Exception as e:
      _log_critical(f"Unhandled exception in ValidationLesion object: {path.stem}", e)

  _compile_output()


if __name__ == "__main__":
  main()
