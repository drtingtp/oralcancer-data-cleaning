import os
from pathlib import Path

import polars as pl

import utils
from validate.general import ValidationGeneral

PATH_INPUT = os.getenv("PATH_INPUT")
PATH_STORE = os.getenv("PATH_STORE")
PATH_OUTPUT = os.getenv("PATH_OUTPUT")


def _compile_output():
  """Compiles parquet files in store into excel file in output."""
  list_store = ["general"]

  for store_item in list_store:
    list_df = []
    output_file = Path(PATH_OUTPUT).joinpath(f"validation_{store_item}.xlsx")

    for file_path in Path(PATH_STORE).glob(f"{store_item}/*.parquet"):
      list_df.append(pl.scan_parquet(file_path))

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
      print(f"Exception occured for get_df(): {path}")
      print(e)

    ValidationGeneral(lf, path.stem).run_all()

    # ValidationLesion(lf, path.stem).run_all()

  _compile_output()


if __name__ == "__main__":
  main()
