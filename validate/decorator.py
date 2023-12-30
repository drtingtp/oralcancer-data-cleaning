from functools import wraps
import polars as pl


def subset_full_ic(func):
  """Wraps validation function to limit the validation function to only apply to
  subset of rows with full IC numbers.
  """

  @wraps(func)
  def wrapper(lf: pl.LazyFrame):
    lf = lf.filter(pl.col("ICNUMBER").str.contains(r"^\d{12}$"))
    return func(lf)

  return wrapper
