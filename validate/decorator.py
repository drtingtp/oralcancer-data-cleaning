from functools import wraps
import polars as pl


def valid_ic(func):
  """Wraps validation function to limit the validation function to only apply to
  subset of rows with valid IC numbers.

  The lf received must have been passed through `_pipe_validate_ic()` to generate
  the `valid_ic` column.
  """

  @wraps(func)
  def wrapper(lf: pl.LazyFrame):
    lf = lf.filter(pl.col("valid_ic") == True)
    return func(lf)

  return wrapper
