from collections import namedtuple

list_lesion_cols = [
  "TYPE1",
  "SIZE1",
  "SITE1A",
  "SITE1B",
  "SITE1C",
  "SITE1D",
  "TYPE2",
  "SIZE2",
  "SITE2A",
  "SITE2B",
  "SITE2C",
  "SITE2D",
  "TYPE3",
  "SIZE3",
  "SITE3A",
  "SITE3B",
  "SITE3C",
  "SITE3D",
  "OTHER PATHOLOGY",
  "SIZE OTHER",
  "SITE OTHER A",
  "SITE OTHER B",
  "SITE OTHER C",
  "SITE OTHER D",
]

# define namedtuple to generate col_map collection
ColMap = namedtuple("ColMap", ["id", "type", "size", "site"])

# calculate chunks from `list_lesion_cols`
# used in generating col_map
chunk_size = 6  # 6 descriptors for each lesion
col_list_len = len(list_lesion_cols)

assert (
  col_list_len % chunk_size
) == 0, f"len(col_list): {col_list_len} not divisible by chunk_size: {chunk_size}"

chunks = int(col_list_len / chunk_size)

# save column names as col_map
col_map: list[ColMap] = []

for n in range(chunks):
  i = n * chunk_size  # i -- starting index
  id = None
  if n <= 2:
    id = n + 1
  else:
    id = "other"

  col_map.append(
    ColMap(
      str(id),  # id: 1, 2, other
      list_lesion_cols[i],  # type
      list_lesion_cols[i + 1],  # size
      [  # site: extract 3rd to 6th element in a chunk: i+2 (inclusive) to i+6 (exclusive)
        list_lesion_cols[j]
        for j in range(
          i + 2,
          i + 6,
        )
      ],
    )
  )
