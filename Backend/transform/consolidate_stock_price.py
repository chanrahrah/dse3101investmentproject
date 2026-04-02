import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
 
from config import PRICES_FILE_FULL, PRICES_DS_ROOT
 
FILE_PATTERN = "**/part-*.parquet"

TARGET_SCHEMA = pa.schema([
    ("date",      pa.timestamp("ms")),
    ("ticker",    pa.large_string()),
    ("adj_close", pa.float64()),
    ("volume",    pa.float64()),
    ("open",      pa.float64()),
    ("high",      pa.float64()),
    ("low",       pa.float64()),
    ("close",     pa.float64()),
    ("year",      pa.dictionary(pa.int32(), pa.int32())),
])

def cast_to_schema(table, schema):
    for field in schema:
        col_idx = table.schema.get_field_index(field.name)
        if col_idx != -1 and table.schema.field(field.name).type != field.type:
            table = table.set_column(col_idx, field.name, table.column(field.name).cast(field.type))
    return table

def main():
    files = sorted(Path(PRICES_DS_ROOT).glob(FILE_PATTERN))
    print(f"Found {len(files)} files...")

    writer = None
    total_rows = 0

    for fpath in files:
        table = pq.read_table(fpath)
        table = cast_to_schema(table, TARGET_SCHEMA)

        if writer is None:
            writer = pq.ParquetWriter(PRICES_FILE_FULL, TARGET_SCHEMA, compression="snappy")

        writer.write_table(table)
        total_rows += len(table)
        print(f"{fpath}  ({len(table):,} rows)")

    if writer:
        writer.close()

    print(f"\nDone — {total_rows:,} total rows → {PRICES_FILE_FULL}")
 
 
if __name__ == "__main__":
    main()