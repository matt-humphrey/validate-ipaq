import polars as pl
import pointblank as pb
import odyssey.core as od

# Set up config
from config.paths import RAW_DATA, PROCESSED_DATA

type Metadata = dict[str, str|int|dict[int|float, str]]
type MetadataDict = dict[str, Metadata]

def main():
    print("hello world")

if __name__ == "__main__":
    main()