import polars as pl
import pointblank as pb
import odyssey.core as od

from utils import validate_ipaq
from config import DATASETS, RAW_DATA, INTERIM_DATA, PROCESSED_DATA

type Metadata = dict[str, str|int|dict[int|float, str]]
type MetadataDict = dict[str, Metadata]

def main():
    prefix = "G220"
    g220 = od.Dataset("G220_Q.sav", INTERIM_DATA)
    lf, _meta = g220.load_data()
    df = lf.collect()

    validate_ipaq(prefix, df).get_tabular_report().write_raw_html("table.html")
    

if __name__ == "__main__":
    main()