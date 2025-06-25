import odyssey.core as od

from utils import read_data
from harmonise import harmonise_ipaq, clean_sit_variables, recalculate_sit_trunc
from config import DATASETS, INTERIM_DATA, PROCESSED_DATA

def main():
    for dset in DATASETS:
        # Handle G217 separately, as it's a different format to the rest
        if dset == "G217":
            pass
        else:
            file = DATASETS[dset]["file"]
            df, meta = read_data(file, INTERIM_DATA)
            harmonised_df = harmonise_ipaq(dset, df)

        # Additional cleaning required for G222 and G126 for SIT variables
        if dset in ["G222", "G126"]:
            harmonised_df = (
                harmonised_df
                .with_columns(clean_sit_variables(dset))
                .with_columns(recalculate_sit_trunc(dset))
            )

        harmonised_lf = harmonised_df.lazy()
        od.write_sav(PROCESSED_DATA/file, harmonised_lf, meta)

if __name__ == "__main__":
    main()