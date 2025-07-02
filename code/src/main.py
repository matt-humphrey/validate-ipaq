from odyssey.core import write_sav

from utils import read_data, update_metadata
from harmonise_long import harmonise_ipaq_long
from harmonise import harmonise_ipaq, clean_sit_variables, recalculate_sit_trunc
from config import DATASETS, INTERIM_DATA, PROCESSED_DATA, METADATA, LONG_METADATA

def main():
    for dset in DATASETS:
        file = DATASETS[dset]["file"]
        df, meta = read_data(file, INTERIM_DATA)
        if dset == "G217":
            harmonised_df = harmonise_ipaq_long(dset, df)
            new_meta = LONG_METADATA
        else:
            harmonised_df = harmonise_ipaq(dset, df)
            new_meta = METADATA

        # Additional cleaning required for G222 and G126 for SIT variables
        if dset in ["G222", "G126"]:
            harmonised_df = (
                harmonised_df
                .with_columns(clean_sit_variables(dset))
                .with_columns(recalculate_sit_trunc(dset))
            )

        harmonised_lf = harmonised_df.lazy()
        harmonised_meta = update_metadata(harmonised_lf, meta, new_meta)

        write_sav(PROCESSED_DATA/file, harmonised_lf, harmonised_meta)

if __name__ == "__main__":
    main()