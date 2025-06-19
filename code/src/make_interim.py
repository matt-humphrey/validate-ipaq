from odyssey.core import Dataset, write_sav
from config import RAW_DATA, INTERIM_DATA, DATASETS

from typing import Any

def rename_metadata_variables(
    metadata: dict[str, dict[str, Any]], 
    rename_map: dict[str, str],
    ) -> dict[str, dict[str, Any]]:
    """
    Apply variable renaming to nested metadata structure.
    """
    return {
        field: _rename_field_variables(field_dict, rename_map)
        for field, field_dict in metadata.items()
    }

def _rename_field_variables(
    field_dict: dict[str, Any], 
    rename_map: dict[str, str]
    ) -> dict[str, Any]:
    """Rename variables in a single field's dictionary."""
    return {
        rename_map.get(key, key): value 
        for key, value in field_dict.items()
    }

def create_interim_spss_files(
    config: dict[str, Any],
    dataset: str
) -> None:
    """
    Apply changes to create interim files by renaming and deleting specified variables.
    """
    dset = _get_dataset_from_config(config, dataset)
    file, vars_to_delete, vars_to_rename = dset.get("file"), dset.get("delete"), dset.get("rename")
    
    data = Dataset(file, RAW_DATA)
    lf, meta = data.load_data()

    harmonised_lf = (
        lf
        .drop(vars_to_delete)
        .rename(vars_to_rename)
        .sort(by="ID")
    )

    harmonised_meta = rename_metadata_variables(meta, vars_to_rename)

    write_sav(INTERIM_DATA/file, harmonised_lf, harmonised_meta)

def _get_dataset_from_config(
    config: dict[str, Any],
    dataset: str
) -> dict[str, Any]:
    "Return the details for a specific dataset from config."
    return config.get(dataset)


if __name__ == "__main__":
    for dataset in DATASETS:
        create_interim_spss_files(DATASETS, dataset=dataset)