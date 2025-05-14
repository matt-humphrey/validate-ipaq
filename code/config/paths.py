from pathlib import Path

HOME = Path(__file__).parents[2]

DATA = HOME / 'data'
OG_DATA = DATA / 'original'
RAW_DATA = DATA / 'raw'
INTERIM_DATA = DATA / 'interim'
PROCESSED_DATA = DATA / 'processed'