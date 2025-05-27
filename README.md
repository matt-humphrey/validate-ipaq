---
author: Matt Humphrey
date_created: 2025-05-14
date_completed: 
tags: 
  - validation
  - harmonisation
---

# Validating and Harmonising IPAQ Data

Play around with both Pointblank and Pandera to see which tool is better for both validating and then implementing changes where appropriate (PB might be better for detection, but Pandera for coercing changes).

## Purpose/Aim

Comprehensively test and validate the IPAQ data to identify unexpected results and discrepancies.
To do this, I plan to write a script that identifies all of these discrepancies.
I'd like to get to the point where I can run it automatically on all datasets at once, or at least to specify a dataset, and have it print out the table of validation results.

## TODO

- [ ] Create interim datasets with superfluous variables removed, and variable names amended

- [ ] Remove unnecessary derived variables like "G220_SIT_WD_TRUNC"
- [ ] Remove any comment variables like "G220_IPAQ_SIT_COM"

- [x] Run tests to verify (examples given for vigorous, but also apply to moderate and walking):
  - [x] If VIG_W is None, all subsequent VIG variables are None
    - [x] If VIG_W is 0 or 1, VIG_MINS and VIG_MET should be >= 0 and not None
  - [x] VIG_MINS = VIG_HPD * 60 + VIG_MPD (maxed out at 180 mins)
  - [x] VIG_MET = VIG_MINS * 8 (different factors for intensity - 8 for vig, 4 for mod, 3.3 for walk)
  - [x] TOT_MET = VIG_MET + MOD_MET + WALK_MET
    - This may fail because one of the values is None instead of 0 -> good!
  - [ ] IPAQ_CAT = ...

## G220

