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

## TODO

- [ ] Remove unnecessary derived variables like "G220_SIT_WD_TRUNC"
- [ ] Remove any comment variables like "G220_IPAQ_SIT_COM"
- [ ] Run tests to verify (examples given for vigorous, but also apply to moderate and walking):
  - [ ] If VIG_W is None, all subsequent VIG variables are 0
  - [ ] VIG_MINS = VIG_HPD * 60 + VIG_MPD (maxed out at 180 mins)
  - [ ] VIG_MET = VIG_MINS * 8 (different factors for intensity - 8 for vig, 4 for mod, 3.3 for walk)
  - [ ] TOT_MET = VIG_MET + MOD_MET + WALK_MET
    - This may fail because one of the values is None instead of 0 -> good!

## G220

