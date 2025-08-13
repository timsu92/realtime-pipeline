## v0.3.0 (2025-08-14)

### BREAKING CHANGE

- Before, target function need to unpack data from upstream. Now, these data is unpacked in advance
- upstream.subscribe(downstream) will be removed in the future

### Feat

- **node**: change subscription's direction

### Fix

- **node**: add type hints and expand arguments in target function
- **node**: show precise information if no job is provided

### Refactor

- **node**: mark Timestamp as type alias

## v0.2.0 (2025-08-07)

### Feat

- **node**: bypass inherited __init__ params

## v0.1.3 (2025-08-06)

### Feat

- drop Python 3.8 support

### Refactor

- **node**: upgrade syntax
- reformat

## v0.1.2 (2025-07-26)

### Fix

- **depend**: drop Python 3.7 to enhance security

## v0.1.1 (2025-07-26)

### Feat

- **vermin**: support as low as Python 3.7

### Fix

- typo
- **__init__**: import path

### Refactor

- **realtime_pipeline**: remove Python 3.12 only syntax

## v0.1.0 (2025-07-25)

### Feat

- initial commit
