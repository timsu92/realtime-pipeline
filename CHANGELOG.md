## v0.5.1 (2026-01-14)

### Fix

- **node**: use typeguard to check on types to avoid blind points
- **node**: treat specially to optional/union

## v0.5.0 (2026-01-05)

### Feat

- **node**: ability to check on downstream data
- **node**: verify type of newly coming upstream nodes
- **node**: different behavior when insufficient upstream data is given
- **typing**: retrieve typed upstreams and downstream from type annotations

### Fix

- **node**: Only clear the event after all upstreams have been removed
- **typings**: circular import
- **typing**: Python 3.9 doesn't support "|" in type annotations

### Refactor

- **typing**: conform to python naming convention

## v0.4.0 (2025-09-19)

### Feat

- **progress**: add started property to check status
- **progress**: make context manager control display only
- **progress**: remove nodes when exiting context manager
- **progress**: remove node function
- **progress,node**: allow name to be given from node's name
- **progress**: keep longer records to calculate speed
- **progress**: manager for each node's speed

### Fix

- **progress**: check current state before executing
- **progress**: lock when adding node

### Refactor

- **node**: organize jobs to do before after target
- **node**: simplify deprecated function code

### Perf

- **progress**: skip import type-only imports
- **progress**: skip updates to progress bar if not started
- **node**: remove extra check

## v0.3.1 (2025-08-15)

### Fix

- **node**: add type for data storage

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
