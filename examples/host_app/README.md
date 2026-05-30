# Strata Host App Fixture

This fixture mimics a host application that receives source files in batches,
stages them into object storage, writes one Strata source manifest, and invokes
Strata through the library API.

It is intentionally local-only. The staged files stand in for object-store
objects, and the JSON source manifest stands in for a host-created batch
description.
