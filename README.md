# gtfs-lib

A library for loading and saving GTFS feeds of arbitrary size with disk-backed storage.
[![Build Status](https://travis-ci.org/conveyal/gtfs-lib.svg?branch=master)](https://travis-ci.org/conveyal/gtfs-lib)

Based on observations over several years of experience using the OneBusAway GTFS library (which is used by OpenTripPlanner), Conveyal created this new GTFS library to meet our current needs.

The main design goals are:

- Avoid all reflection tricks and work imperatively even if it is a bit verbose
- Allow loading and processing GTFS feeds (much) bigger than available memory
- Perform extensive syntax and semantic validation of feed contents
- Tolerate and recover from parsing errors, aiming to load the entire file even when values are missing or corrupted

A gtfs-lib GTFSFeed object should faithfully represent the contents of a single GTFS feed file. At least during the initial load, no heuristics are applied to clean up the data. Basic syntax is verified, and any problems encountered are logged in detail. At this stage, fields or entites may be missing, and the data may be nonsensical. Then in an optional post-load validation phase, semantic checks are performed and problems are optionally repaired.
