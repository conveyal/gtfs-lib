# gtfs-lib [![Build Status](https://travis-ci.org/conveyal/gtfs-lib.svg?branch=master)](https://travis-ci.org/conveyal/gtfs-lib)

A library for loading and saving GTFS feeds of arbitrary size with disk-backed storage.

Based on observations over several years of experience using the OneBusAway GTFS library (which is used by OpenTripPlanner), Conveyal created this new GTFS library to meet our current needs.

The main design goals are:

- Avoid all reflection tricks and work imperatively even if it is a bit verbose
- Allow loading and processing GTFS feeds (much) bigger than available memory
- Introduce the concept of feed IDs, and do not use agency IDs for this purpose.
- Perform extensive syntax and semantic validation of feed contents
- Tolerate and recover from parsing errors, aiming to load the entire file even when values are missing or corrupted

A gtfs-lib GTFSFeed object should faithfully represent the contents of a single GTFS feed file. At least during the initial load, no heuristics are applied to clean up the data. Basic syntax is verified, and any problems encountered are logged in detail. At this stage, fields or entites may be missing, and the data may be nonsensical. Then in an optional post-load validation phase, semantic checks are performed and problems are optionally repaired.

## Usage

gtfs-lib can be used as a Java library or run via the command line.

### Library (maven)

```xml
<dependency>
  <groupId>com.conveyal</groupId>
  <artifactId>gtfs-lib</artifactId>
  <version>${choose-a-version}</version>
</dependency>
```

### Command line

```bash
## download repo
$ git clone https://github.com/conveyal/gtfs-lib.git
$ cd gtfs-lib
## build the jar
$ mvn package
## Run the validation suite on a GTFS file and save the results to json files.
## Note: Change the version number to match the shaded jar file name in /target directory.
$ java -cp gtfs-lib-3.4.0-SNAPSHOT-shaded.jar com.conveyal.gtfs.GTFS --load /path/to/gtfs.zip --validate --json /optional/path/to/results
```

### Load and Validation results

The result from running the load or validate command line option with
the `--json` option is a json file containing summary information about
the feed and the process (load or validate) that was run. The results will
be stored at `[feedId]-[load|validate].json` in the system temp directory
or the optional directory specified with the `--json` option.

#### load.json

```json
{
  "filename" : "/Users/me/files/gtfs.zip",
  "uniqueIdentifier" : "dqzn_ogndwayamkasyatagjfkoa",
  "errorCount" : 0,
  "fatalException" : null,
  "agency" : {
    "rowCount" : 1,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 165
  },
  "calendar" : {
    "rowCount" : 29,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 1503
  },
  "calendarDates" : {
    "rowCount" : 176,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 4904
  },
  "fareAttributes" : {
    "rowCount" : 0,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 0
  },
  "fareRules" : {
    "rowCount" : 0,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 0
  },
  "feedInfo" : {
    "rowCount" : 0,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 0
  },
  "frequencies" : {
    "rowCount" : 0,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 0
  },
  "routes" : {
    "rowCount" : 275,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 42123
  },
  "shapes" : {
    "rowCount" : 715639,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 26076306
  },
  "stops" : {
    "rowCount" : 4702,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 281423
  },
  "stopTimes" : {
    "rowCount" : 2054189,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 128596782
  },
  "transfers" : {
    "rowCount" : 0,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 0
  },
  "trips" : {
    "rowCount" : 46036,
    "errorCount" : 0,
    "fatalException" : null,
    "fileSize" : 4009568
  },
  "loadTimeMillis" : 62746,
  "completionTime" : 1535468396785
}
```

#### validate.json

```json
{
  "fatalException" : null,
  "errorCount" : 1193,
  "declaredStartDate" : null,
  "declaredEndDate" : null,
  "firstCalendarDate" : {
    "year" : 2018,
    "month" : "JUNE",
    "chronology" : {
      "id" : "ISO",
      "calendarType" : "iso8601"
    },
    "dayOfMonth" : 30,
    "dayOfWeek" : "SATURDAY",
    "era" : "CE",
    "dayOfYear" : 181,
    "leapYear" : false,
    "monthValue" : 6
  },
  "lastCalendarDate" : {
    "year" : 2018,
    "month" : "SEPTEMBER",
    "chronology" : {
      "id" : "ISO",
      "calendarType" : "iso8601"
    },
    "dayOfMonth" : 1,
    "dayOfWeek" : "SATURDAY",
    "era" : "CE",
    "dayOfYear" : 244,
    "leapYear" : false,
    "monthValue" : 9
  },
  "dailyBusSeconds" : [ 9999, ... ],
  "dailyTramSeconds" : [ 0, ... ],
  "dailyMetroSeconds" : [ 0, ... ],
  "dailyRailSeconds" : [ 0, ... ],
  "dailyTotalSeconds" : [ 2220, ... ],
  "dailyTripCounts" : [ 1, ... ],
  "fullBounds" : {
    "minLon" : -74.040876,
    "minLat" : 40.572635,
    "maxLon" : -73.779519,
    "maxLat" : 40.762524
  },
  "boundsWithoutOutliers" : {
    "minLon" : 0.0,
    "minLat" : 0.0,
    "maxLon" : 0.0,
    "maxLat" : 0.0
  },
  "validationTime" : 16319
}

```