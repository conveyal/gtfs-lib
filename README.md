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
## run the validation suite on a GTFS file and save the result to result.json - change the version number to match file name in /target
$ java -jar target/gtfs-lib-2.2.0-SNAPSHOT-shaded.jar -validate /path/to/gtfs.zip /path/to/result.json
```

### Validation result

The result from running the command line validator is a json file containing
basic info about the feed as well, geographic info (bounding box, plus a merged buffers of the stop
locations), and a list of validation issues.

```json
{
  "fileName": "/path/to/gtfs.zip",
  "validationTimestamp": "Tue Mar 21 11:21:56 EDT 2017",
  "feedStatistics": {
    "feed_id": "feed-id",
    "revenueTime": 14778300,
    "startDate": "2017-04-10",
    "endDate": "2017-07-02",
    "agencyCount": 1,
    "routeCount": 81,
    "stopCount": 3875,
    "tripCount": 8633,
    "frequencyCount": 0,
    "stopTimeCount": 385558,
    "shapePointCount": 246084,
    "fareAttributeCount": 10,
    "fareRuleCount": 186,
    "serviceCount": 3,
    "datesOfService": [
      "2017-04-10",
      "2017-04-11",
      ...
    ],
    "bounds": {
      "west": -122.173638697,
      "east": -121.54902915,
      "south": 36.974922178,
      "north": 37.558388156
    },
    "mergedBuffers": {GeoJSON MultiPolygon},
  },
  "errorCount": 203,
  "errors": [
    {
      "file": "stops",
      "line": 2282,
      "field": "stop_id",
      "affectedEntityId": "3006",
      "errorType": "UnusedStopError",
      "priority": "LOW",
      "stop": {
        "sourceFileLine": 2282,
        "stop_id": "3006",
        "stop_code": "63006",
        "stop_name": "COTTLE & MALONE",
        "stop_desc": null,
        "stop_lat": 37.290717809,
        "stop_lon": -121.895693535,
        "zone_id": "1",
        "stop_url": null,
        "location_type": 0,
        "parent_station": null,
        "stop_timezone": null,
        "wheelchair_boarding": null,
        "feed_id": "feed-id"
      },
      "message": "Stop Id 3006 is not used in any trips.",
      "messageWithContext": "stops line 2282, field 'stop_id': Stop Id 3006 is not used in any trips."
    }
    ...
  ]
} 
```
