# -------------------------------------------------------- #
#   Building Site Model
# -------------------------------------------------------- #
# 4/2/25

Assumptions:
    Customers have locations which may be part of sites
    Assets may have multiple owners

Test Cases:
    1. All Assets for a customer
    2. New assets in last 24hrs
    3. new assets in last 30 days
    4. unassigned assets
    5. assets per building/floor/room

Scale:
    Assets - 10M
    Sites - 1 per building, 5% have more buildings
    Customers have <1000 locations (usually < 100)
    Buildings have 1000's assets
    Locations 1000s, floors x1-, rooms x100
    Customers - 100s


Asset:
    {
        name:,
        asset_id: "A-998467",
        customer_id: "C-39654",
        location_id: "L-99326",
        coordinates: {type: "point", latlong: [71.3485, 35.6985]}},
        category: "Chiller",
        deviceType: "Chiller - model 32",
        make: "Carrier",
        model: "32.1011",
        in_service_date: 2016,
        details: [
            {dtype: "Floor", value: "22"},
            {dtype: "Room", value: "B-2011"},
            {dtype: "Edge-device", value: "A-34665"}
        ],
        currentState: [
            {signal: "temperature - degrees C", value: 46, timestamp: "", alarm: true},
            {signal: "temperature - degrees C", value: 46, timestamp: ""},
            {signal: "temperature - degrees C", value: 46, timestamp: ""}
        ]
    }

Location:
    {
        location_id: "L-38576",
        name : "Wang Center building-A",
        customer_id: "C-395868",
        location_type: "Building", // ["Truck","Ship"]
        modified_at: date,
        modified_by: Brady,
        details: [
           {dtype: "Site", parent_id: "S-39485"},
           {dtype: "coordinates", {type: "point", latlong: [71.3485, 35.6985]}},
           {dtype: "size-sqft", "value": "221000"}
        ],
        address: {
            street
            city,
            state,
            zip,
            country
        }
    }
find everything in a building:
    db.asset.find({"location_id" : <location_id>})
find everything on a floor in a building:
    db.assets.find({"location.tags" : {$elemMatch: {name: "floor", value: "22"}}})
new assets:
    db.assets.find({in_service_date: {$gte: now() - 24.hrs}})
unassigned assets:
    db.assets.find({"location.building_id" : NULL})
location of an asset:
    db.assets.find({asset_id: "A-495867"}).project({"location.tags": 1})
within 10 miles of x:
    db.assets.find({"location.coordinates" : {
            $near: {
                $geometry: {
                    type: "Point" ,
                    coordinates: [ <longitude> , <latitude> ]
                },
                $maxDistance: 16000} (meters)
            }
        })

change to location of a group of assets:
    db.assets.updateMany({"location.building_id: "B-596876", deviceType: "mini_split", "location.tags": {$elemMatch: {name: "floor", value: "22"}}},{
        $set: {building_id: <new_building>, location: <new_location>}
    })


[
  {
    $group:
      /**
       * _id: The id of the group.
       * fieldN: The first field name.
       */
      {
        _id: "customer_id",
        cust: {
          $first: "$customer_id"
        },
        cnt: {
          $sum: 1
        }
      }
  }
]


find everything in a building:
    db.asset.find({"location_id" : "L-2000006"})
find everything on a floor in a building:
    db.asset.find({"location_id" : "L-2000123", "parents" : {$elemMatch: {rtype: "floor", value: 4}}},{name: 1,asset_id: 1,"parents.rtype":1,"parents.value":1 })
new assets:
    db.asset.find({in_service_at: {$gt:new Date(Date.now() - 24*60*60 * 1000)}})
move assset to another building:
    db.asset.updateMany({"location_id" : location_id},{"$set": {"location_id" : newlocation_id, "location" : rec["address"]["location"]}}))
unassigned assets:
    db.asset.find({"location_id" : {$exists: false}})
location of an asset:
    db.asset.aggregate([
        {$match: {_id: "A-2000576"}},
        {$lookup: {
            from: "location"

        }}
    ]
    )

39.00622, -76.72803
within 100 miles of x:
    db.asset.find({"location.coordinates" : {
            $near: {
                $geometry: {
                    type: "Point" ,
                    coordinates: [ -76.72803 , 39.00622 ]
                },
                $maxDistance: 160000} (meters)
            }
        })

geopipe = [
    {$geoNear : {
        near: { type: 'Point', coordinates: [ -32.9156, -117.14392 ] },
        distanceField: 'distance',
        maxDistance: 160000
    }},
    {$project: {
        asset_id: 1, name: 1, distance: 1, _id: 0
    }}
]
db.asset.aggregate(geopipe)

location of an asset:
[
  {
    $match:
      {
        asset_id: "A-1000576"
      }
  },
  {
    $lookup:
      {
        from: "location",
        localField: "location_id",
        foreignField: "location_id",
        as: "building"
      }
  },
  {
    $unwind:
      {
        path: "$building"
      }
  },
  {
    $project:
     {
        asset_id: 1,
        name: 1,
        in_service_at: 1,
        customer_id: 1,
        location_id: 1,
        address: "$building.address"
      }
  }
]


proc1
proc_num = 0
base = base + (batch_size * batch_count * procnum)
top =  base + (batch_size * batch_count * procnum + 1)

proc2
proc_num = 1
base = base + (batch_size * batch_count * procnum)
top =  base + (batch_size * batch_count * procnum + 1)

proc3
proc_num = 2
base = base + (batch_size * batch_count * procnum)
top =  base + (batch_size * batch_count * procnum + 1)


# ------------------------------------------------------------ #
#  Merging with timeseries lynx data
#  5/21/25

Goal - provide a data set that has full metadata and ownership context on devices with telemetry

# Issue 1- Solve server timestamp

Check
[
    {$sample: { size : 100}},
    {$project: {timestamp: 1, server_timestamp: 1, _id: 0, 
        "diff" : {$dateDiff: {startDate: "$timestamp", endDate: "$server_timestamp", unit: "second"}}
    }}
]

[
    {$sample: { size : 100000}},
    {$addFields: {"time_diff" : {$dateDiff: {startDate: "$timestamp", endDate: "$server_timestamp", unit: "second"}}
    }},
    {$project: {timestamp: 1, server_timestamp: 1, _id: 0, time_diff: 1}},
    {$out: "timediff"}
]
- Strategy:
db.flespi.update({$dateDiff: {startDate: "$timestamp", endDate: "$server_timestamp", unit: "second"}: {$gt: 10000}})
loop through records
    if data diff > 10000 - delete
    set: {"metadata.device_id": a correct id, }


#  6/6/25
asset
asset-location: vehicle, container, mobile-dwelling, kiosk
parents: fleet, customer, ship, storage-facility

Big customer = 3000 units
unit on a truck part of a fleet for a customer
location important

# -------------------------------------------------------- #
#   Building IOT Loader Model
# -------------------------------------------------------- #
# 7/29/25

# -- On 8-native cluster

# --------------- Scenario ---------------- #
Demonstrate flexible queries:
- Find all assets in a tenant
- Find all assets from a tenant of a certain model
- Find all assets within a radius of a point
- Find all assets that have been active in the last 24 hours
- Find all assets with out of tolerance values
- Find all assets with alarms
- Find all assets with alarms in the last 24 hours
- Find all assets from a tenant that are inactive
- Find all assets from a tenant that are active
- Find all assets from a tenant that are in a certain state

- Create a trend graph of different telemetry values on a group of assets
  - by geography
  - by asset type

- Put the system under load
  - queries against the asset collection
  - ingestion of telemetry values
  

# -------- Queries ------------- #
Demonstrate flexible queries:
- Find all assets in a tenant
db.asset.find({"tenant.tenant_id": "T-1000001"})
- Find all assets from a tenant of a certain model
db.asset.find({"tenant.tenant_id": "T-1000001", "vendor" : "Chilly Billy"})
- Find all assets within a radius of a point
db.asset.find({"tenant.tenant_id": "T-1000001", "location.coordinates": { $near: { $geometry: { type: "Point", coordinates: [-76.72803, 43.0718] }, $maxDistance: 16000 } } })
- Find all assets that have been active in the last 24 hours
db.asset.find({"tenant.tenant_id": "T-1000001", "last_active_at": { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) } })
- Find all assets with out of tolerance values
db.asset.find({"tenant.tenant_id": "T-1000001", "telemetry": { $elemMatch: { "value": { $gt: 100 } } } })
- Find all assets with alarms
db.asset.find({"tenant.tenant_id": "T-1000001", "alarms": { $elemMatch: { "active": true } } })
- Find all assets with alarms in the last 24 hours
db.asset.find({"tenant.tenant_id": "T-1000001", "alarms": { $elemMatch: { "active": true, "last_active_at": { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) } } } })
- Find all assets from a tenant that are inactive
db.asset.find({"tenant.tenant_id": "T-1000001", "active": false })
- Find all assets from a tenant that are active
db.asset.find({"tenant.tenant_id": "T-1000001", "active": true })
- Find all assets from a tenant that are in a certain state
db.asset.find({"tenant.tenant_id": "tenant_id", "state": "state" })

- Create a trend graph of different telemetry values on a group of assets
  - by geography
  - by asset type

- Put the system under load
  - queries against the asset collection
  - ingestion of telemetry values

 - Indexes:
 [
  { v: 2, key: { _id: 1 }, name: '_id_' },
  {
    v: 2,
    key: { 'tenant.tenant_id': 1, vendor: 1 },
    name: 'tenant.tenant_id_1_vendor_1'
  },
  {
    v: 2,
    key: { 'location.coordinates': '2dsphere' },
    name: 'location.coordinates_2dsphere',
    '2dsphereIndexVersion': 3
  }
]

- Window function
[
  {
    $match:
      {
        timestamp: {
          $gt: ISODate(
            "2024-06-13T15:23:24.437+00:00"
          )
        },
        "measures.device_type_id": 645
      }
  },
  {
    $setWindowFields:
      {
        partitionBy: "$metadata.ident",
        sortBy: {
          timestamp: 1
        },
        output: {
          avgTemp: {
            $avg: "$measures.freezer_air_temperature",
            window: {
              range: [-99, 0],
              unit: "minute"
            }
          }
        }
      }
  }
]

# -- Data Cleanup:
- Null timestamp
1970-01-01T00:00:00.000+00:00
1989-10-28T12:20:34.000+00:00
- MaxTimestamp: 2024-05-19T00:12:32.164+00:00 <- not really!>

[{$match: {server_timestamp: {$lt: ISODate("2024-04-27T05:40:33.000+00:00")}}},
{$count: "numrecords"}]
-> 440000

[{$match: {timestamp: {$lt: ISODate("2024-04-27T05:40:33.000+00:00")}}},
{$count: "numrecords"}]
-> 4441

[{$match: {timestamp: {$lt: ISODate("2024-04-27T05:40:33.000+00:00")}, server_timestamp: {$lt: ISODate("2024-04-27T05:40:33.000+00:00")}}},
{$count: "numrecords"}]

# -- Time Series 
    db.createCollection(
       "telemetry",
       {
          timeseries: {
             timeField: "timestamp",
             metaField: "metadata", // Optional, if you have a field identifying unique series
             granularity: "minutes" // Optional, e.g., "seconds", "minutes", "hours"
          }
       }
    )

- Import from gzip dump:
mongorestore --uri "mongodb+srv://8-native.vmwqj.mongodb.net" --ssl --username main_admin --password $_PWD_ --authenticationDatabase admin dump --db site_data --collection telemetry --gzip --archive=~/Documents/mongodb/customers/Carrier/Lynx/flespi.gz

mdb/bin/mongorestore --uri "mongodb+srv://8-native.vmwqj.mongodb.net" --ssl --username main_admin --password $_PWD_ --authenticationDatabase admin dump --nsInclude="site_data.flespi_base" --nsTo "site_data.telemetry" --gzip --archive=flespi.gz


# -- Find Max Timestamp:
db.flespi_base.aggregate(pipe)
- New Max: 2024-06-13T20:23:24.437+00:00
- New Max: 2024-06-13T20:23:16.355Z - $gt = 1 document
pipe = [
    {$match: {timestamp: {$gt: ISODate("2024-06-13T20:23:24.437+00:00")}}},
    {$project: {timestamp: 1, _id: 0}},
    {$sort: {timestamp: -1}},
    {$limit: 20}
]

db.flespi_base.countDocuments({timestamp: {$gt: ISODate("2024-06-05T00:12:32.164+00:00")}})
> 2989027

# -- dump data:
mongodump --uri "mongodb+srv://claims-demo.vmwqj.mongodb.net" --ssl --username main_admin --password $_PWD_ --authenticationDatabase admin --db telemetry --collection flespi_base
mongorestore --uri "mongodb+srv://8-native.vmwqj.mongodb.net" --ssl --username main_admin --password $_PWD_ --authenticationDatabase admin dump --db site_data

Data Cleanup:
- db.flespi_base.deleteMany({timestamp: {$lt: ISODate("2024-04-27T05:40:33.000+00:00")}})
 acknowledged: true,
  deletedCount: 4441
- db.flespi_base.updateMany({server_timestamp: {$lt: ISODate("2024-04-27T05:40:33.000+00:00")}},{$set: {server_timestamp: "$timestamp", est_timestamp: "yes"}})
matchedCount: 447827,
  modifiedCount: 447827,
- db.flespi_base.aggregate([
    {$group: {_id: "$metadata.ident",
    count: {$sum: 1}}},
    {$out: "temp_idents"}
])
dum cleaned up data:
mdb/bin/mongodump --uri "mongodb+srv://8-native.vmwqj.mongodb.net" --ssl --username main_admin --password $_PWD_ --authenticationDatabase admin --db site_data --collection flespi_base

# get remaining data for Lynx
export _PWD_=LNiPSx51yoW45t1M
mdb/bin/mongodump --archive=flespi.gz --gzip --uri "mongodb+srv://cluster-dev.q1gws.mongodb.net" --ssl --username bradybyrd --password $_PWD_ --authenticationDatabase admin --gzip --db telemetry --collection flespi

- Expand disk:
[ec2-user@ip-172-31-5-88 tools]$ sudo lsblk
NAME      MAJ:MIN RM SIZE RO TYPE MOUNTPOINTS
xvda      202:0    0  50G  0 disk 
├─xvda1   202:1    0  20G  0 part /
├─xvda127 259:0    0   1M  0 part 
└─xvda128 259:1    0  10M  0 part /boot/efi
[ec2-user@ip-172-31-5-88 tools]$ sudo growpart /dev/xvda 1
CHANGED: partition=1 start=24576 old: size=41918431 end=41943007 new: size=104832991 end=104857567

# -- Time Series 
    db.createCollection(
       "signals",
       {
          timeseries: {
             timeField: "timestamp",
             metaField: "metadata",
             granularity: "minutes"
          }
       }
    )

mdb/bin/mongorestore --uri "mongodb+srv://8-native.vmwqj.mongodb.net" --ssl --username main_admin --password $_PWD_ --authenticationDatabase admin --gzip --archive=flespi.gz --nsFrom telemetry.flespi --nsTo site_data.signals
 newIP: 3.136.234.190

- New Data
pipe = [
    {$match: {$and: [{timestamp: {$gt: ISODate("2024-06-13T20:23:24.437+00:00")}},{timestamp: {$lt: ISODate("2024-06-14T20:23:24.437+00:00")}}]}},
    {$project: {timestamp: 1, _id: 0}},
    {$sort: {timestamp: -1}},
    {$limit: 100}
]

pipe = [
    {$match: {$and: [{timestamp: {$gt: ISODate("2024-06-13T20:23:24.437+00:00")}},{timestamp: {$lt: ISODate("2024-06-14T08:23:24.437+00:00")}}]}},
    {$project: {timestamp: 1, _id: 0}},
    {$count: "numrecs"}
]
> 157,000 for 12hr period => 4/sec

- Check for new idents
pipe = [
    {$match: {$and: [{timestamp: {$gt: ISODate("2024-06-13T20:23:24.437+00:00")}},{timestamp: {$lt: ISODate("2024-07-14T20:23:24.437+00:00")}}]}},
    {$group: {_id: "$metadata.ident",
      count: {$sum: 1}}},
    {$out: "temp_idents"}
]
db.flespi.aggregate(pipe)
33700 - idents

check:
{$and: [{timestamp: {$gt: ISODate("2024-06-13T20:23:24.437+00:00")}},{timestamp: {$lt: ISODate("2024-07-14T20:23:24.437+00:00")}}], "metadata.ident: "LOPU5391999"}

Order #A382373419
# -- dump cleaned up data:
mdb/bin/mongodump --archive=flespi.gz --gzip --uri "mongodb+srv://8-native.vmwqj.mongodb.net" --ssl --username main_admin --password $_PWD_ --authenticationDatabase admin --gzip --db site_data --collection flespi_base

ssh -i ../servers/bb_mdb_aws.pem ec2-user@3.12.71.125
scp -i ../servers/bb_mdb_aws.pem ec2-user@3.12.71.125:~/tools/flespi.gz .

- Incorporate metadata.ident into assets

- Ingestion scenario:

- Lookup:
pipe = [
    {$match: {_id: "A-1000068"}},
    {$lookup: {
        from: "flespi_base",
        localField: "identifier",
        foreignField: "metadata.ident",
        pipeline: [{$limit: 100}],
        as: "measurements"
    }}
]
db.asset.aggregate(pipe)


{"metadata.ident": 'EVGU0000372', timestamp: {$gt: ISODate("2024-06-11T20:56:32.244+00:00")}}


[
  {$match:{"metadata.ident": "EVGU0000372","timestamp": { $gt: ISODate("2024-06-11T20:56:32.244+00:00")}}},
  {$sort:{"timestamp": -1}},
  {$limit: 1}
]

- pure agg:
[
  {
    $match:
      /**
       * query: The query in MQL.
       */
      {
        "metadata.ident": "EVGU0000372",
        timestamp: {
          $gt: ISODate(
            "2024-06-11T20:56:32.244+00:00"
          ),
        },
      },
  },
  {
    $sort:
      /**
       * Provide the field name for the count.
       */
      {
        timestamp: -1,
      },
  },
  {
    $limit:
      /**
       * Provide the number of documents to limit.
       */
      1,
  },
  {
    $addFields: {
      "measures.timestamp": {
        $dateAdd: {
          startDate: "$timestamp",
          unit: "second",
          amount: 0,
        },
      },
    },
  },
  {
    $project:
      /**
       * specifications: The fields to
       *   include or exclude.
       */
      {
        measures: 1,
      },
  },
  {
    $unwind:
      /**
       * path: Path to the array field.
       * includeArrayIndex: Optional name for index.
       * preserveNullAndEmptyArrays: Optional
       *   toggle to unwind null and empty values.
       */
      {
        path: "$measures",
      },
  },
]

Filter null:
$project: {
new_measures: {
        $filter: {
          input: "$measures",
          as: "subDoc",
          cond: { $ne: ["$$subDoc.fieldToCheck", null] }
        }

"2024-06-11T20:56:32.244+00:00"
utc_string = "2024-06-11T20:56:32.244+00:00"
format_str_offset = "%Y-%m-%d %H:%M:%S%z"
cur_date = datetime.strptime(utc_string, format_str_offset)


pipeline = [
  {
    $match:
      {
        timestamp: {
          $gt: {
            $dateSubtract: {
              startDate: ISODate("2024-06-14T20:23:24.437+00:00"),
              unit: "day",
              amount: 2
            }
          }
      },
      "measures.device_type_id": 645
    }
  },
  {
    $addFields:
      /**
       * newField: The new field name.
       * expression: The new field expression.
       */
      {
        thresdate: {
          $dateSubtract: {
            startDate: ISODate(
              "2024-06-15T20:23:24.437+00:00"
            ),
            unit: "day",
            amount: 2
          }
        }
      }
  }
]

pipeline = [
  {
    $match:
      {
        timestamp: {
          $gt: {
            $dateSubtract: {
              startDate: ISODate("2024-06-14T20:23:24.437+00:00"),
              unit: "day",
              amount: 2
            }
          }
      }
      }
  }
]

pipeline = [
  {
    $match:
      {
        $expr: { $gt: ["$timestamp", {$dateSubtract: {
              startDate: ISODate("2024-06-14T20:23:24.437+00:00"),
              unit: "day",
              amount: 2}}]
              } 
      }
  }
]

pipeline = [
  {
    $match:
      {
        $expr: { timestamp: {
          $gt: {
            $dateSubtract: {
              startDate: ISODate("2024-06-14T20:23:24.437+00:00"),
              unit: "day",
              amount: 2
            }
          }
      }
      }
  }
]

pipeline2 = [
  {
    $match:
      {
        timestamp: {
          $gt: ISODate("2024-06-14T20:23:24.437+00:00")
          }
      }
  }
]

# ------ Accumulated Fields -------------- #
db.telemetry.aggregate([
  {
    $match:
      {
        timestamp: {$gt: ISODate("2024-06-12T20:23:24.437+00:00")},
        "measures.device_name": "SIMDEVI000107"
      }
  },
  
  {
    $project: {
      fieldNames: { $objectToArray: "$measures" }
    }
  },
  
  {
    $unwind: "$fieldNames"
  },
  
  {
    $group: {
      _id: "$fieldNames.k",
      dataTypes: { $addToSet: { $type: "$fieldNames.v" } },
      sampleValues: { $addToSet: "$fieldNames.v" },
      count: { $sum: 1 }
    }
  },
  
  {
    $project: {
      fieldName: "$_id",
      dataTypes: 1,
      sampleValues: { $slice: ["$sampleValues", 3] }, // First 3 sample values
      occurrenceCount: "$count",
      occurrencePercentage: { 
        $round: [{ $multiply: [{ $divide: ["$count", 1000] }, 100] }, 2] 
      },
      _id: 0
    }
  },
  
  {
    $sort: { occurrenceCount: -1 }
  }
])

- Failes many device_types are NULL
db.telemetry.aggregate([
  {
    $match:
      {
        timestamp: {$gt: ISODate("2024-06-13T20:23:24.437+00:00")},
        "measures.device_type_id": 645
      }
  },
  {
    $group: {
      _id: "measures.device_type_id",
      count: { $sum: 1 }
    }
  }
])

- 12hrs later
db.telemetry.aggregate([
  {
    $match:
      {
        timestamp: {$gt: ISODate("2024-06-13T12:23:24.437+00:00")}
      }
  },
  {
    $group: {
      _id: "$metadata.ident",
      count: { $sum: 1 }
    }
  },
  {$sort: {count: -1}}
])
- Metadata.ident:
[
  { _id: 'SIM00000836', count: 14 },
  { _id: 'EVGU0000112', count: 3 },
  { _id: 'SIM00000414', count: 17 },
  { _id: 'SIM00000811', count: 14 },
  { _id: 'SIM00000154', count: 18 },
  { _id: 'SIM00000022', count: 18 },
  { _id: '36753084', count: 75 },
  { _id: 'CARU0000096', count: 5 },
  { _id: 'SIM00000863', count: 14 },
  { _id: 'CARU0000074', count: 2 },
  { _id: 'EVGU0000487', count: 2 },
  { _id: 'SIM00000951', count: 14 },
  { _id: 'EVGU0000862', count: 3 },
  { _id: 'DHIU9893460', count: 2 },
  { _id: 'DHIU5949719', count: 31 },
  { _id: '4639401', count: 183 },
  { _id: 'EVGU0000053', count: 2 },
  { _id: 'SIM00000346', count: 17 },
  { _id: 'EVGU0000235', count: 3 },
  { _id: 'EVGU0000870', count: 6 }
]

- DeviceNames:
[
  { _id: 'EVGN000000569', count: 2 },
  { _id: '1111908878', count: 142 },
  { _id: 'EVGN000000758', count: 3 },
  { _id: 'D-DHIU5950833', count: 2 },
  { _id: 'SIMDEVI000700', count: 16 },
  { _id: 'EVGN000000017', count: 4 },
  { _id: 'SIMDEVI000981', count: 14 },
  { _id: 'D-DHIU9895570', count: 1 },
  { _id: 'SIMDEVI000542', count: 17 },
  { _id: 'SIMDEVI000107', count: 18 },
  { _id: 'D-DHIU5946643', count: 155 },
  { _id: 'SIMDEVI000855', count: 14 },
  { _id: 'SIMDEVI000653', count: 16 },
  { _id: 'SIMDEVI000245', count: 17 },
  { _id: 'SIMDEVI000356', count: 17 },
  { _id: 'D-DHIU9894872', count: 1 },
  { _id: 'D-DHIU5946509', count: 2 },
  { _id: 'D-DHIU5951974', count: 8 },
  { _id: 'CARD000000010', count: 5 },
  { _id: 'D-DHIU5953848', count: 31 }
]

DeviceType_ids:
[
  { _id: null, count: 36961 },
  { _id: 4, count: 651 },
  { _id: 730, count: 13313 },
  { _id: 645, count: 12660 }
]


# --------------------- As Update ------------------------- #
# - TODO: add primary field - updated_at

# -- Goal

{
  {
    measures.container_O2Setpoint: {value: null, updated: "2024-06-13T20:26:03.437+00:00"},
    measures.container_Power: {value: '81.78', updated: "2024-06-13T20:26:03.437+00:00"},
    measures.container_PowerStatus: {value: 1, updated: "2024-06-13T20:26:03.437+00:00"},
    measures.container_QuestState:{value: 0, updated: "2024-06-13T20:26:03.437+00:00"},
    measures.container_RTS:{value: -14.42, updated: "2024-06-13T20:26:03.437+00:00"}
  }
}


db.getCollection('telemetry').aggregate(
  [
    {
      $match: {
        timestamp: {
          $gt: ISODate('2024-06-12T20:23:24.437Z')
        },
        'metadata.ident': 'SIM00000107'
      }
    },
    {
      $addFields: { 'measures.ts': '$timestamp' }
    },
    {
      $project: {
        fieldNames: {
          $objectToArray: '$measures'
        },
        ts: '$timestamp'
      }
    },
    { $unwind: { path: '$fieldNames' } },
    {
      $project: {
        key: '$fieldNames.k',
        value: '$fieldNames.v',
        ts: '$ts'
      }
    },
    { $match: { value: { $ne: null } } },
    {
      $group: {
        _id: '$key',
        ex: { $addToSet: '$value' },
        dataTypes: {
          $addToSet: { $type: '$value' }
        },
        count: { $sum: 1 },
        cur: { $last: '$value' },
        ts: { $last: '$ts' }
      }
    }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);



# ------------- From Assets -------------------- #
[
  {
    $match: {
      identifier: "SIM00000107"
    }
  },
  {
    $lookup: {
      from: "telemetry",
      localField: "identifier",
      foreignField: "metadata.ident",
      as: "results",
      pipeline: [
        {
          $match: {
            timestamp: {
              $gt: ISODate(
                "2024-06-12T20:23:24.437Z"
              )
            }
          }
        },
        {
          $addFields: {
            "measures.ts": "$timestamp"
          }
        },
        {
          $project: {
            fieldNames: {
              $objectToArray: "$measures"
            },
            ts: "$timestamp"
          }
        },
        {
          $unwind: {
            path: "$fieldNames"
          }
        },
        {
          $project: {
            key: "$fieldNames.k",
            value: "$fieldNames.v",
            ts: "$ts"
          }
        },
        {
          $match: {
            value: {
              $ne: null
            }
          }
        },
        {
          $group: {
            _id: "$key",
            ex: {
              $addToSet: "$value"
            },
            dataTypes: {
              $addToSet: {
                $type: "$value"
              }
            },
            count: {
              $sum: 1
            },
            cur: {
              $last: "$value"
            },
            ts: {
              $last: "$ts"
            }
          }
        },
        {
          $group: {
            _id: "",
            updated_at: {
              $last: "$ts"
            },
            measures: {
              $addToSet: {
                k: "$_id",
                v: {
                  value: "$cur",
                  ts: "$ts"
                }
              }
            }
          }
        },
        {
          $project: {
            updated_at: 1,
            measures: {
              $arrayToObject: "$measures"
            }
          }
        }
      ]
    }
  },
  {
    $unwind:
      {
        path: "$results"
      }
  },
  {
    $addFields:
      {
        merged: {
          $mergeObjects: [
            "$measures",
            "$results.measures"
          ]
        }
      }
  }
]


# --------------- Grouping with multiple devices
db.flespi.aggregate([
  {
    $match: {$and: [{timestamp: {$gt: ISODate("2024-06-13T20:23:24.437+00:00")}},
        {timestamp: {$lt: ISODate("2024-06-14T08:23:24.437+00:00")}}]}
  },
  
  {
    $project: {
      fieldNames: { $objectToArray: "$measures" },
      ident: "$metadata.ident",
      ts: "$timestamp"
    }
  },
  
  {
    $unwind: "$fieldNames"
  },
  {
    $match: { "fieldNames.v": { $ne: null}}
  },
  {
    $group: {
      _id: {ident: "$ident", key: "$fieldNames.k"},
      value: {$last: "$fieldNames.v"},
      ts: {$last: "$ts"}
    }
  },
  {
      $group: {
        _id: "$_id.ident",
        updated_at: {
          $last: "$ts"
        },
        measures: {
          $addToSet: {
            k: "$_id.key",
            v: {
              value: "$value",
              ts: "$ts"
            }
          }
        }
      }
    },
    {
      $project: {
        updated_at: 1,
        measures: {
          $arrayToObject: "$measures"
        }
      }
    }
])

# ----------- Update with Condition -------------------- #
cur_update = datetime.datetime.fromISODate()
db.asset.updateOne({identifier: cur_id}, {$set:{
  updated_at: {$cond: {
    if: {$gt: ["$updated_at", ]}
  }}
} } )