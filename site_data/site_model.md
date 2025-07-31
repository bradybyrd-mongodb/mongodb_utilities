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

# 7/29/25

Null timestamp
1970-01-01T00:00:00.000+00:00
1989-10-28T12:20:34.000+00:00
MaxTimestamp: 2024-05-19T00:12:32.164+00:00

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
       "my_timeseries_collection",
       {
          timeseries: {
             timeField: "timestamp",
             metaField: "sensorId", // Optional, if you have a field identifying unique series
             granularity: "hours" // Optional, e.g., "seconds", "minutes", "hours"
          }
       }
    )


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
