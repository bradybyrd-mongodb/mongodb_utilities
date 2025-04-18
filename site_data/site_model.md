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
    db.asset.find({"location_id" : "L-1000006"})
find everything on a floor in a building:
    db.asset.find({"location_id" : "L-1000106", "parents" : {$elemMatch: {rtype: "floor", value: 4}}})
new assets:
    db.asset.find({in_service_date: {$gte: now() - 24.hrs}})
move assset to another building:
    db.asset.updateMany({"location_id" : location_id},{"$set": {"location_id" : newlocation_id, "location" : rec["address"]["location"]}}))
unassigned assets:
    db.asset.find({"location_id" : {$exists: false}})
location of an asset:
    db.asset.aggregate([
        {$match: {asset_id: "A-1000576"}},
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
        near: { type: 'Point', coordinates: [ -76.72803, 39.00622 ] },
        distanceField: 'distance',
        maxDistance: 160000
    }},
    {$project: {
        asset_id: 1, name: 1, distance: 1, _id: 0
    }}
]

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