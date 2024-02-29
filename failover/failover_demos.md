# --------------------------------------------------------------------- #
#   BJB =  MultiRegion Demo
# mongosh "mongodb+srv://multiregion.vmwqj.mongodb.net/" --apiVersion 1 --username <username>

- Goal 
    Workload running in two regions
        Data Loading from compute in each region - tag source
        {source: USWEST,
        ts: timestamp,
        }
- Commands:
  West Load:
    python3 microservice.py location=west action=run_load
  East Load:
    python3 microservice.py location=east action=run_load