#  Full Stack Demo with CDC, Confluent and MongoDB
# - comm suppression use case

Components:
    Postgres - acts as legacy relational system (claim changes are triggered here)
    Confluent postgres connector - does CDC to kafka
    Confluent mongodb connector - kafka conn
    Atlas - 
    Node/Python scripts:
        send updates to claims in postgres(GCP)
        monitor comm queues (collections - comms_sent, comms_suppressed) prints what got sent/supressed and why
        comm_suppression:
            receive claim change
            lookup member preferences
                check if comm is allowed - some ruleset
                if allowed - create doc in comms_sent
                if not - create in comms_suppressed (add reason why)
    