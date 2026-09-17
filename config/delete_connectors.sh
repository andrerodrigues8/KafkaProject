#!/bin/bash

curl -X DELETE -H "Accept:application/json" -H  "Content-Type:application/json" http://connect:8083/connectors/jdbc-source-suppliers-example
curl -X DELETE -H "Accept:application/json" -H  "Content-Type:application/json" http://connect:8083/connectors/jdbc-postgresql-sink
