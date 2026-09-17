#!/bin/bash

curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://connect:8083/connectors -d @sink.json
curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://connect:8083/connectors -d @source.json
