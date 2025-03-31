#!/bin/bash

# Run the SQL script
psql -U $USER  -d template1 -f ./initdb/setup_imdb_db.sql
