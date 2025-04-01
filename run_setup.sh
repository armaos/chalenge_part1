#!/bin/bash

# Run the SQL script (i use my own user: alexandros)
psql -U $USER  -d template1 -f setup_imdb_db_for_conda.sql
