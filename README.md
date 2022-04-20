# uk_rail_lake_formation
Lake formation project demonstrating how to save a UK Rail data stream into governed tables

This repo contains various approaches for processing streaming data that originates
from the [Open UK Rail data feeds](https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/)

All data consumed is from the Train Movements API

This repo contains:
- A script to connect to the Open Rail data queue and save the resulting data to S3. This script is run and restarted as needed by system.d (./connect_to_queue)
- Notebooks to explore and process the data (./notebook)
- A Lambda to process incoming files and save them to a Lake Formation governed table (./process_landed_file)
- A failed attempt at using Glue to process Bronze to Silver (./glue_etl)
- A batch job to process bronze to silver (./batch_processing)
