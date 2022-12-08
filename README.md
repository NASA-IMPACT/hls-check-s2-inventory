# hls-check-s2-inventory
This repository checks the HLS S2 archive with the catalogue CSVs produced by Copernicus (more information [here](https://scihub.copernicus.eu/userguide/CatalogueViewInCSV)). Requirements for this workflow include:

1. Start Date
2. End Date
3. Archive S3 bucket in AWS

Note that that the copernicus inventory CSVs currently do not require authentication with Scihub. The reconciliation only MGRS Tile Ids included in the [HLS Tile grid](https://raw.githubusercontent.com/NASA-IMPACT/hls-land_tiles/master/HLS.land.tiles.txt). Copernicus CSVs are separated out into daily files by platform and are updated monthly on the dates provided by Copernicus under "Update Frequency" at the link above.

The [reconciliation script](https://github.com/NASA-IMPACT/hls-check-s2-inventory/blob/main/check-s2-archive.py) first checks for the existence of the S2 file in the inventory. If the file is found, it checks the file sizes match and ensures the file has not been updated based on the last modified date on the S3 object in the HLS S2 archive. Results are output to a json file with filename formatted as `missing_scenes_<start date>_<end date>.json`. The json is organized by Sensing Date and each date contains a list of objects missing from the archive. Dates with empty lists are synced between the two databases.
