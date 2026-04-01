from __future__ import annotations
import sys
from typing import Optional
import pandas as pd
import csv
from pathlib import Path
from io import BytesIO
import pyarrow as pa
import pyarrow.csv as pv

# Geo stack
import geopandas as gpd
from shapely import wkt as shapely_wkt
from shapely import wkt
from shapely.geometry import mapping
from shapely.validation import make_valid
from shapely.errors import TopologicalError

# BigQuery
#from google.cloud import bigquery, storage

# Increase the max field size to the maximum allowed
csv.field_size_limit(sys.maxsize)


def shapefile_to_csv_geojson(
    input_path: str,
    output_path: str,
    output_format: str = "csv",
    source_crs: Optional[str] = None,
    target_crs: int = 4326,
    wkt_col: str = "geometry_wkt",
    include_index: bool = False,
    fix_invalid: bool = True,
    include_bbox: bool = False,
) -> None:
    """
    Read a shapefile/GeoJSON/any vector supported by GeoPandas, reproject to target_crs,
    serialize geometry as WKT into a single column, and write CSV (attributes + WKT).

    Parameters
    ----------
    input_path : path to .shp, .geojson, .gpkg, etc.
    output_csv : path to write CSV
    source_crs : set if the input has no CRS metadata (e.g., old shapefile missing .prj)
    target_crs : 'EPSG:4326' for BigQuery GEOGRAPHY compatibility
    wkt_col : name of WKT column
    include_index : include CSV row index
    fix_invalid : attempt to repair invalid geometries with make_valid()
    include_bbox : also include bbox columns: minx, miny, maxx, maxy
    """
    
    print("Loading shapefile...")
    try:
        gdf = gpd.read_file(input_path)
    except (IOError, ConnectionError, FileNotFoundError) as e:
        raise RuntimeError(f"Error reading shapefile: {e}")
  
    # Check CRS
    print("Checking coordinate reference system (CRS)...")
    # If CRS is missing and you know it, set it
    if gdf.crs is None and source_crs:
        gdf.set_crs(source_crs, inplace=True)
    elif gdf.crs is None and source_crs is None:
        print("No CRS defined in source file. You **must** set one manually.")
        raise ValueError("Source Shapefile has no CRS defined.")

    print(f"Source CRS detected/set: {gdf.crs}")

    # Reproject to target CRS for consistent lon/lat order and BigQuery GEOGRAPHY
    if gdf.crs.to_epsg() != target_crs:
        print(f"Reprojecting to EPSG:{target_crs} (WGS84)...")
        gdf = gdf.to_crs(epsg=target_crs)
    else:
        print("Already in WGS84 (EPSG:4326).")

    # Validate geometries
    print("Checking for invalid geometries...")
    invalid_count = (~gdf.is_valid).sum()

    if invalid_count > 0 and fix_invalid:
        print(f"Found {invalid_count} invalid geometries. Attempting to fix...")
        try:
            gdf["geometry"] = gdf["geometry"].apply(lambda geom: make_valid(geom) if geom is not None else None)
            # check if still invalid geometries remain after make_valid
            if (~gdf.is_valid).sum() > 0:
                # buffer(0) is a common fix for invalid polygons
                gdf["geometry"] = gdf["geometry"].buffer(0)
        except TopologicalError:
            print("Some geometries could not be fixed.")
            raise
        print("Invalid geometries fixed.")
    else:
        print("All geometries are valid.")

    # Convert to WKT CSV or GeoJson and write output
    if output_format.lower() == "csv":
        _vector_to_wkt_csv(gdf, output_path, wkt_col=wkt_col, include_index=include_index, include_bbox=include_bbox)
    elif output_format.lower() == "geojson":
        _vector_to_geojson(gdf, output_path)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")
    

def _vector_to_wkt_csv(
    gdf: gpd.GeoDataFrame,
    output_path: str,
    wkt_col: str = "geometry_wkt",
    include_index: bool = False,
    include_bbox: bool = False,
) -> None:
    """
    Convert a vector file to a CSV with WKT geometries.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame containing the vector data.
    output_path : str
        Path to the output CSV file.
    wkt_col : str, optional
        Name of the WKT column, by default "geometry_wkt"
    include_index : bool, optional
        Whether to include the index in the CSV, by default False
    include_bbox : bool, optional
        Whether to include bounding box columns, by default False
    """
   
    # Serialize to WKT
    gdf[wkt_col] = gdf.geometry.to_wkt()

    # Optionally include bounding boxes for quick spatial filtering in tabular systems
    if include_bbox:
        b = gdf.geometry.bounds  # DataFrame with minx, miny, maxx, maxy
        gdf = pd.concat([gdf.drop(columns=["geometry"]), b], axis=1)
    else:
        gdf = gdf.drop(columns=["geometry"])

    gdf.to_csv(output_path, index=include_index,  float_format="%.8f", quoting=csv.QUOTE_ALL)  # quoting=1 for quoting all fields, adjust as needed
    print(f"Wrote CSV with WKT to: {output_path}\n")


def _vector_to_geojson(
    gdf: gpd.GeoDataFrame,
    output_path: str,
    ) -> None:

    """
    Method to convert a Shapefile to GeoJSON.
    Handles CRS, invalid geometries, and export reliability.
    """
    print(f"Writing GeoJSON to: {output_path}")

    try:
        gdf.to_file(output_path, driver="GeoJSON")
    except (IOError, ConnectionError) as e:
        raise RuntimeError(f"Error writing GeoJSON: {e}")

    print("Conversion complete!\n\n")
    

# def load_csv_to_bigquery(
#     project_id: str,
#     dataset_id: str,
#     table_id: str,
#     csv_path: str,
#     write_disposition: str = "WRITE_TRUNCATE",
#     autodetect: bool = True,
#     location: Optional[str] = None,
# ) -> None:
#     """
#     Load CSV file to BigQuery (WKT will be STRING).
#     Later, convert WKT -> GEOGRAPHY via SQL.
#     """
#     client = bigquery.Client(project=project_id, location=location)
#     table_ref = f"{project_id}.{dataset_id}.{table_id}"

#     job_config = bigquery.LoadJobConfig(
#         autodetect=autodetect,
#         source_format=bigquery.SourceFormat.CSV,
#         skip_leading_rows=1,
#         write_disposition=write_disposition,
#         field_delimiter=",",
#         quote_character='"',
#         allow_quoted_newlines=True,
#         encoding="UTF-8",
#     )

#     with open(csv_path, "rb") as f:
#         load_job = client.load_table_from_file(f, table_ref, 
#                                                job_config=job_config)
#     load_job.result()

#     table = client.get_table(table_ref)
#     print(f"Loaded {table.num_rows} rows into {table_ref}")


# def create_table_with_geography_from_wkt(
#     project_id: str,
#     dataset_id: str,
#     src_table: str,
#     dest_table: str,
#     wkt_col: str = "geometry_wkt",
#     geog_col: str = "geom",
#     drop_wkt: bool = False,
#     location: Optional[str] = None,
# ) -> None:
#     """
#     Create a new table with a native GEOGRAPHY column from a WKT STRING column.
#     By default keeps the WKT (set drop_wkt=True to remove).
#     """
#     client = bigquery.Client(project=project_id, location=location)
 
#     if drop_wkt:
#         sql = f"""
#         CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{dest_table}` AS
#         SELECT
#           * EXCEPT({wkt_col}),
#           ST_GeogFromText({wkt_col}) AS {geog_col}
#         FROM `{project_id}.{dataset_id}.{src_table}`;
#         """
#     else:
#         sql = f"""
#         CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{dest_table}` AS
#         SELECT
#           t.*,
#           ST_GeogFromText(t.{wkt_col}) AS {geog_col}
#         FROM `{project_id}.{dataset_id}.{src_table}` AS t;
#         """

#     job = client.query(sql)
#     job.result()
#     print(f"Created table `{project_id}.{dataset_id}.{dest_table}` with GEOGRAPHY column `{geog_col}`")



# def shapefile_csv_to_bigquery(
#     shapefile_path: str,
#     tmp_csv_path: str,
#     project_id: str,
#     dataset_id: str,
#     raw_table: str,
#     geog_table: str,
#     wkt_col: str = "geometry_wkt",
#     geog_col: str = "geom",
# ) -> None:
#     """
#       load CSV to BQ raw table
#       create GEOGRAPHY table
#     """

#     load_csv_to_bigquery(project_id, dataset_id, raw_table, tmp_csv_path)
#     create_table_with_geography_from_wkt(project_id, dataset_id, raw_table, 
#                                          geog_table, wkt_col=wkt_col, geog_col=geog_col
#                                          )


def split_csv_by_parts(
    input_csv: str,
    output_dir: str,
    parts: int,
    quoting: int = csv.QUOTE_ALL,
    escapechar: str = "\\",
    lineterminator: str = "\n",
) -> None:
    """
    Split a CSV into `parts` roughly-equal files while preserving row integrity.
    Works safely with quoted WKT columns.

    Example: parts=4 → file_part_1.csv ... file_part_4.csv
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Count rows (excluding header)
    with open(input_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        total_rows = sum(1 for _ in reader)

    rows_per_part = (total_rows // parts) + (1 if total_rows % parts else 0)

    with open(input_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)

        file_index = 1
        row_count = 0

        out_path = Path(output_dir, f"split_part_{file_index}.csv")
        out_file = open(out_path, "w", encoding="utf-8", newline="")
        writer = csv.writer(out_file, quoting=quoting, escapechar=escapechar, lineterminator=lineterminator)

        writer.writerow(header)

        for row in reader:
            writer.writerow(row)
            row_count += 1

            if row_count >= rows_per_part:
                out_file.close()
                file_index += 1
                row_count = 0

                if file_index <= parts:
                    out_path = Path(output_dir, f"split_part_{file_index}.csv")
                    out_file = open(out_path, "w", encoding="utf-8", newline="")
                    writer = csv.writer(out_file, quoting=quoting, escapechar=escapechar, lineterminator=lineterminator)
                    writer.writerow(header)

        out_file.close()

    print(f"Split into {parts} parts in: {output_dir}")


# def load_and_merge_csv_from_bucket(
#     bucket_name: str,
#     prefix: str,
#     project: str,
#     bq_dataset: str,
#     bq_table: str,
#     pattern: str = None,
#     write_disposition: str = "WRITE_TRUNCATE"
# ):
#     """
#     Robust method to:
#     1. Load multiple split CSV files from GCS
#     2. Merge them safely
#     3. Write to BigQuery reliably

#     Args:
#         bucket_name: GCS bucket name
#         prefix: Folder/prefix where part files live (e.g., "splits/")
#         project: GCP project id
#         bq_dataset: BigQuery dataset
#         bq_table: BigQuery table
#         write_disposition: WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY
#     """
#     # Connect to GCS and list objects
#     storage_client = storage.Client(project=project)
#     bucket = storage_client.bucket(bucket_name)

#     print("Listing files in bucket...")
#     blobs = list(bucket.list_blobs(prefix=prefix))

#     if pattern:
#         part_files = [b for b in blobs if b.name.endswith(".csv") and pattern in b.name]

#     if not part_files:
#         raise RuntimeError("No CSV files found in bucket for prefix: " + prefix)

#     print(f"Found {len(part_files)} CSV split parts.")

#     # Load CSV split parts safely (using PyArrow for speed)
#     tables = []

#     for b in sorted(part_files, key=lambda x: x.name):  # ensures correct ordering
#         print(f"Loading: {b.name}")

#         data = b.download_as_bytes()  # robust download
#         buffer = BytesIO(data)

#         # Use PyArrow's CSV reader for large-scale performance
#         table = pv.read_csv(
#             buffer,
#             read_options=pv.ReadOptions(autogenerate_column_names=False),
#             parse_options=pv.ParseOptions(delimiter=","),
#         )

#         tables.append(table)

#     # Merge tables efficiently
#     print("Merging split parts into one Arrow table...")
#     merged_table = pa.concat_tables(tables, promote=True)

#     print("Merge complete. Rows:", merged_table.num_rows)

#     # Optional: convert to Pandas (BigQuery accepts Arrow or Pandas)
#     df = merged_table.to_pandas()

#     ##############################################################
#     # Load into BigQuery
   
#     print("Writing merged dataset to BigQuery...")

#     client = bigquery.Client(project=project)
#     table_id = f"{project}.{bq_dataset}.{bq_table}"

#     job_config = bigquery.LoadJobConfig(
#         write_disposition=write_disposition,
#         autodetect=True,           # BigQuery infers schema reliably from Arrow
#         source_format=bigquery.SourceFormat.PARQUET,
#     )

#     # Use Arrow → Parquet in-memory strategy (most reliable)
#     parquet_bytes = pa.BufferOutputStream()
#     pa.parquet.write_table(merged_table, parquet_bytes)
#     parquet_bytes = parquet_bytes.getvalue()

#     load_job = client.load_table_from_file(
#         BytesIO(parquet_bytes),
#         table_id,
#         job_config=job_config,
#     )

#     load_job.result()  # Wait for completion

#     print(f"Loaded {merged_table.num_rows} rows into {table_id}")


# # Example usage:
# # load_and_merge_csv_from_bucket(
# #     bucket_name="my-bucket",
# #     prefix="split_csv/",
# #     project="my-gcp-project",
# #     bq_dataset="analytics",
# #     bq_table="final_merged_table"
# # )