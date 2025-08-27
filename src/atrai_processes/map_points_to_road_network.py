import geopandas as gpd

def map_points_to_road_segments(
    point_gdf: gpd.GeoDataFrame,
    road_segments: gpd.GeoDataFrame,
    numeric_columns: list,
    id_column: str = "id",
    distance_col: str = "distance_to_road"
) -> gpd.GeoDataFrame:
    """
    Maps point-based data (e.g., roughness, overtaking) to road segments
    and returns a GeoDataFrame of aggregated results per segment.

    Args:
        point_gdf (GeoDataFrame): Point data with geometry and numeric columns.
        road_segments (GeoDataFrame): Line geometries of road segments.
        numeric_columns (list): List of numeric column names to aggregate.
        id_column (str): Column to count (default is 'id').
        distance_col (str): Name of the distance column to compute.

    Returns:
        GeoDataFrame: Aggregated data per road segment.
    """
    # Ensure CRS match
    road_segments = road_segments.set_crs(4326)
    point_gdf = point_gdf.set_crs(4326)
    point_gdf = point_gdf.to_crs(road_segments.crs)
    road_segments = road_segments.reset_index(drop=True)

    # Spatial join to find nearest segment
    joined = gpd.sjoin_nearest(
        point_gdf,
        road_segments,
        how="left",
        distance_col=distance_col,
    )

    # Columns to aggregate
    agg_dict = {col: "mean" for col in numeric_columns}
    agg_dict[distance_col] = "mean"
    agg_dict[id_column] = "count"

    # Group by road segment index
    aggregated = joined.groupby("index_right").agg(agg_dict)

    # Rename columns
    new_columns = {
        col: f"Average {col}" for col in numeric_columns
    }
    new_columns[distance_col] = "Average Distance to Road"
    new_columns[id_column] = f"Number of Points"
    aggregated = aggregated.rename(columns=new_columns)

    # Join geometry back in
    result = aggregated.merge(
        road_segments, left_on="index_right", right_index=True
    )
    result["id"] = result.index

    return gpd.GeoDataFrame(result, geometry="geometry", crs=road_segments.crs)