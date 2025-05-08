import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import LineString
import logging
from sklearn.cluster import DBSCAN
from shapely.geometry import Point

LOGGER = logging.getLogger(__name__)

def filter_points(points, eps=10):
    """
    Filter points using DBSCAN clustering to remove clustered points.
    Args:
        points (list): List of shapely Point objects.
        eps (float): The maximum distance between two samples for one to be considered as in the neighborhood of the other.
    Returns:
        list: List of filtered points.
    """
    coords = np.array([(point.y, point.x) for point in points])
    clustering = DBSCAN(eps=eps / 111139, min_samples=1).fit(coords)  # Convert meters to degrees
    unique_labels = set(clustering.labels_)

    filtered_points = []
    for label in unique_labels:
        cluster_points = coords[clustering.labels_ == label]
        centroid = cluster_points.mean(axis=0)
        filtered_points.append(centroid)

    return [Point(y, x) for y, x in filtered_points]


def process_tours(data, interval=15):
    """
    Process tours from the given data.
    Args:
        data (pd.DataFrame): DataFrame containing the data to process.
        interval (int): Time interval in minutes to determine if a new tour starts.
    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing the processed tours.
    """
    boxids = data["id"].unique()
    all_tours = []

    for boxid in boxids:
        subset = data[data["id"] == boxid]
        subset["createdAt"] = pd.to_datetime(subset["createdAt"])
        subset = subset.sort_values(by="createdAt")
        subset["tdiff"] = subset["createdAt"].diff()
        subset["tour"] = (subset["tdiff"] > pd.Timedelta(minutes=interval)).cumsum()
        subset.drop(columns=["tdiff"], inplace=True)

        tours = subset["tour"].unique()
        for tour in tours:
            tour_data = subset[subset["tour"] == tour]
            ps = pd.Series([x for x in tour_data["geometry"]])
            valid_points = [p for p in ps if p is not None]
            valid_points = filter_points(valid_points)  # Apply filtering to remove clustered points
            if len(valid_points) > 1:
                line = LineString(valid_points)
                start_time = tour_data["createdAt"].iloc[0]
                end_time = tour_data["createdAt"].iloc[-1]
                duration = (end_time - start_time).total_seconds()
                distance = line.length * 111139  # Convert degrees to meters (approximation for WGS 84).

                # Apply filters: at least 120 seconds, 500 meters, and 10 points
                if duration >= 120 and distance >= 500 and len(valid_points) >= 10:
                    all_tours.append(
                        {
                            "boxid": boxid,
                            "tour": f"tour_{tour}",
                            "geometry": line,
                            "start_time": start_time,
                            "end_time": end_time,
                            "duration": duration,
                            "distance": distance,
                            "kcal": calc_calories(
                                duration_s=duration,
                            ),
                        }
                    )

    gdf = gpd.GeoDataFrame(all_tours, geometry="geometry").set_crs("EPSG:4326")

    return gdf


def calc_calories(duration_s):
    """
    Super simple calculation of calories burned based on MET value.
    Args:
        duration_s (int): Duration in seconds.
    Returns:
        float: Estimated calories burned.
    """
    average_weight_kg = 70
    met = 6.5  # reduced intensity
    duration_min = duration_s / 60
    calories_burned = (met * average_weight_kg * 3.5 / 200) * duration_min
    return calories_burned


def tour_stats(tours_gdf):
    latest_stats = {
        "trip_count": len(tours_gdf),
        "total_duration_s": tours_gdf["duration"].sum(),  # unit: seconds
        "average_duration_s": tours_gdf["duration"].mean(),
        "max_duration_s": tours_gdf["duration"].max(),
        "min_duration_s": tours_gdf["duration"].min(),
        "total_distance_m": tours_gdf["distance"].sum(),
        "average_distance_m": tours_gdf["distance"].mean(),
        "max_distance_m": tours_gdf["distance"].max(),
        "min_distance_m": tours_gdf["distance"].min(),
        "average_speed_kmh": (tours_gdf["distance"] / tours_gdf["duration"]).mean() * 3.6,  # unit: km/h
        "total_kcal": tours_gdf["kcal"].sum(),
    }

    latest_stats = {key: (value.isoformat() if isinstance(value, pd.Timestamp) else value) for key, value in latest_stats.items()}
    weekly_stats = calculate_weekly_stats(tours_gdf)
    weekly_stats["week"] = weekly_stats["week"].dt.to_pydatetime().astype(str)

    return {"latest_stats": latest_stats, "weekly_stats": weekly_stats.to_dict(orient="records")}


def calculate_weekly_stats(tours_gdf):
    tours_gdf["week"] = tours_gdf["start_time"].dt.to_period("W").apply(lambda r: r.start_time)
    weekly_stats = tours_gdf.groupby("week").agg(
        trip_count=("duration", "count"),
        total_duration_s=("duration", "sum"),
        average_duration_s=("duration", "mean"),
        max_duration_s=("duration", "max"),
        min_duration_s=("duration", "min"),
        total_distance_m=("distance", "sum"),
        average_distance_m=("distance", "mean"),
        max_distance_m=("distance", "max"),
        min_distance_m=("distance", "min"),
        average_speed_kmh=("distance", lambda x: (x / tours_gdf.loc[x.index, "duration"]).mean() * 3.6),
        total_kcal=("kcal", "sum"),
    ).reset_index()

    return weekly_stats