import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import LineString
import logging

logger = logging.getLogger(__name__)


def process_tours(data, intervall=15):
    boxids = data["id"].unique()
    all_tours = []

    for boxid in boxids:
        subset = data[data["id"] == boxid]
        subset["createdAt"] = pd.to_datetime(subset["createdAt"])
        subset = subset.sort_values(by="createdAt")
        subset["tdiff"] = subset["createdAt"].diff()
        subset["tour"] = (subset["tdiff"] > pd.Timedelta(minutes=intervall)).cumsum()
        subset.drop(columns=["tdiff"], inplace=True)

        tours = subset["tour"].unique()
        for tour in tours:
            tour_data = subset[subset["tour"] == tour]
            ps = pd.Series([x for x in tour_data["geometry"]])
            valid_points = [p for p in ps if p is not None]
            if len(valid_points) > 1:
                line = LineString(valid_points)
                start_time = tour_data["createdAt"].iloc[0]
                end_time = tour_data["createdAt"].iloc[-1]
                duration = (end_time - start_time).total_seconds()
                distance = line.length * 111139  # Convert degrees to meters (approximation for WGS 84).

                # Apply filters: at least 60 seconds, 500 meters, and 10 points
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
                                avg_speed=(distance / duration) * 3.6,
                                time=duration,
                            ),
                        }
                    )

    gdf = gpd.GeoDataFrame(all_tours, geometry="geometry").set_crs("EPSG:4326")
    logger.info(gdf.head())

    return gdf


def calc_calories(avg_speed, time):
    kmh = avg_speed * 3.6
    hours = time / 3600
    kcal = (300 + 30 * (kmh - 15)) * hours
    kcal = np.where(kcal > 0, kcal, 0)
    return kcal.tolist() if isinstance(kcal, np.ndarray) else kcal


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
        "average_distance_per_trip_m": tours_gdf["distance"].mean(),  # unit: m
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