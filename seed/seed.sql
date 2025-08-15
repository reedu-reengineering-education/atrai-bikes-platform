CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE TABLE IF NOT EXISTS road_roughness (id SERIAL PRIMARY KEY, geom GEOMETRY(Point, 4326), roughness FLOAT);
CREATE TABLE IF NOT EXISTS osem_bike_data (id SERIAL PRIMARY KEY, geometry GEOMETRY(Point, 4326));
CREATE TABLE IF NOT EXISTS distances_flowmap (
    id SERIAL PRIMARY KEY,
    "Average Overtaking Distance" FLOAT,
    "Average Overtaking Manoeuvre" FLOAT,
    "Average Normalized Overtaking Distance" FLOAT,
    "Average Distance to Road" FLOAT,
    "Number of Points" INTEGER,
    geometry GEOMETRY(LineString, 4326)
);
CREATE TABLE IF NOT EXISTS bike_road_network (osmid TEXT PRIMARY KEY, geometry GEOMETRY(LineString, 4326), name TEXT);
CREATE TABLE IF NOT EXISTS statistics (
    tag TEXT PRIMARY KEY,
    geometry GEOMETRY(Polygon, 4326),
    statistics TEXT,
    "updatedAt" TIMESTAMP
);