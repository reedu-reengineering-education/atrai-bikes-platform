CREATE TABLE IF NOT EXISTS road_roughness (id SERIAL PRIMARY KEY, geometry GEOMETRY(Point, 4326));
--CREATE TABLE IF NOT EXISTS osem_bike_data (id SERIAL PRIMARY KEY, geometry GEOMETRY(Point, 4326));
CREATE TABLE IF NOT EXISTS distances_flowmap (
    id SERIAL PRIMARY KEY,
    "Average Overtaking Distance" FLOAT,
    "Average Overtaking Manoeuvre" FLOAT,
    "Average Normalized Overtaking Distance" FLOAT,
    "Average Distance to Road" FLOAT,
    "Number of Points" INTEGER,
    geometry GEOMETRY(LineString, 4326)
);
