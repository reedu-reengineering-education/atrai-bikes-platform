# Münster Bike Data Maps

data (via openSenseMap API): https://github.com/jonathan2001/OpenSenseMapToolbox.git

All maps were created with bounding boxes to exclude data from outside Münster and from inside the reedu office building. Only devices (device IDs) with at least 10 measurements are considered in the analysis.

## 1. Speed Map
- **Normalized Speed**: Normalized using the 99.9th percentile to handle outliers.
- **Pop-ups**: Speed is converted back to km/h from the normalized speed.
- **Road Network**: Based on the Münster OSM road network, excluding primary, secondary, and tertiary roads (mainly used by cars), with data points assigned via nearest-neighbor search.
- **Additional Info**: Number of data points per road segment displayed in the pop-ups.

## 2. Traffic Flow Map
- **Traffic Flow Index**: $\( \text{Normalized Speed} \times (1 - \text{Standing}^2) \)$, where the "Standing" parameter has twice the weight.
- **Standing**: Probability of the bike standing still (0-1 range).
- **Normalized Speed**: Normalized using the 99.9th percentile.
- **Road Network**: Same as the Speed Map.
- **Data Filtering**: 
  - Excludes start and stop points where consecutive "Standing" values > 0.9 and time differences < 10 minutes (for consecutive points in individual rides) to avoid skewed results.

## 3. Distances Flow Map
- **Overtaking Distance**: Only data points with "Overtaking Maneuver" > 0.05 are included.
- **Normalization**: Overtaking distances are normalized to a 200 cm legal minimum threshold (values above 200 cm are capped at 200 cm).
- **Road Network**: Same as the Speed and Traffic Flow Maps.

## 4. (Seasonal) Temperature Heatmap
- **Temperature Data**: Divided by meteorological seasons (Spring data not available yet).
- **Data Filtering**: 
  - Excludes the first and last 60 seconds of each ride (time differences between consecutive points < 10 min) to reduce indoor sensor bias.
  
## 5. PM2.5 Heatmap
- **PM2.5 Fraction**: Selected due to its best agreement with governmental measurements in Münster.
- **Data Filtering**: 
  - Relative humidity > 75% is excluded to improve optical sensor accuracy.
  - Outliers are removed using 1.5 times the interquartile range (IQR) for each sensor.

## 6. PM2.5 Timeframe Heatmap
- **Correction Methods**: Same as the PM2.5 Heatmap (humidity and IQR filtering).
- **Customizable Filters**: Time range (hours of the day) and meteorological season.
  - Both filters can be turned off individually in the `filter_season_and_time` function (set to False).

## 7. Danger Zones
- **Risk Index (RI)**: Weighted combination of "Overtaking Maneuver" and "Overtaking Distance":

  $\[
  RI = 0.3 \times \text{Overtaking Maneuver} + 0.7 \times (1 - \text{Normalized Distance})
  \]$

- **Distance Normalization**: Maximum measurable distance is 400 cm.
- **Data Filtering**: Only includes data points where "Overtaking Maneuver" > 0.05.

## 8. Danger Zones with PM
- **Correction Methods**: Same as the PM2.5 Heatmap and Timeframe Heatmap.
- **Weighted Risk Index (RI)**: Includes PM data and overtaking parameters:
  
  $\[
  RI = 0.15 \times \text{Overtaking Maneuver} + 0.35 \times (1 - \text{Normalized Distance}) + 0.2 \times \text{PM1} + 0.15 \times \text{PM2.5} + 0.1 \times \text{PM4} + 0.05 \times \text{PM10}
  \]$
  
- **Weighting**: Smaller PM fractions are weighted higher due to their greater potential harm.

## 9. Road Roughness Colored Map
- **AI Model**: Calculates road roughness from vibration sensor data.
- **Categories**: Percentages of 4 roughness types in ascending order:
  - Asphalt (1)
  - Paving (2)
  - Compacted (3)
  - Sett (4)

- **Road Roughness Score (RRS)**:
  
  $\[
  RRS = 1 \times \text{Asphalt} + 2 \times \text{Paving} + 3 \times \text{Compacted} + 4 \times \text{Sett}
  \]$
  
- **Normalization**: RRS is normalized to a Road Roughness Index (0-100 range).

## 10. PM Boxplots
- **Data Filtering**: Relative humidity > 75% is excluded (no IQR filtering here).
- **Visualization**: Boxplots display PM measurements for each device (PM1, PM2.5, PM4, PM10).
- **Y-axis Limit**: Restricted to 0-80 for better visibility (`plt.ylim`).

## 11. PM Monthly Average
- **PM2.5 Averages**: Calculated monthly with the same humidity correction as the boxplots.
- **Data Type**: Stored as `float64` (not rounded).
- **Missing Data**: No qualifying data for August 2024.

## 12. PM Diurnal Cycle
- **PM2.5 Averages**: Calculated across the day with the same humidity correction as above.
- **Time Rounding**: Measurement timestamps are rounded to the nearest half-hour.
- **Averages**: Calculated for each half-hour, stored as `float64` (not rounded).
