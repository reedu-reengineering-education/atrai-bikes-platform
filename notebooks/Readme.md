# Data Overview

## 1. Speed
- **Normalized speed** (normalized with the 99.9th percentile to handle a few outliers)
  - Speed in the pop-ups is calculated back to km/h
- **Underlying Road Network** from Münster without primary, secondary, or tertiary roads (mainly used by cars) to assign data points with nearest-neighbour search  

## 2. Traffic Flow
- **Weighted traffic flow index** with twice the weight on `Standing` parameter
  - `Standing`: Probability of the bike standing still (0-1)  
  - `Normalized Speed`: Normalized with the 99.9th percentile  
- **Visualization excludes start and stop points** of each ride to avoid skewed results  
- **Same road network** as Speed  

## 3. Distances Flowmap
- Distance at (possible) overtaking situations  
  - Filtered out all data points where `Overtaking Manoeuvre < 0.05`  
  - Normalized overtaking distance with `200 cm` (legal minimum overtaking distance) to achieve better contrast in the map  
- **Same road network** as Speed & Traffic Flow  

## 4. Temperature
- **Temperature data** from all sensors (divided into seasons, but only Autumn had significant data point entries)  
- **Excluded first and last 60 seconds** of each ride to minimize errors from indoor temperature/sensor cool-off period  

## 5. PM2.5
- `PM2.5` as the PM fraction with best agreement with governmental measurements in Münster  
- **Filtered out data** where relative humidity > 75% for best accuracy in optical sensors  
- **Outlier detection** using `1.5 * IQR` for each individual sensor  

## 6. Danger Zones
- **Heatmap** of weighted combination of `Overtaking Manoeuvre` (weight = 0.3) and `Overtaking Distance` (weight = 0.7)  
- **Filtered out data** where relative humidity > 75% (for best accuracy in optical sensors)  
- **Weighted risk index** similar to danger zones, including:  
  - `Overtaking Manoeuvre` (0.15)  
  - `Normalized Overtaking Distance` (0.35)  
  - `Normalized PM1` (0.2)  
  - `Normalized PM2.5` (0.15)  
  - `Normalized PM4` (0.1)  
  - `Normalized PM10` (0.05)  

## 7. Road Roughness
- **AI model** calculating road roughness from vibration sensor  
  - Based on percentages of 4 categories (in ascending roughness): `Asphalt`, `Paving`, `Compacted`, `Sett`  
- **Normalized & weighted Road Roughness Index** based on AI model categories  
