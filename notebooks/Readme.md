1.	Speed:
  •	Normalized speed (normalized with the 99.9th percentile to handle a few outliers)
    o	Speed in the pop-ups is calculated back to km/h
  •	Underlying Road Network from Münster without primary, secondary or tertiary roads (mainly used by cars) to assign data points with nearest-neighbour search

2.	Traffic Flow:
  •	Weighted traffic flow index based on to put twice the weight on Standing parameter
    o	Standing: Probability of the bike standing still (0-1) 
    o	Normalized Speed: Normalized with the 99.9th percentile
  •	Visualization without start and stop points of each ride to avoid skewed results
  •	Same road network as 1.

3.	Distances Flowmap:
  •	Distance at (possible) overtaking situations 
    o	Filtered out all data points with ‘Overtaking Manoeuvre’ (probability of an overtaking happening) < 0.05
    o	Normalized overtaking distance with 200 cm (legal minimum overtaking distance) to achieve better contrast in map (all distances above 200 cm are viewed as not dangerous in the map)
  •	Same road network as 1.+2.

4.	Temperature:
  •	Temperature data from all sensors (divided into seasons, but only Autumn had significant data point entries) 
  •	Without the first and last 60 seconds of each ride to minimize error from indoor temperature/sensors cool-off period

5.	PM2.5:
  •	PM2.5 as PM fraction with best agreement with governmental measurements in Münster
  •	Data with rel. humidity above 75% filtered out to achieve the best possible results from optical sensors
  •	Outlier detection with 1.5 times IQR for each individual sensor

6.	dangerzones
  •	heatmap of weighted combination of ‘Overtaking Manoeuvre’ (weight = 0.3) and ‘Overtaking Distance’ (weight = 0.7) in risk index 
  •	Data with rel. humidity above 75% filtered out to achieve the best possible results from optical sensors (PM)
  •	Weighted risk index (including PM) similar to dangerzones with ‘Overtaking Manoeuvre’ (weight = 0.15) , ‘Normalized Overtaking Distance’ (weight = 0.35), ‘Normalized PM1’ (weight = 0.2), ‘Normalized PM2.5’ (weight = 0.15), ‘Normalized PM4’ (weight = 0.1), and ‘Normalized PM10’ (weight = 0.05)

7.	Road roughness
  •	AI model calculating road roughness from vibration sensor based on percentages of 4 categories in ascending order in terms of roughness: Asphalt, Paving, Compacted and Sett
  •	Normalized and weighted Road Roughness Index based on categories from AI model
