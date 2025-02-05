legend_html = '''
<div style="position: fixed; bottom: 30px; left: 30px; width: 300px; height: 160px;
            background-color: white; z-index:9999; font-size:14px;
            border:2px solid grey; padding: 10px; text-align: center;">
    <b>Road Roughness Index (RRI)</b><br>

    <!-- Small color boxes above each range -->
    <div style="display: flex; justify-content: space-between; margin: 5px auto; width: 250px;">
        <div style="width: 20px; height: 20px; background: green;"></div>
        <div style="width: 20px; height: 20px; background: lightgreen;"></div>
        <div style="width: 20px; height: 20px; background: yellow;"></div>
        <div style="width: 20px; height: 20px; background: orange;"></div>
        <div style="width: 20px; height: 20px; background: red;"></div>
    </div>

    <!-- Labels for ranges -->
    <div style="display: flex; justify-content: space-between; margin: 5px auto; width: 250px;">
        <span>0-20</span>
        <span>21-40</span>
        <span>41-60</span>
        <span>61-80</span>
        <span>81-100</span>
    </div>

    <p style="margin-top: 10px; font-size: 12px; text-align: center;">
        RRI based on surface textures<br>
        calculated by senseBox:bike
    </p>
</div>
'''