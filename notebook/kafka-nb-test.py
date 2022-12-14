# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # 4 - Differences by direction of travel - to and from Prague
# MAGIC From the data stream, implement a stream processing application that will calculate 
# MAGIC differences in delay for suburban lines - arrival and departure delays.
# MAGIC 
# MAGIC Input: stream
# MAGIC Output: Dashboard map with arrivals to Prague and marking the differences in delays during 
# MAGIC the day

# COMMAND ----------

# MAGIC %run "./create_stream"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Get data
# MAGIC 
# MAGIC - We collect data from the stream each hour
# MAGIC - There is also memory stream for debugging etc.

# COMMAND ----------

# Start stream write into memory

buses_stream_mem_append = buses_select.writeStream \
        .format("memory")\
        .queryName("mem_buses")\
        .outputMode("append")\
        .start()
        

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Get data from memory for debugging purposes
# MAGIC 
# MAGIC drop table if exists buses;
# MAGIC 
# MAGIC create table buses select * from mem_buses

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Get persisted data collected over period of time
# MAGIC 
# MAGIC drop table if exists buses;
# MAGIC 
# MAGIC create table buses select * from buses_hourly;

# COMMAND ----------

# MAGIC  %sql select count(*) from buses

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data selection
# MAGIC 
# MAGIC - Pick only bus lines from/to Prague
# MAGIC - Then only the latest entry per each hour is selected

# COMMAND ----------

# MAGIC 
# MAGIC %sql 
# MAGIC 
# MAGIC -- Select distinct line number and destination combinations
# MAGIC drop table if exists bus_line_destinations;
# MAGIC create table bus_line_destinations from buses select distinct properties.trip.gtfs.trip_headsign as destination, properties.trip.gtfs.route_short_name as line_number;
# MAGIC 
# MAGIC -- Filter only lines who end up in prague from one of the directions
# MAGIC drop table if exists lines_with_prague_destination;
# MAGIC create table lines_with_prague_destination from bus_line_destinations select * where destination like '%Praha%';
# MAGIC 
# MAGIC -- Create buses table only from prague lines
# MAGIC drop table if exists prague_bus_lines;
# MAGIC create table prague_bus_lines from buses select * where properties.trip.gtfs.route_short_name in (select line_number from lines_with_prague_destination)

# COMMAND ----------

# MAGIC %sql select count(*) from prague_bus_lines

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists bus_data;
# MAGIC 
# MAGIC create table bus_data SELECT 
# MAGIC   cast(geometry.coordinates[0] as double) AS x,
# MAGIC   cast(geometry.coordinates[1] as double) AS y,
# MAGIC   properties.trip.origin_route_name as line_number,
# MAGIC   properties.trip.gtfs.trip_headsign as destination,
# MAGIC   properties.last_position.delay.actual as delay,
# MAGIC --   using last departure as a timestamp
# MAGIC   cast(properties.last_position.origin_timestamp as timestamp) as timestamp,  
# MAGIC   properties
# MAGIC FROM prague_bus_lines ;
# MAGIC 
# MAGIC drop table if exists latest_bus_data;
# MAGIC 
# MAGIC -- Get latest data
# MAGIC create table latest_bus_data select *, substring(timestamp, 0, 13) as timestamp_hour from bus_data order by timestamp desc;
# MAGIC 
# MAGIC -- Get only the latest bus entry per hour 
# MAGIC drop table if exists hourly_bus_data;
# MAGIC create table hourly_bus_data select * from (
# MAGIC    select *, row_number() over (partition by timestamp_hour, line_number  order by timestamp) as row_number
# MAGIC    from latest_bus_data
# MAGIC    ) 
# MAGIC where row_number = 1 order by timestamp desc; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hourly_bus_data order by timestamp_hour desc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Plots
# MAGIC 
# MAGIC - Map containing latest bus positions with their direction and delay (red ones are delayed, green ones are on time)
# MAGIC - Bar plots with differences between delays from/to Prague

# COMMAND ----------

# MAGIC %pip install osmnx
# MAGIC %pip install numpy==1.23.0

# COMMAND ----------

import osmnx 

custom_filter='["highway"~"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|road|road_link"]'

G = osmnx.graph_from_place("Prague, Czechia", custom_filter=custom_filter)
# G = osmnx.graph_from_bbox(12.09,51.06,18.87,48.55,custom_filter=custom_filter)


# COMMAND ----------

def rows_by_interval(dataframe, time_from, time_to, selector='timestamp'):
    return dataframe[(dataframe[selector] >= time_from) & (dataframe[selector] < time_to)]
 
def df_len(dataframe):
    return dataframe.shape[0]
    

# COMMAND ----------

hourly_prague_buses = _sqldf.toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Map of Prague with latest bus positions in given hour

# COMMAND ----------

from datetime import datetime, timedelta

import matplotlib.pyplot as plt
from matplotlib import widgets

# The latest collected entry
now = datetime(2022, 12, 14, 9, 15)

red, green, gray = '#b90000', '#056805', '#777'

buses = hourly_prague_buses

last_n_hours = 3
for i in range(last_n_hours):    
    fig, ax = osmnx.plot_graph(G, show=False, close=False,figsize=(10,10), bgcolor='white',edge_color=gray,node_color=gray)
    time_to = now - timedelta(hours=i)
    time_from = now - timedelta(hours=i+1)
    
    buses_in_timeframe = rows_by_interval(buses, time_from, time_to).drop_duplicates(subset=['destination','line_number'],keep='last')
    
    delayed_buses, buses_on_time = buses_in_timeframe[buses_in_timeframe['delay']>0], buses_in_timeframe[buses_in_timeframe['delay']<=0]
    
    ax.scatter('x', 'y', data=delayed_buses, c=red)
    ax.scatter('x', 'y', data=buses_on_time, c=green)

    ax.set_title(f'Latest bus delay between {time_from.hour}:00 and {time_to.hour}:00')
    
    buses_in_timeframe.reset_index()

    for _, row in buses_in_timeframe.iterrows():    
        delay, x, y = row['delay'], row['x'], row['y']
        label = f"{row['line_number']}: {row['destination']} \n delay: {delay}s"
        color = green if delay <= 0 else red
        ax.annotate(label, (x, y), c=color)
    
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Bar plots showing differences between delays of buses heading from/to Prague

# COMMAND ----------

buses = hourly_prague_buses

buses_in = buses[buses['destination'].str.contains('Praha')]
buses_out = buses[~buses['destination'].str.contains('Praha')]

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np


hour_range = 24

hour_intervals = list(map(lambda i: (now - timedelta(hours=i+1), now - timedelta(hours=i)), reversed(range(hour_range))))
in_buses_intervals = list(map(lambda interval: rows_by_interval(buses_in, interval[0], interval[1]), hour_intervals))
out_buses_intervals = list(map(lambda interval: rows_by_interval(buses_out, interval[0], interval[1]), hour_intervals))

labels = list(map(lambda interval: f"from: {interval[0].hour} to: {interval[1].hour}",hour_intervals))
in_medians = list(map(lambda buses: buses['delay'].median(), in_buses_intervals))
out_medians = list(map(lambda buses: buses['delay'].median(), out_buses_intervals))

in_sizes = list(map(lambda buses: len(buses['delay'].index), in_buses_intervals))
out_sizes = list(map(lambda buses: len(buses['delay'].index), out_buses_intervals))


x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, in_medians, width, label='To Prague')
rects2 = ax.bar(x + width/2, out_medians, width, label='From prague')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Departure delay median [s]')
ax.set_title(f'Departure delay median from {(now - timedelta(hours=hour_range)).hour} to {now.hour}')
ax.set_xticks(x, labels)
ax.figure.set_size_inches(25, 10)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)

fig.tight_layout()

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, in_sizes, width, label='To Prague')
rects2 = ax.bar(x + width/2, out_sizes, width, label='From prague')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Dataset size')
ax.set_title(f'Dataset sizes from {(now - timedelta(hours=hour_range)).hour} to {now.hour}')
ax.set_xticks(x, labels)

ax.figure.set_size_inches(31.3, 10)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)


plt.show()

# COMMAND ----------


