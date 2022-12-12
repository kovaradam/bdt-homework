from pyspark.sql.types import *

pid_schema = StructType([
    StructField('geometry', StructType([
        StructField('coordinates', ArrayType(StringType()), True),
        StructField('type', StringType())])),
    StructField('properties', StructType([
        StructField('last_position', StructType([
            StructField('bearing', IntegerType()),
            StructField('delay', StructType([
                StructField("actual", IntegerType()),
                StructField("last_stop_arrival", StringType()),
                StructField("last_stop_departure", StringType())])),
            StructField("is_canceled", BooleanType()),
            StructField('last_stop', StructType([
                StructField("arrival_time", StringType()),
                StructField("departure_time", StringType()),
                StructField("id", StringType()),
                StructField("sequence", IntegerType())])),
            StructField('next_stop', StructType([
                StructField("arrival_time", StringType()),
                StructField("departure_time", StringType()),
                StructField("id", StringType()),
                StructField("sequence", IntegerType())])),
            StructField("origin_timestamp", StringType()),
            StructField("shape_dist_traveled", StringType()),
            StructField("speed", StringType()),
            StructField("state_position", StringType()),
            StructField("tracking", BooleanType())])),
        StructField('trip', StructType([
            StructField('agency_name', StructType([
                StructField("real", StringType()),
                StructField("scheduled", StringType())])),
            StructField('cis', StructType([
                StructField("line_id", StringType()),
                StructField("trip_number", StringType())])),
            StructField('gtfs', StructType([
                StructField("route_id", StringType()),
                StructField("route_short_name", StringType()),
                StructField("route_type", IntegerType()),
                StructField("trip_headsign", StringType()),
                StructField("trip_id", StringType()),
                StructField("trip_short_name", StringType())])),
            StructField("origin_route_name", StringType()),
            StructField("sequence_id", IntegerType()),
            StructField("start_timestamp", StringType()),
            StructField("vehicle_registration_number", IntegerType()),
            StructField('vehicle_type', StructType([
                StructField("description_cs", StringType()),
                StructField("description_en", StringType()),
                StructField("id", IntegerType())])),
            StructField("wheelchair_accessible", BooleanType()),
            StructField("air_conditioned", BooleanType())]))])),
    StructField("type", StringType())
])
