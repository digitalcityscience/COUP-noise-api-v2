import json
import os

import geopandas as gpd
import numpy
from geomet import wkt

from noise_api.noise_analysis.road_info import RoadInfo

cwd = os.path.dirname(os.path.abspath(__file__))

# road_type_ids from IffStar NoiseModdeling
road_types_iffstar_noise_modelling = {
    "boulevard": 56,  # in boulevard 70km/h
    "street": 53,  # extra boulevard Street 50km/h
    "alley": 54,  # extra boulevard Street <50km/,
    "railroad": 99,  # railroad
}

all_roads = []


def reset_all_roads():
    global all_roads
    all_roads = []


# opens a json from path
def open_geojson(path):
    with open(path) as f:
        return json.load(f)


# extract traffic data from road properties
def get_car_traffic_data(road_properties):
    #  https://d-nb.info/97917323X/34
    # p. 68, assuming a mix of type a) and type b) for car traffic around grasbrook (strong morning peak)
    # most roads in hamburg belong to type a) or b)
    # assuming percentage of daily traffic in peak hour = 11%
    # truck traffic: the percentage of daily traffic in peak hour seems to be around 8%
    # source. https://bast.opus.hbz-nrw.de/opus45-bast/frontdoor/deliver/index/docId/1921/file/SVZHeft29.pdf

    # Sources daily traffic: HafenCity GmbH , Standortanalyse, S. 120

    if road_properties["road_type"] == "railroad":
        return None, None, None

    car_traffic = int(int(road_properties["car_traffic_daily"]) * 0.11)
    truck_traffic = int(int(road_properties["truck_traffic_daily"]) * 0.08)
    max_speed = int(road_properties["max_speed"])

    return max_speed, car_traffic, truck_traffic


# source for train track data = http://laermkartierung1.eisenbahn-bundesamt.de/mb3/app.php/application/eba
def get_train_track_data(road_properties):
    if not road_properties["road_type"] == "railroad":
        return None, None, None, None

    train_speed = road_properties["train_speed"]
    train_per_hour = road_properties["trains_per_hour"]
    ground_type = road_properties["ground_type"]
    has_anti_vibration = road_properties["has_anti_vibration"]

    return train_speed, train_per_hour, ground_type, has_anti_vibration


def apply_traffic_settings_to_roads(roads, traffic_settings):
    max_speed = traffic_settings["max_speed"]
    traffic_quota = traffic_settings["traffic_quota"]

    for road in roads["features"]:
        # only adjust traffic settings of manipulatable roads
        if (
            "traffic_settings_adjustable" in list(road["properties"].keys())
            and road["properties"]["traffic_settings_adjustable"]
        ):
            if max_speed is not None:
                road["properties"]["max_speed"] = max_speed

            if traffic_quota is not None:
                road["properties"]["truck_traffic_daily"] = road["properties"][
                    "truck_traffic_daily"
                ] * (traffic_quota / 100)
                road["properties"]["car_traffic_daily"] = road["properties"][
                    "car_traffic_daily"
                ] * (traffic_quota / 100)

    return roads


def get_road_queries(roads_gdf, traffic_settings):
    roads_geojson = json.loads(roads_gdf.to_json())
    roads_geojson = apply_traffic_settings_to_roads(roads_geojson, traffic_settings)
    road_features = roads_geojson["features"]

    for feature in road_features:
        id = feature["properties"]["id"]
        road_type = get_road_type(feature["properties"])
        coordinates = feature["geometry"]["coordinates"]

        # input road type might not be defined. road is not imported # TODO consider using a fallback
        if road_type == 0:
            continue
        if feature["geometry"]["type"] == "MultiLineString":
            # beginning point of the road
            start_point = coordinates[0][0]
            # end point of the road
            end_point = coordinates[-1][-1]
        else:
            # beginning point of the road
            start_point = coordinates[0]
            # end point of the road
            end_point = coordinates[1]
            # build string containing all coordinates

        geom = wkt.dumps(feature["geometry"], decimals=0)

        max_speed, car_traffic, truck_traffic = get_car_traffic_data(
            feature["properties"]
        )
        (
            train_speed,
            train_per_hour,
            ground_type,
            has_anti_vibration,
        ) = get_train_track_data(feature["properties"])

        # init new RoadInfo object
        road_info = RoadInfo(
            id,
            geom,
            road_type,
            start_point,
            end_point,
            max_speed,
            car_traffic,
            truck_traffic,
            train_speed,
            train_per_hour,
            ground_type,
            has_anti_vibration,
        )
        all_roads.append(road_info)

    nodes = create_nodes(all_roads)
    sql_insert_strings = []
    for road in all_roads:
        sql_insert_string = get_insert_query_for_road(road, nodes)
        sql_insert_strings.append(sql_insert_string)

    return sql_insert_strings


# returns sql queries for the traffic table,
def get_traffic_queries():
    sql_insert_strings_noisy_roads = []
    nodes = create_nodes(all_roads)
    for road in all_roads:
        node_from = get_node_for_point(road.get_start_point(), nodes)
        node_to = get_node_for_point(road.get_end_point(), nodes)

        if (
            road.get_road_type_for_query()
            == road_types_iffstar_noise_modelling["railroad"]
        ):
            # train traffic
            train_speed = road.get_train_speed()
            trains_per_hour = road.get_train_per_hour()
            ground_type = road.get_ground_type_train_track()
            has_anti_vibration = road.is_anti_vibration()

            sql_insert_string = (
                "INSERT INTO roads_traffic (node_from,node_to, train_speed, "
                "trains_per_hour, ground_type, has_anti_vibration) "
                "VALUES ({0},{1},{2},{3},{4},{5});".format(
                    node_from,
                    node_to,
                    train_speed,
                    trains_per_hour,
                    ground_type,
                    has_anti_vibration,
                )
            )
        else:
            # car traffic
            traffic_cars = road.get_car_traffic()
            traffic_trucks = road.get_truck_traffic()
            max_speed = road.get_max_speed()
            load_speed = max_speed * 0.9
            junction_speed = max_speed * 0.85

            sql_insert_string = (
                "INSERT INTO roads_traffic (node_from,node_to,load_speed,junction_speed,max_speed,"
                "lightVehicleCount,heavyVehicleCount) "
                "VALUES ({0},{1},{2},{3},{4},{5},{6});".format(
                    node_from,
                    node_to,
                    load_speed,
                    junction_speed,
                    max_speed,
                    traffic_cars,
                    traffic_trucks,
                )
            )
        sql_insert_strings_noisy_roads.append(sql_insert_string)

    return sql_insert_strings_noisy_roads


# returns a wkt string for a multipolygon containing all buildings
def get_buildings_geom_as_wkt(buildings_gdf: gpd.GeoDataFrame) -> str:
    # simplify complex geometries to speed up calculation and avoid hickups with spatial db.
    buildings_gdf.geometry = buildings_gdf.geometry.simplify(0.1)

    # calculation of noise results is 5sec. faster if building geometries are provided as single multipolygon
    return f"'{buildings_gdf.geometry.unary_union}'"


# create nodes for all roads - nodes are connection points of roads
def create_nodes(all_roads):
    nodes = []
    for road in all_roads:
        coordinates_start_point = road.get_start_point()
        nodes.append(coordinates_start_point)
        coordinates_end_point = road.get_end_point()
        nodes.append(coordinates_end_point)
    unique_nodes = []

    # filter for duplicates
    for node in nodes:
        if not any(
            numpy.array_equal(node, unique_node) for unique_node in unique_nodes
        ):
            unique_nodes.append(node)
    return unique_nodes


def get_node_for_point(point, nodes):
    dict_of_nodes = {i: nodes[i] for i in range(0, len(nodes))}
    for node_id, node in dict_of_nodes.items():
        if point == node:
            return node_id
    print("could not find node for point", point)
    exit()


def get_road_type(road_properties):
    # if not in road types continue
    for output_road_type in road_types_iffstar_noise_modelling.keys():
        if road_properties["road_type"] == output_road_type:
            return road_types_iffstar_noise_modelling[output_road_type]

    print("no matching noise road_type_found for", road_properties["road_type"])
    return 0


def get_insert_query_for_road(road, nodes):
    node_from = get_node_for_point(road.get_start_point(), nodes)
    node_to = get_node_for_point(road.get_end_point(), nodes)

    sql_insert_string = (
        "INSERT INTO roads_geom (the_geom,NUM,node_from,node_to,road_type) "
        "VALUES (ST_GeomFromText('{0}'),{1},{2},{3},{4});".format(
            road.get_geom(),
            road.get_road_id(),
            node_from,
            node_to,
            road.get_road_type_for_query(),
        )
    )

    return sql_insert_string
