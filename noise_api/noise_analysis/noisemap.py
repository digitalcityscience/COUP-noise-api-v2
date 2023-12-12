import json
import logging
import os
import shlex
import subprocess
from pathlib import Path

import psycopg2
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from noise_api.noise_analysis import queries
from noise_api.noise_analysis.geo_helpers import (
    all_z_values_to_zero,
    geojson_to_gdf_with_metric_crs,
)
from noise_api.noise_analysis.sql_query_builder import (
    get_buildings_geom_as_wkt,
    get_road_queries,
    get_traffic_queries,
    reset_all_roads,
)

logger = logging.getLogger(__name__)

DB_NAME = (os.path.abspath(".") + os.sep + "mydb").replace(os.sep, "/")


class H2DatabaseContextManager:
    def __enter__(self):
        self.h2_subprocess, self.psycopg2 = self.boot_h2_database_in_subprocess()
        self.conn, self.psycopg2_cursor = self.initiate_database_connection(
            self.psycopg2
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(ImportError),
    )
    def boot_h2_database_in_subprocess(self):
        args = shlex.split(
            'java -cp "bin/*:bundle/*:sys-bundle/*" org.h2.tools.Server -pg -trace'
        )
        f = open("log.txt", "w+")
        p = subprocess.Popen(args, cwd=ORBISGIS_DIR, stdout=f)
        print("ProcessID H2-database ", p.pid)

        try:
            import psycopg2

            return p, psycopg2
        except ImportError as e:
            stdout, stderr = p.communicate()
            logger.warn("Could not connect to database.")
            logger.warn(stdout, stderr)
            p.terminate()
            raise e

    @retry(
        stop=stop_after_attempt(6),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(psycopg2.OperationalError),
    )
    def initiate_database_connection(self, psycopg2):
        # DB name has to be an absolute path
        conn_string = (
            f"host='localhost' port=5435 dbname='{DB_NAME}' user='sa' password='sa'"
        )
        print("Connecting to database\n ->%s" % (conn_string))

        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print("Connected!\n")

        cursor.execute(queries.H2GIS_SPATIAL)

        # Initialize NoiseModelling functions
        for alias_name, function_name in queries.FUNCTIONS_TO_INIT:
            cursor.execute(
                queries.CREATE_ALIAS.substitute(
                    alias=alias_name,
                    func=function_name,
                )
            )

        return conn, cursor

    def cleanup(self):
        # Close connections to the database
        print("Closing cursor")
        self.psycopg2_cursor.close()

        print("Closing database connection")
        self.conn.close()

        # Terminate the database process as it constantly blocks memory
        self.h2_subprocess.terminate()


def get_geojson_path(filename:str):
    cwd = os.path.dirname(os.path.abspath(__file__))
    results_folder = os.path.abspath(f"{cwd}/results/")

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)

    return os.path.abspath(f"{results_folder}/{filename}.geojson")


ORBISGIS_DIR = Path(__file__).parent / "orbisgis_java"


def export_result_from_db_to_geojson(cursor):
    geojson_path = get_geojson_path("result")
    cursor.execute(f"CALL GeoJsonWrite('{geojson_path}', 'CONTOURING_NOISE_MAP');")

    with open(geojson_path) as f:
        return json.load(f)


def table_to_geojson(cursor, table_name):
    geojson_path = get_geojson_path(table_name)
    cursor.execute(f"CALL GeoJsonWrite('{geojson_path}', '{table_name.upper()}');")

    with open(geojson_path) as f:
        return json.load(f)


def get_settings():
    return {
        'settings_name': 'max triangle area',
        'max_prop_distance': 750,  # the lower the less accurate
        'max_wall_seeking_distance': 50,  # the lower  the less accurate
        'road_with': 1.5,  # the higher the less accurate
        'receiver_densification': 2.8,  # the higher the less accurate
        'max_triangle_area': 275,  # the higher the less accurate
        'sound_reflection_order': 0,  # the higher the less accurate
        'sound_diffraction_order': 0,  # the higher the less accurate
        'wall_absorption': 0.23,  # the higher the less accurate
    }


def calculate_noise_result(
        cursor, connection, traffic_settings, buildings_geojson, roads_geojson
) -> dict:
    # reproject input geojsons to local metric crs
    # TODO: all coordinates for roads and buildings are currently set to z level 0
    # TODO when upgrading to new noise version, that has proper 3D implementation- we should change this.
    buildings_gdf = all_z_values_to_zero(
        geojson_to_gdf_with_metric_crs(buildings_geojson)
    )
    roads_gdf = all_z_values_to_zero(geojson_to_gdf_with_metric_crs(roads_geojson))

    print("make buildings table ..")

    cursor.execute(queries.RESET_BUILDINGS_TABLE)
    cursor.execute(
        queries.INSERT_BUILDING.substitute(
            building=get_buildings_geom_as_wkt(buildings_gdf)
        )
    )

    table_to_geojson(cursor, "buildings")

    print("Make roads table (just geometries and road type)..")
    reset_all_roads()
    cursor.execute(queries.RESET_ROADS_GEOM_TABLE)
    roads_queries = get_road_queries(traffic_settings, roads_gdf)
    for road in roads_queries:
        cursor.execute("""{0}""".format(road))

    print("Making traffic information table ...")
    cursor.execute(queries.RESET_ROADS_TRAFFIC_TABLE)
    traffic_queries = get_traffic_queries()
    for traffic_query in traffic_queries:
        cursor.execute("""{0}""".format(traffic_query))

    print("Duplicating geometries to give sound level for each traffic direction ...")
    cursor.execute(queries.RESET_ROADS_DIR_TABLES)

    print("Computing the sound level for each segment of roads ...")

    # compute the power of the noise source and add it to the table roads_src_global
    # for railroads (road_type = 99) use the function BTW_EvalSource (TW = Tramway)
    # for car roads use the function BR_EvalSource
    cursor.execute(queries.RESET_ROADS_GLOBAL_TABLE)

    table_to_geojson(cursor, "roads_src_global")

    print("Applying frequency repartition of road noise level ...")
    cursor.execute(queries.RESET_ROADS_SRC_TABLE)
    table_to_geojson(cursor, "roads_src")


    print("Please wait, sound propagation from sources through buildings ...")
    
    cursor.execute("""drop table if exists tri_lvl; create table tri_lvl as SELECT * from BR_TriGrid((select 
    st_expand(st_envelope(st_accum(the_geom)), 750, 750) the_geom from ROADS_SRC),'buildings','roads_src','DB_M','',
    {max_prop_distance},{max_wall_seeking_distance},{road_with},{receiver_densification},{max_triangle_area},
    {sound_reflection_order},{sound_diffraction_order},{wall_absorption}); """.format(**get_settings()))
    
    #cursor.execute(queries.RESET_TRI_LVL_TABLE)
    table_to_geojson(cursor, "tri_lvl")

    print("Computation done !")

    print("Creating isocountour and save it as a geojson in the working folder..")
    cursor.execute(queries.RESET_TRICONTOURING_MAP)
    table_to_geojson(cursor, "tricontouring_noise_map")
    table_to_geojson(cursor, "contouring_noise_map")

    return export_result_from_db_to_geojson(cursor)


def run_noise_calculation(task_def: dict):
    with H2DatabaseContextManager() as h2_context:
        noise_result_geojson = calculate_noise_result(
            h2_context.psycopg2_cursor,
            h2_context.conn,
            {
                "max_speed": task_def["max_speed"],
                "traffic_quota": task_def["traffic_quota"],
            },
            task_def["buildings"],
            task_def["roads"],
        )

    # Try to make noise computation even faster
    # by adjustiong: https://github.com/Ifsttar/NoiseModelling/blob/master/noisemap-core/
    # src/main/java/org/orbisgis/noisemap/core/jdbc/JdbcNoiseMap.java#L30
    # by shifting to GB center
    #   https: // github.com / Ifsttar / NoiseModelling / blob / master / noisemap - core / src / main / java / org /
    #   orbisgis / noisemap / core / jdbc / JdbcNoiseMap.java  # L68

    return {"geojson": noise_result_geojson}


def test():
    task = {
        "max_speed": 42,
        "traffic_quota": 40,
        "buildings": {
            "type": "FeatureCollection",
            "features": [
                {
                    "id": "73",
                    "type": "Feature",
                    "properties": {
                        "building_height": 40.7
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    10.014099378866533,
                                    53.524805539336455
                                ],
                                [
                                    10.013584681218306,
                                    53.52424591306608
                                ],
                                [
                                    10.013181258332839,
                                    53.52437766902722
                                ],
                                [
                                    10.0136959520914,
                                    53.52493728805215
                                ],
                                [
                                    10.014099378866533,
                                    53.524805539336455
                                ]
                            ]
                        ]
                    }
                },
                {
                    "id": "72",
                    "type": "Feature",
                    "properties": {
                        "building_height": 40.7
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    10.01486057803836,
                                    53.52563313819909
                                ],
                                [
                                    10.014244356000154,
                                    53.524963172991846
                                ],
                                [
                                    10.013840928404411,
                                    53.52509493118538
                                ],
                                [
                                    10.014457130960604,
                                    53.52576489860589
                                ],
                                [
                                    10.01486057803836,
                                    53.52563313819909
                                ]
                            ]
                        ]
                    }
                },
                {
                    "id": "71",
                    "type": "Feature",
                    "properties": {
                        "building_height": 40.7
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    10.015701527165058,
                                    53.5265474282797
                                ],
                                [
                                    10.015012794045756,
                                    53.52579864680569
                                ],
                                [
                                    10.01477470401294,
                                    53.52587640050063
                                ],
                                [
                                    10.015463434230112,
                                    53.526625183350184
                                ],
                                [
                                    10.015701527165058,
                                    53.5265474282797
                                ]
                            ]
                        ]
                    }
                },
                {
                    "id": "70",
                    "type": "Feature",
                    "properties": {
                        "building_height": 40.7
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    10.015597750718719,
                                    53.526827320769115
                                ],
                                [
                                    10.015738566728485,
                                    53.52698040821248
                                ],
                                [
                                    10.015733840319239,
                                    53.526782873634794
                                ],
                                [
                                    10.015597750718719,
                                    53.526827320769115
                                ]
                            ]
                        ]
                    }
                },
                {
                    "id": "67",
                    "type": "Feature",
                    "properties": {
                        "building_height": 19.7
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    10.011569702204943,
                                    53.52570189930657
                                ],
                                [
                                    10.01034465640258,
                                    53.52436980700336
                                ],
                                [
                                    10.009722963465965,
                                    53.524572817898274
                                ],
                                [
                                    10.010947995785207,
                                    53.52590491658985
                                ],
                                [
                                    10.011569702204943,
                                    53.52570189930657
                                ]
                            ]
                        ]
                    }
                },
                {
                    "id": "66",
                    "type": "Feature",
                    "properties": {
                        "building_height": 23.9
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    10.012860074476707,
                                    53.527104915532334
                                ],
                                [
                                    10.011627699435754,
                                    53.52576495214621
                                ],
                                [
                                    10.011005992377848,
                                    53.525967969731944
                                ],
                                [
                                    10.012238353855702,
                                    53.52730793954479
                                ],
                                [
                                    10.012860074476707,
                                    53.527104915532334
                                ]
                            ]
                        ]
                    }
                },
                {
                    "id": "65",
                    "type": "Feature",
                    "properties": {
                        "building_height": 32.3
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    10.012925323902595,
                                    53.527175852620374
                                ],
                                [
                                    10.012303602563586,
                                    53.5273788769731
                                ],
                                [
                                    10.012512460138776,
                                    53.527605961750126
                                ],
                                [
                                    10.013314676427372,
                                    53.52759916568125
                                ],
                                [
                                    10.012925323902595,
                                    53.527175852620374
                                ]
                            ]
                        ]
                    }
                },
                {
                    "id": "63",
                    "type": "Feature",
                    "properties": {
                        "building_height": 28.1
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    10.011323691798829,
                                    53.52730092855511
                                ],
                                [
                                    10.011085586737638,
                                    53.52737868394193
                                ],
                                [
                                    10.011003428014629,
                                    53.527618731102315
                                ],
                                [
                                    10.011611248473693,
                                    53.52761359004825
                                ],
                                [
                                    10.011323691798829,
                                    53.52730092855511
                                ]
                            ]
                        ]
                    }
                }
            ]
        },
        "roads": {
            "type": "FeatureCollection",
            "features": [
                {
                    "id": "6",
                    "type": "Feature",
                    "properties": {
                        "car_traffic_daily": 9125,
                        "id": 4,
                        "max_speed": 60,
                        "road_type": "boulevard",
                        "traffic_settings_adjustable": False,
                        "truck_traffic_daily": 2040
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [
                                10.015719700069049,
                                53.526191884898054
                            ],
                            [
                                10.0150686298969,
                                53.52545322870338
                            ],
                            [
                                10.01459813428357,
                                53.52496265159586
                            ],
                            [
                                10.014335422596513,
                                53.524703265293425
                            ],
                            [
                                10.01420143520423,
                                53.52446336304828
                            ],
                            [
                                10.013385892565978,
                                53.52377582169494
                            ],
                            [
                                10.012856668000119,
                                53.52350987230347
                            ],
                            [
                                10.012249284490858,
                                53.52328867481946
                            ],
                            [
                                10.012220421472291,
                                53.52328065184323
                            ]
                        ]
                    }
                },
                {
                    "id": "2",
                    "type": "Feature",
                    "properties": {
                        "ground_type": 1,
                        "has_anti_vibration": False,
                        "id": 103,
                        "road_type": "railroad",
                        "traffic_settings_adjustable": False,
                        "train_speed": 80,
                        "trains_per_hour": 26
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [
                                10.015696044136266,
                                53.52520314323848
                            ],
                            [
                                10.01544991236562,
                                53.52492642764882
                            ],
                            [
                                10.0141673465077,
                                53.5230974136704
                            ]
                        ]
                    }
                },
                {
                    "id": "1",
                    "type": "Feature",
                    "properties": {
                        "ground_type": 1,
                        "has_anti_vibration": False,
                        "id": 102,
                        "road_type": "railroad",
                        "traffic_settings_adjustable": False,
                        "train_speed": 80,
                        "trains_per_hour": 26
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [
                                10.015698267177568,
                                53.525296061696565
                            ],
                            [
                                10.015393879597392,
                                53.52495385173759
                            ],
                            [
                                10.014092530969918,
                                53.52309804827115
                            ]
                        ]
                    }
                },
                {
                    "id": "0",
                    "type": "Feature",
                    "properties": {
                        "ground_type": 1,
                        "has_anti_vibration": False,
                        "id": 101,
                        "road_type": "railroad",
                        "traffic_settings_adjustable": False,
                        "train_speed": 80,
                        "trains_per_hour": 26
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [
                                10.015699997385378,
                                53.52536838039002
                            ],
                            [
                                10.01535445425897,
                                53.52497990122113
                            ],
                            [
                                10.01403518208507,
                                53.52309853468448
                            ]
                        ]
                    }
                },
                {
                    "id": "5",
                    "type": "Feature",
                    "properties": {
                        "car_traffic_daily": 2615,
                        "id": 3,
                        "max_speed": 60,
                        "road_type": "boulevard",
                        "traffic_settings_adjustable": False,
                        "truck_traffic_daily": 790
                    },
                    "geometry": {
                        "type": "MultiLineString",
                        "coordinates": [
                            [
                                [
                                    10.012219483626744,
                                    53.52328010862719
                                ],
                                [
                                    10.011608824647345,
                                    53.52311908899523
                                ]
                            ],
                            [
                                [
                                    10.009861092347728,
                                    53.523133863976575
                                ],
                                [
                                    10.009828931100333,
                                    53.52313718148473
                                ],
                                [
                                    10.00822168735532,
                                    53.523380026633966
                                ],
                                [
                                    10.008165495117522,
                                    53.523402546708176
                                ]
                            ]
                        ]
                    }
                },
                {
                    "id": "15",
                    "type": "Feature",
                    "properties": {
                        "car_traffic_daily": 1665,
                        "id": 1009,
                        "max_speed": 50,
                        "road_type": "street",
                        "traffic_settings_adjustable": False,
                        "truck_traffic_daily": 765
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [
                                10.01010117316096,
                                53.525703887558784
                            ],
                            [
                                10.010006188990866,
                                53.52551592010156
                            ],
                            [
                                10.00981686451524,
                                53.52516694664098
                            ],
                            [
                                10.009626260820001,
                                53.52476404942073
                            ],
                            [
                                10.009389133137025,
                                53.5243076104114
                            ],
                            [
                                10.008190979746313,
                                53.52340083739593
                            ]
                        ]
                    }
                },
                {
                    "id": "4",
                    "type": "Feature",
                    "properties": {
                        "car_traffic_daily": 2615,
                        "id": 2,
                        "max_speed": 60,
                        "road_type": "boulevard",
                        "traffic_settings_adjustable": False,
                        "truck_traffic_daily": 790
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [
                                10.008155631523788,
                                53.52340262988036
                            ],
                            [
                                10.008110854063379,
                                53.52340953042165
                            ]
                        ]
                    }
                },
                {
                    "id": "14",
                    "type": "Feature",
                    "properties": {
                        "car_traffic_daily": 3315,
                        "id": 1008,
                        "max_speed": 50,
                        "road_type": "street",
                        "traffic_settings_adjustable": False,
                        "truck_traffic_daily": 1560
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [
                                10.012255052066774,
                                53.52760814127168
                            ],
                            [
                                10.011019597442715,
                                53.52626243456985
                            ],
                            [
                                10.010700938438506,
                                53.52618422732218
                            ],
                            [
                                10.010564554024743,
                                53.526158413010855
                            ],
                            [
                                10.010291144054099,
                                53.52607982221758
                            ],
                            [
                                10.01010117316096,
                                53.525703887558784
                            ]
                        ]
                    }
                }
            ]
        }
    }

    run_noise_calculation(task)
