from __future__ import print_function

import json
import logging
import os
import shlex
import subprocess
from pathlib import Path
from time import sleep

from noise_api.noise_analysis import queries
from noise_api.noise_analysis.sql_query_builder import (
    get_road_queries,
    get_traffic_queries,
    make_building_queries,
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

    def boot_h2_database_in_subprocess(self):
        java_command = (
            'java -cp "bin/*:bundle/*:sys-bundle/*" org.h2.tools.Server -pg -trace'
        )

        args = shlex.split(java_command)
        f = open("log.txt", "w+")
        p = subprocess.Popen(args, cwd=ORBISGIS_DIR, stdout=f)
        print("ProcessID H2-database ", p.pid)

        # Allow time for database booting
        sleep(2)

        # Database process is running
        import_tries = 0
        while p.poll() is None:
            try:
                import psycopg2
            except ImportError:
                print("Could not connect to database.")
                print("Trying again in 5 seconds")
                sleep(5)
                import_tries += 1
            else:
                print("Successfully imported psycopg2")
                return p, psycopg2
            finally:
                if import_tries > 4:
                    stdout, stderr = p.communicate()
                    print("Could not import psycopg2, trouble with database process")
                    print(stdout, stderr)
                    p.terminate()

    def initiate_database_connection(self, psycopg2):
        # Define our connection string
        # DB name has to be an absolute path
        conn_string = (
            f"host='localhost' port=5435 dbname='{DB_NAME}' user='sa' password='sa'"
        )

        # Print the connection string we will use to connect
        print("Connecting to database\n ->%s" % (conn_string))

        # Get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
        print("Connected!\n")

        # Initialize NoiseModelling functions
        for alias_name, function_name in queries.FUNCTIONS_TO_INIT:
            cursor.execute(
                queries.CREATE_ALIAS.substitute(
                    alias=alias_name,
                    func=function_name,
                )
            )

        cursor.execute("CALL H2GIS_SPATIAL();")

        return conn, cursor

    def cleanup(self):
        # Close connections to the database
        print("Closing cursor")
        self.psycopg2_cursor.close()

        print("Closing database connection")
        self.conn.close()

        # Terminate the database process as it constantly blocks memory
        self.h2_subprocess.terminate()


def get_result_path():
    cwd = os.path.dirname(os.path.abspath(__file__))
    results_folder = os.path.abspath(f"{cwd}/results/")

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)

    return os.path.abspath(f"{results_folder}/result.geojson")


ORBISGIS_DIR = Path(__file__).parent / "orbisgis_java"


def export_result_from_db_to_geojson(cursor):
    geojson_path = get_result_path()
    cursor.execute(f"CALL GeoJsonWrite('{geojson_path}', 'CONTOURING_NOISE_MAP');")

    with open(geojson_path) as f:
        return json.load(f)


def calculate_noise_result(
    cursor, traffic_settings, buildings_geojson, roads_geojson
) -> dict:
    # Scenario sample
    # Sending/Receiving geometry data using odbc connection is very slow
    # It is advised to use shape file or other storage format, so use SHPREAD or FILETABLE sql functions

    reset_all_roads()

    cursor.execute(queries.RESET_BUILDINGS_TABLE)
    cursor.execute(queries.RESET_ROADS_GEOM_TABLE)
    cursor.execute(queries.RESET_ROADS_TRAFFIC_TABLE)

    print("Making buildings table ...")
    buildings_queries = make_building_queries(buildings_geojson)
    for building in buildings_queries:
        cursor.execute(queries.INSERT_BUILDING.substitute(building=building))

    print("Making roads table ...")
    roads_queries = get_road_queries(traffic_settings, roads_geojson)
    for road in roads_queries:
        cursor.execute("""{0}""".format(road))

    print("Making traffic information table ...")
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

    print("Applying frequency repartition of road noise level ...")
    cursor.execute(queries.RESET_ROADS_SRC_TABLE)

    print("Please wait, sound propagation from sources through buildings ...")
    cursor.execute(queries.RESET_TRI_LVL_TABLE)

    print("Computation done !")

    print("Creating isocountour and save it as a geojson in the working folder..")
    cursor.execute(queries.RESET_TRICONTOURING_MAP)

    return export_result_from_db_to_geojson(cursor)


def run_noise_calculation(task_def: dict):
    with H2DatabaseContextManager() as h2_context:
        # get noise result as json
        noise_result_geojson = calculate_noise_result(
            h2_context.psycopg2_cursor,
            {
                "max_speed": task_def["max_speed"],
                "traffic_quota": task_def["traffic_quota"],
            },
            task_def["buildings"],
            task_def["roads"],
        )

    # TODO remove user from here and investigate how to implement clipping
    # cityPyo_user = ""

    # noise_result_geojson = clip_gdf_to_project_area(noise_result_geojson, cityPyo_user)
    # print("Result geojson save in ", noise_result_geojson)

    # # close connections to database
    # print("closing cursor")
    # psycopg2_cursor.close()

    # print("closing database connection")
    # conn.close()

    # # terminate database process as it constantly blocks memory
    # h2_subprocess.terminate()

    # Try to make noise computation even faster
    # by adjustiong: https://github.com/Ifsttar/NoiseModelling/blob/master/noisemap-core/
    # src/main/java/org/orbisgis/noisemap/core/jdbc/JdbcNoiseMap.java#L30
    # by shifting to GB center
    #   https: // github.com / Ifsttar / NoiseModelling / blob / master / noisemap - core / src / main / java / org /
    #   orbisgis / noisemap / core / jdbc / JdbcNoiseMap.java  # L68

    # if calculation_settings["result_format"] == "png":
    #     return convert_result_to_png(noise_result_geojson)

    return noise_result_geojson
