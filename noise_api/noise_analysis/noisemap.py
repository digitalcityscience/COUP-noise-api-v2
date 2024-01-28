import json
import logging
import os
import shlex
import subprocess
from pathlib import Path

import geopandas as gpd
import psycopg2
from shapely.geometry import box
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


def get_geojson_path(filename: str):
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


def get_settings():
    return {
        "settings_name": "max triangle area",
        "max_prop_distance": 750,  # the lower the less accurate
        "max_wall_seeking_distance": 50,  # the lower  the less accurate
        "road_with": 1.5,  # the higher the less accurate
        "receiver_densification": 2.8,  # the higher the less accurate
        "max_triangle_area": 275,  # the higher the less accurate
        "sound_reflection_order": 0,  # the higher the less accurate
        "sound_diffraction_order": 0,  # the higher the less accurate
        "wall_absorption": 0.23,  # the higher the less accurate
    }


def calculate_noise_result(
    cursor, buildings_geojson, roads_geojson, traffic_settings
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

    print("Make roads table (just geometries and road type)..")
    reset_all_roads()
    cursor.execute(queries.RESET_ROADS_GEOM_TABLE)
    roads_queries = get_road_queries(roads_gdf, traffic_settings)
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

    print("Applying frequency repartition of road noise level ...")
    cursor.execute(queries.RESET_ROADS_SRC_TABLE)

    print("Please wait, sound propagation from sources through buildings ...")

    cursor.execute(
        """drop table if exists tri_lvl; create table tri_lvl as SELECT * from BR_TriGrid((select
    st_expand(st_envelope(st_accum(the_geom)), 750, 750) the_geom from ROADS_SRC),'buildings','roads_src','DB_M','',
    {max_prop_distance},{max_wall_seeking_distance},{road_with},{receiver_densification},{max_triangle_area},
    {sound_reflection_order},{sound_diffraction_order},{wall_absorption}); """.format(
            **get_settings()
        )
    )

    # this leads to an invalid tri_lvl table , with all receiver values = 1 . replaced with old query above
    # cursor.execute(queries.RESET_TRI_LVL_TABLE)

    print("Computation done !")

    print("Creating isocountour and save it as a geojson in the working folder..")
    cursor.execute(queries.RESET_TRICONTOURING_MAP)

    noise_result_geojson = export_result_from_db_to_geojson(cursor)

    # clip to buildings extend
    result_gdf = gpd.GeoDataFrame.from_features(
        noise_result_geojson["features"], crs="EPSG:4326"
    )
    # rename "idiso" column to "value"
    result_gdf = result_gdf.rename(columns={"idiso": "value"})
    result_gdf_clip = gpd.clip(
        result_gdf, box(*list(buildings_gdf.to_crs("EPSG:4326").total_bounds))
    )

    return json.loads(result_gdf_clip.to_json())


def run_noise_calculation(task_def: dict):
    with H2DatabaseContextManager() as h2_context:
        noise_result_geojson = calculate_noise_result(
            h2_context.psycopg2_cursor,
            task_def["buildings"],
            task_def["roads"],
            {
                "max_speed": task_def.get("max_speed", None),
                "traffic_quota": task_def.get("traffic_quota", None),
            },
        )

    # Try to make noise computation even faster
    # by adjustiong: https://github.com/Ifsttar/NoiseModelling/blob/master/noisemap-core/
    # src/main/java/org/orbisgis/noisemap/core/jdbc/JdbcNoiseMap.java#L30
    # by shifting to GB center
    #   https: // github.com / Ifsttar / NoiseModelling / blob / master / noisemap - core / src / main / java / org /
    #   orbisgis / noisemap / core / jdbc / JdbcNoiseMap.java  # L68

    return {"geojson": noise_result_geojson}
