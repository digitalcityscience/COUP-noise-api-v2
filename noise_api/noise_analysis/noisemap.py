from __future__ import print_function

import json
import os
import shlex
import subprocess
from pathlib import Path
from time import sleep

from noise_api.config import settings
from noise_api.noise_analysis.queries import CREATE_ALIAS
from noise_api.noise_analysis.sql_query_builder import (
    get_road_queries,
    get_traffic_queries,
    make_building_queries,
    reset_all_roads,
)


def get_result_path():
    cwd = os.path.dirname(os.path.abspath(__file__))
    results_folder = os.path.abspath(cwd + "/results/")

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)

    return os.path.abspath(results_folder + "/" "result.geojson")


ORBISGIS_DIR = Path(__file__).parent / "orbisgis_java"


def boot_h2_database_in_subprocess():
    java_command = (
        'java -cp "bin/*:bundle/*:sys-bundle/*" org.h2.tools.Server -pg -trace'
    )

    args = shlex.split(java_command)
    f = open("log.txt", "w+")
    p = subprocess.Popen(args, cwd=ORBISGIS_DIR, stdout=f)
    print("ProcessID H2-database ", p.pid)

    # allow time for database booting
    sleep(2)

    # database process is running
    import_tries = 0
    while p.poll() is None:
        try:
            import psycopg2
        except ImportError:
            print("Could not connect to database.")
            print("Trying again in 5sec")
            sleep(5)
            import_tries += 1
        # successful import continue with project
        else:
            print("Sucessfully imported psycopg2")
            return p, psycopg2
        # terminate database process in case something went wrong
        finally:
            if import_tries > 4:
                stdout, stderr = p.communicate()
                print("Could not import psycopg2, trouble with database process")
                print(stdout, stderr)
                p.terminate()


# invokes H2GIS functions in the database
# returns the database cursor (psycopg2)
def initiate_database_connection(psycopg2):
    # TODO: invoke db from subprocess if not running
    # Define our connection string
    # db name has to be an absolute path
    db_name = (os.path.abspath(".") + os.sep + "mydb").replace(os.sep, "/")
    conn_string = (
        f"host='localhost' port=5435 dbname='{db_name}' user='sa' password='sa'"
    )

    # print the connection string we will use to connect
    print("Connecting to database\n	->%s" % (conn_string))

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print("Connected!\n")

    # Initialize NoiseModelling functions
    functions_to_initialize = [
        ("H2GIS_SPATIAL", "org.h2gis.functions.factory.H2GISFunctions.load"),
        ("BR_PtGrid3D", "org.orbisgis.noisemap.h2.BR_PtGrid3D.noisePropagation"),
        ("BR_PtGrid", "org.orbisgis.noisemap.h2.BR_PtGrid.noisePropagation"),
        (
            "BR_SpectrumRepartition",
            "org.orbisgis.noisemap.h2.BR_SpectrumRepartition.spectrumRepartition",
        ),
        ("BR_EvalSource", "org.orbisgis.noisemap.h2.BR_EvalSource.evalSource"),
        ("BTW_EvalSource", "org.orbisgis.noisemap.h2.BTW_EvalSource.evalSource"),
        (
            "BR_SpectrumRepartition",
            "org.orbisgis.noisemap.h2.BR_SpectrumRepartition.spectrumRepartition",
        ),
        ("BR_TriGrid", "org.orbisgis.noisemap.h2.BR_TriGrid.noisePropagation"),
        ("BR_TriGrid3D", "org.orbisgis.noisemap.h2.BR_TriGrid3D.noisePropagation"),
    ]

    for alias_name, function_name in functions_to_initialize:
        cursor.execute(
            CREATE_ALIAS.substitute(
                alias=alias_name,
                func=function_name,
            )
        )

    cursor.execute("CALL H2GIS_SPATIAL();")

    return conn, cursor


def calculate_noise_result(
    cursor, traffic_settings, buildings_geojson, roads_geojson
) -> dict:
    # Scenario sample
    # Sending/Receiving geometry data using odbc connection is very slow
    # It is advised to use shape file or other storage format, so use SHPREAD or FILETABLE sql functions

    print("make buildings table ..")

    reset_all_roads()

    cursor.execute(
        """
    drop table if exists buildings;
    create table buildings ( the_geom GEOMETRY );
    """
    )

    buildings_queries = make_building_queries(buildings_geojson)
    for building in buildings_queries:
        # print('building:', building)
        # Inserting building into database
        cursor.execute(
            """
        -- Insert 1 building from automated string
        INSERT INTO buildings (the_geom) VALUES (ST_GeomFromText({0}));
        """.format(
                building
            )
        )

    print("Make roads table (just geometries and road type)..")
    cursor.execute(
        """
        drop table if exists roads_geom;
        create table roads_geom ( the_geom GEOMETRY, NUM INTEGER, node_from INTEGER, node_to INTEGER, road_type INTEGER);
        """
    )
    roads_queries = get_road_queries(traffic_settings, roads_geojson)
    for road in roads_queries:
        # print('road:', road)
        cursor.execute("""{0}""".format(road))

    print("Make traffic information table..")
    cursor.execute(
        """
    drop table if exists roads_traffic;
     create table roads_traffic ( 
	node_from INTEGER,
	node_to INTEGER,
	load_speed DOUBLE,
	junction_speed DOUBLE,
	max_speed DOUBLE,
	lightVehicleCount DOUBLE,
	heavyVehicleCount DOUBLE,
	train_speed DOUBLE,
	trains_per_hour DOUBLE,
	ground_type INTEGER,
	has_anti_vibration BOOLEAN
	);
    """
    )

    traffic_queries = get_traffic_queries()
    for traffic_query in traffic_queries:
        print(traffic_query)
        cursor.execute("""{0}""".format(traffic_query))

    print("Duplicate geometries to give sound level for each traffic direction..")
    cursor.execute(
        """
    drop table if exists roads_dir_one;
    drop table if exists roads_dir_two;
    CREATE TABLE roads_dir_one AS SELECT the_geom,road_type,load_speed,junction_speed,max_speed,lightVehicleCount,heavyVehicleCount, train_speed, trains_per_hour, ground_type, has_anti_vibration FROM roads_geom as geo,roads_traffic traff WHERE geo.node_from=traff.node_from AND geo.node_to=traff.node_to;
    CREATE TABLE roads_dir_two AS SELECT the_geom,road_type,load_speed,junction_speed,max_speed,lightVehicleCount,heavyVehicleCount, train_speed, trains_per_hour, ground_type, has_anti_vibration FROM roads_geom as geo,roads_traffic traff WHERE geo.node_to=traff.node_from AND geo.node_from=traff.node_to;
    -- Collapse two direction in one table
    drop table if exists roads_geo_and_traffic;
    CREATE TABLE roads_geo_and_traffic AS select * from roads_dir_one UNION select * from roads_dir_two;"""
    )

    print("Compute the sound level for each segment of roads..")

    # compute the power of the noise source and add it to the table roads_src_global
    # for railroads (road_type = 99) use the function BTW_EvalSource (TW = Tramway)
    # for car roads use the function BR_EvalSource
    cursor.execute(
        """
    drop table if exists roads_src_global;
    CREATE TABLE roads_src_global AS SELECT the_geom, 
    CASEWHEN(
        road_type = 99,
        BTW_EvalSource(train_speed, trains_per_hour, ground_type, has_anti_vibration),
        BR_EvalSource(load_speed,lightVehicleCount,heavyVehicleCount,junction_speed,max_speed,road_type,ST_Z(ST_GeometryN(ST_ToMultiPoint(the_geom),1)),ST_Z(ST_GeometryN(ST_ToMultiPoint(the_geom),2)),ST_Length(the_geom),False)
        ) as db_m from roads_geo_and_traffic;
	"""
    )

    print("Apply frequency repartition of road noise level..")

    cursor.execute(
        """
    drop table if exists roads_src;
    CREATE TABLE roads_src AS SELECT the_geom,
    BR_SpectrumRepartition(100,1,db_m) as db_m100,
    BR_SpectrumRepartition(125,1,db_m) as db_m125,
    BR_SpectrumRepartition(160,1,db_m) as db_m160,
    BR_SpectrumRepartition(200,1,db_m) as db_m200,
    BR_SpectrumRepartition(250,1,db_m) as db_m250,
    BR_SpectrumRepartition(315,1,db_m) as db_m315,
    BR_SpectrumRepartition(400,1,db_m) as db_m400,
    BR_SpectrumRepartition(500,1,db_m) as db_m500,
    BR_SpectrumRepartition(630,1,db_m) as db_m630,
    BR_SpectrumRepartition(800,1,db_m) as db_m800,
    BR_SpectrumRepartition(1000,1,db_m) as db_m1000,
    BR_SpectrumRepartition(1250,1,db_m) as db_m1250,
    BR_SpectrumRepartition(1600,1,db_m) as db_m1600,
    BR_SpectrumRepartition(2000,1,db_m) as db_m2000,
    BR_SpectrumRepartition(2500,1,db_m) as db_m2500,
    BR_SpectrumRepartition(3150,1,db_m) as db_m3150,
    BR_SpectrumRepartition(4000,1,db_m) as db_m4000,
    BR_SpectrumRepartition(5000,1,db_m) as db_m5000 from roads_src_global;"""
    )

    print("Please wait, sound propagation from sources through buildings..")

    cursor.execute(
        """drop table if exists tri_lvl; create table tri_lvl as SELECT * from BR_TriGrid((select 
    st_expand(st_envelope(st_accum(the_geom)), 750, 750) the_geom from ROADS_SRC),'buildings','roads_src','DB_M','',
    {max_prop_distance},{max_wall_seeking_distance},{road_with},{receiver_densification},{max_triangle_area},
    {sound_reflection_order},{sound_diffraction_order},{wall_absorption}); """.format(
            **settings.computation.dict()
        )
    )

    print("Computation done !")

    print("Create isocountour and save it as a geojson in the working folder..")

    cursor.execute(
        """
    drop table if exists tricontouring_noise_map;
    -- create table tricontouring_noise_map AS SELECT * from ST_SimplifyPreserveTopology(ST_TriangleContouring('tri_lvl','w_v1','w_v2','w_v3',31622, 100000, 316227, 1000000, 3162277, 1e+7, 31622776, 1e+20));
    create table tricontouring_noise_map AS SELECT * from ST_TriangleContouring('tri_lvl','w_v1','w_v2','w_v3',31622, 100000, 316227, 1000000, 3162277, 1e+7, 31622776, 1e+20);
    -- Merge adjacent triangle into polygons (multiple polygon by row, for unique isoLevel and cellId key)
    drop table if exists multipolygon_iso;
    create table multipolygon_iso as select ST_UNION(ST_ACCUM(the_geom)) the_geom ,idiso, CELL_ID from tricontouring_noise_map GROUP BY IDISO, CELL_ID;
    -- Explode each row to keep only a polygon by row
    drop table if exists simple_noise_map;
    -- example form internet : CREATE TABLE roads2 AS SELECT id_way, ST_PRECISIONREDUCER(ST_SIMPLIFYPRESERVETOPOLOGY(THE_GEOM),0.1),1) the_geom, highway_type t FROM roads; 
    -- ST_SimplifyPreserveTopology(geometry geomA, float tolerance);
    -- create table simple_noise_map as select ST_SIMPLIFYPRESERVETOPOLOGY(the_geom, 2) the_geom, idiso, CELL_ID from multipolygon_iso;
    drop table if exists contouring_noise_map;
    -- create table CONTOURING_NOISE_MAP as select ST_Transform(ST_SETSRID(the_geom,{0}),{1}),idiso, CELL_ID from ST_Explode('simple_noise_map'); 
    create table CONTOURING_NOISE_MAP as select ST_Transform(ST_SETSRID(the_geom,{0}),{1}),idiso, CELL_ID from ST_Explode('multipolygon_iso'); 
    drop table multipolygon_iso;""".format(
            25832, 4326
        )
    )

    # export result from database to geojson
    # time_stamp = str(datetime.now()).split('.', 1)[0].replace(' ', '_').replace(':', '_')

    geojson_path = get_result_path()
    cursor.execute("CALL GeoJsonWrite('" + geojson_path + "', 'CONTOURING_NOISE_MAP');")

    print("*********")
    print(traffic_queries)
    print("*********")

    with open(geojson_path) as f:
        resultdata = json.load(f)

        return resultdata


def run_noise_calculation(task_def: dict):
    h2_subprocess, psycopg2 = boot_h2_database_in_subprocess()
    conn, psycopg2_cursor = initiate_database_connection(psycopg2)

    # get noise result as json
    noise_result_geojson = calculate_noise_result(
        psycopg2_cursor,
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

    # close connections to database
    print("closing cursor")
    psycopg2_cursor.close()

    print("closing database connection")
    conn.close()

    # terminate database process as it constantly blocks memory
    h2_subprocess.terminate()

    # Try to make noise computation even faster
    # by adjustiong: https://github.com/Ifsttar/NoiseModelling/blob/master/noisemap-core/
    # src/main/java/org/orbisgis/noisemap/core/jdbc/JdbcNoiseMap.java#L30
    # by shifting to GB center
    #   https: // github.com / Ifsttar / NoiseModelling / blob / master / noisemap - core / src / main / java / org /
    #   orbisgis / noisemap / core / jdbc / JdbcNoiseMap.java  # L68

    # if calculation_settings["result_format"] == "png":
    #     return convert_result_to_png(noise_result_geojson)

    return noise_result_geojson
