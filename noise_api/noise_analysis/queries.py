from string import Template

from noise_api.config import settings

# TODO do not use template here, but for now before finding best approach


H2GIS_SPATIAL = """
CREATE ALIAS IF NOT EXISTS H2GIS_SPATIAL FOR "org.h2gis.functions.factory.H2GISFunctions.load";
CALL H2GIS_SPATIAL();
"""

CREATE_ALIAS = Template('CREATE ALIAS IF NOT EXISTS $alias FOR "$func";')

FUNCTIONS_TO_INIT = [
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

INSERT_BUILDING = Template(
    """
    INSERT INTO buildings (the_geom) VALUES (ST_GeomFromText($building));
"""
)

RESET_BUILDINGS_TABLE = """
    DROP TABLE IF EXISTS buildings;
    CREATE TABLE buildings (the_geom GEOMETRY);
"""

RESET_TRI_LVL_TABLE = Template(
    """
    DROP TABLE IF EXISTS tri_lvl;
    CREATE TABLE tri_lvl
        AS SELECT *
            FROM BR_TriGrid(
                    (SELECT
                        ST_Expand(ST_Envelope(ST_Accum(the_geom)), 750, 750) the_geom
                        FROM roads_src),
                    'buildings',
                    'roads_src',
                    'db_m',
                    '',
                    $max_prop_distance,
                    $max_wall_seeking_distance,
                    $road_with,
                    $receiver_densification,
                    $max_triangle_area,
                    $sound_reflection_order,
                    $sound_diffraction_order,
                    $wall_absorption
                );
"""
).substitute(**settings.computation.dict())


RESET_ROADS_TRAFFIC_TABLE = """
    DROP TABLE IF EXISTS roads_traffic;
    CREATE TABLE roads_traffic (
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

RESET_ROADS_DIR_TABLES = """
    DROP TABLE IF EXISTS roads_dir_one;
    DROP TABLE IF EXISTS roads_dir_two;

    CREATE TABLE roads_dir_one AS
        SELECT
            the_geom,
            road_type,
            load_speed,
            junction_speed,
            max_speed,
            lightVehicleCount,
            heavyVehicleCount,
            train_speed,
            trains_per_hour,
            ground_type,
            has_anti_vibration
        FROM roads_geom AS
            geo, roads_traffic traff
        WHERE
            geo.node_from = traff.node_from
        AND
            geo.node_to = traff.node_to;

    CREATE TABLE roads_dir_two AS
        SELECT
            the_geom,
            road_type,
            load_speed,
            junction_speed,
            max_speed,
            lightVehicleCount,
            heavyVehicleCount,
            train_speed,
            trains_per_hour,
            ground_type,
            has_anti_vibration
        FROM roads_geom AS
            geo, roads_traffic traff
        WHERE
            geo.node_to = traff.node_from
        AND
            geo.node_from = traff.node_to;

    DROP TABLE IF EXISTS roads_geo_and_traffic;
    CREATE TABLE roads_geo_and_traffic AS
        SELECT
            *
        FROM
            roads_dir_one
        UNION
            SELECT
                *
            FROM roads_dir_two;
"""


RESET_TRICONTOURING_MAP = """
    DROP TABLE IF EXISTS tricontouring_noise_map;
    CREATE TABLE tricontouring_noise_map
        AS SELECT *
            FROM ST_TriangleContouring(
                'tri_lvl',
                'w_v1',
                'w_v2',
                'w_v3',
                31622,
                100000,
                316227,
                1000000,
                3162277,
                1e+7,
                31622776,
                1e+20
            );

    DROP TABLE IF EXISTS multipolygon_iso;
    CREATE TABLE multipolygon_iso AS
        SELECT
            ST_UNION(ST_ACCUM(the_geom)) the_geom,
            idiso,
            CELL_ID
        FROM tricontouring_noise_map
        GROUP BY
            IDISO,
            CELL_ID;

    DROP TABLE IF EXISTS simple_noise_map;
    DROP TABLE IF EXISTS contouring_noise_map;
    CREATE TABLE contouring_noise_map AS
        SELECT
            ST_Transform(ST_SETSRID(the_geom, {0}), {1}),
            idiso,
            CELL_ID
        FROM
            ST_Explode('multipolygon_iso');
    DROP TABLE multipolygon_iso;
""".format(
    25832, 4326
)

RESET_ROADS_GLOBAL_TABLE = """
    drop table if exists roads_src_global;
    CREATE TABLE roads_src_global AS SELECT the_geom,
    CASEWHEN(
        road_type = 99,
        BTW_EvalSource(train_speed, trains_per_hour, ground_type, has_anti_vibration),
        BR_EvalSource(load_speed,lightVehicleCount,heavyVehicleCount,junction_speed,max_speed,road_type,ST_Z(ST_GeometryN(ST_ToMultiPoint(the_geom),1)),ST_Z(ST_GeometryN(ST_ToMultiPoint(the_geom),2)),ST_Length(the_geom),False)
        ) as db_m from roads_geo_and_traffic;
"""

RESET_ROADS_SRC_TABLE = """
    DROP TABLE IF EXISTS roads_src;
    CREATE TABLE roads_src AS
        SELECT the_geom,
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
            BR_SpectrumRepartition(5000,1,db_m) as db_m5000
        FROM roads_src_global
"""


RESET_ROADS_GEOM_TABLE = """
    DROP TABLE IF EXISTS roads_geom;
    CREATE TABLE roads_geom (
        the_geom GEOMETRY,
        num INTEGER,
        node_from INTEGER,
        node_to INTEGER,
        road_type INTEGER);
"""
