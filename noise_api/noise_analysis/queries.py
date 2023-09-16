from string import Template

from noise_api.config import settings

# TODO do not use template here, but for now before finding best approach

CREATE_ALIAS = Template("""CREATE ALIAS IF NOT EXISTS $alias FOR \"$func\";""")

FUNCTIONS_TO_INIT = [
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

INSERT_BUILDING = Template(
    """
        -- Insert 1 building from automated string
        INSERT INTO buildings (the_geom) VALUES (ST_GeomFromText($building));
        """
)

RESET_BUILDINGS_TABLE = """
    drop table if exists buildings;
    create table buildings ( the_geom GEOMETRY );
    """

RESET_TRI_LVL_TABLE = """drop table if exists tri_lvl; create table tri_lvl as SELECT * from BR_TriGrid((select 
    st_expand(st_envelope(st_accum(the_geom)), 750, 750) the_geom from ROADS_SRC),'buildings','roads_src','DB_M','',
    {max_prop_distance},{max_wall_seeking_distance},{road_with},{receiver_densification},{max_triangle_area},
    {sound_reflection_order},{sound_diffraction_order},{wall_absorption}); """.format(
    **settings.computation.dict()
)

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
    DROP TABLE IF EXISTS roads_geo_and_traffic;

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


RESET_ROADS_GEOM_TABLE = """
        drop table if exists roads_geom;
        create table roads_geom ( the_geom GEOMETRY, NUM INTEGER, node_from INTEGER, node_to INTEGER, road_type INTEGER);
        """
