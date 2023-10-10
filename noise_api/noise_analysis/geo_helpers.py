import geopandas as gpd
from shapely import get_coordinates, wkb
from shapely.geometry import LineString, MultiLineString, Polygon


def geojson_to_gdf_with_metric_crs(geojson_wgs: dict) -> gpd.GeoDataFrame:
    gdf_wgs = gpd.GeoDataFrame.from_features(geojson_wgs)
    gdf_wgs = gdf_wgs.set_crs("EPSG:4326", allow_override=True)

    return gdf_wgs.to_crs("EPSG:25832")


def all_z_values_to_zero(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = drop_z_value_from_coords(gdf)  # drop any z values first
    gdf["geometry"] = gdf["geometry"].apply(add_zero_as_z_value)  # set all z to 0

    return gdf


def drop_z_value_from_coords(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # DROP Z VALUES IN GEOMETRY IF EXISTS
    def _drop_z(geom):
        return wkb.loads(wkb.dumps(geom, output_dimension=2))

    gdf.geometry = gdf.geometry.transform(_drop_z)

    return gdf

def add_zero_as_z_value(geometry: gpd.GeoSeries):
    """
    Reset the Z value of each coordinate of the given geometry to 0.
    """
    if isinstance(geometry, LineString):
        coords = [
            (x, y, 0) for x, y in list(get_coordinates(geometry, include_z=False))
        ]
        return LineString(coords)

    if isinstance(geometry, MultiLineString):
        coords = get_coordinates(geometry, include_z=False)
        fixed_coords = [[(x, y, 0) for x, y in line] for line in coords]

        return MultiLineString(fixed_coords)

    if isinstance(geometry, Polygon):
        exterior = [(x, y, 0) for x, y in list(geometry.exterior.coords)]
        interiors = []
        for interior in geometry.interiors:
            interiors.append([(x, y, 0) for x, y in list(interior.coords)])

        return Polygon(exterior, holes=interiors)
    else:
        raise ValueError(
            "Provide buildings as polygons and roads as (Multi)LineStrings"
        )
