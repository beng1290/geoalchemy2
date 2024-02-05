"""This module defines specific functions for MySQL dialect."""

from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.sqltypes import NullType

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.functions import GenericFunction

_POSSIBLE_TYPES = [
    "geometry",
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
]


def reflect_geometry_column(inspector, table, column_info):
    return


def before_create(table, bind, **kw):
    return


def after_create(table, bind, **kw):
    return


def before_drop(table, bind, **kw):
    return


def after_drop(table, bind, **kw):
    return


#
# edit by Benjamin Green - 02.04.2024
# provide the name changes for mssql
# all functions typed as geometry will be
# designated [columnname].STFUNC()
#
_MSSQL_FUNCTIONS = {
    "ST_Area": ["STArea", Geometry],
    "ST_AsBinary": ["STAsBinary", Geometry],
    "ST_AsEWKB": ["STAsBinary", Geometry],
    "ST_AsText": ["STAsText", Geometry],
    "ST_Boundary": ["STBoundary", Geometry],
    "ST_Buffer": ["STBuffer", Geometry],
    "ST_Centroid": ["STCentroid", Geometry],
    "ST_Contains": ["STContains", Geometry],
    "ST_ConvexHull": ["STConvexHull", Geometry],
    "ST_Crosses": ["STCrosses", Geometry],
    #'': ['STCurveN', Geometry],
    "ST_CurveToLine": ["STCurveToLine", Geometry],
    "ST_Difference": ["STDifference", Geometry],
    "ST_Dimension": ["STDimension", Geometry],
    "ST_Disjoint": ["STDisjoint", Geometry],
    "ST_Distance": ["STDistance", Geometry],
    "ST_EndPoint": ["STEndpoint", Geometry],
    "ST_Envelope": ["STEnvelop", Geometry],
    "ST_Equals": ["STEquals", Geometry],
    "ST_ExteriorRing": ["STExteriorRing", Geometry],
    "ST_GeometryN": ["STGeometryN", Geometry],
    "ST_GeometryType": ["STGeometryType", Geometry],
    "ST_InteriorRingN": ["STInteriorRingN", Geometry],
    "ST_Intersection": ["STIntersection", Geometry],
    "ST_Intersects": ["STIntersects", Geometry],
    "ST_IsClosed": ["STIsClosed", Geometry],
    "ST_IsEmpty": ["STIsEmpty", Geometry],
    "ST_IsRing": ["STIsRing", Geometry],
    "ST_IsSimple": ["STIsSimple", Geometry],
    "ST_IsValid": ["STIsValid", Geometry],
    "ST_Length": ["STLength", Geometry],
    #'': ['STNumCurves', Geometry],
    "ST_NumGeometries": ["STNumGeometries", Geometry],
    "ST_NumInteriorRing": ["STNumInteriorRing", Geometry],
    "ST_NumPoints": ["STNumPoints", Geometry],
    "ST_Overlaps": ["STOverlaps", Geometry],
    "ST_PointN": ["STPointN", Geometry],
    "ST_PointOnSurface": ["STPointOnSurface", Geometry],
    "ST_Relate": ["STRelate", Geometry],
    "ST_SRID": ["STSrid", Geometry],
    "ST_StartPoint": ["STStartPoint", Geometry],
    "ST_SymDifference": ["STSymDifference", Geometry],
    "ST_Touches": ["STTouches", Geometry],
    "ST_Union": ["STUnion", Geometry],
    "ST_Within": ["STWithin", Geometry],
    "ST_X": ["STX", Geometry],
    "ST_Y": ["STY", Geometry],
    "ST_IsValidDetail": ["IsValidDetail", Geometry],
    "ST_M": ["M", Geometry],
    "ST_MakeValid": ["MakeValid", Geometry],
    "ST_ShortestLine": ["ShortestLineTo", Geometry],
    #'': ['BufferWithCurves', Geometry],
    #'': ['BufferWithTolerance', Geometry],
    #'': ['CurveToLineWithTolerance', Geometry],
    #'': ['Filter', Geometry],
    #'': ['HasM', Geometry],
    #'': ['HasZ', Geometry],
    #'': ['InstanceOf', Geometry],
    #'': ['IsNull', Geometry],
    #'': ['Reduce', Geometry],
    #'': ['MinDbCompatabilityLevel', Geometry],
    #'': ['ToString', Geometry],
    #'': ['Z', Geometry],
    "ST_GeomFromEWKT": "STGeomFromText",
    "ST_GeomFromText": "STGeomFromText",
    "ST_GeomFromEWKB": "STGeomFromWKB",
    "ST_GeomFromWKB": "STGeomFromWKB",
    # "ST_AsGeoJSON": ""
    # ToDo: should add the OGC Static Geometry Methods
    # ToDo: should empty methods not available in MSSQL
}


def _compiles_mssql(cls, fn):
    #
    # edit by Benjamin Green - 02.04.2024
    # allows for changing the type of the function
    # not just the function name, so that we can implement
    # more of the geometry functions in MSSQL
    #
    if isinstance(fn, list):
        #
        def _compile_mssql(element, compiler, **kw):
            #
            processed_args = compiler.process(element.clauses, **kw)
            processed_args = processed_args.split(",")
            #
            if len(processed_args) > 1:
                return f"{processed_args[0]}.{fn[0]} ({processed_args[1]})"
            else:
                return "{}.{}()".format(processed_args[0], fn[0])

    else:
        #
        def _compile_mssql(element, compiler, **kw):
            return "{}({})".format(fn, compiler.process(element.clauses, **kw))

    #
    compiles(getattr(functions, cls), "mssql")(_compile_mssql)


def register_mssql_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "mssql_function_name_1",
                    "function_name_2": "mssql_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_mssql(cls, fn)


register_mssql_mapping(_MSSQL_FUNCTIONS)


def _compile_GeomFromText_MSSql(element, compiler, **kw):
    element.identifier = "ST_GeomFromText"
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        return "{}({})".format(element.identifier, compiled)


def _compile_GeomFromWKB_MSSql(element, compiler, **kw):
    element.identifier = "ST_GeomFromWKB"
    wkb_data = list(element.clauses)[0].value
    if isinstance(wkb_data, memoryview):
        list(element.clauses)[0].value = wkb_data.tobytes()
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        return "{}({})".format(element.identifier, compiled)


@compiles(functions.ST_GeomFromText, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromText(element, compiler, **kw):
    return _compile_GeomFromText_MSSql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKT, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromEWKT(element, compiler, **kw):
    return _compile_GeomFromText_MSSql(element, compiler, **kw)


@compiles(functions.ST_GeomFromWKB, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MSSql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MSSql(element, compiler, **kw)
