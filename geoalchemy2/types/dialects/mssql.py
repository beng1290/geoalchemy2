"""This module defines specific functions for MSSQL dialect."""
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


def bind_processor_process(spatial_type, bindvalue):
    return bindvalue
