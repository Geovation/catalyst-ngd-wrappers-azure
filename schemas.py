'''
Schemas for the API endpoints.
    - ColSchema,
    - LimitGeomSchema,
    - LimitColSchema,
    - GeomColSchema,
    - LimitGeomColSchema
'''

from marshmallow import Schema, INCLUDE
from marshmallow.fields import Integer, String, Boolean, List


class BaseSchema(Schema):
    '''Abstract schema for logs queries'''
    log_request_details = Boolean(data_key='log-request-details', required=False)

    class Meta:
        '''Pass other fields forward to the API'''
        unknown = INCLUDE

class CollectionsSchema(BaseSchema):
    '''Schema for the latest collections endpoint'''
    recent_update_days = Integer(data_key='recent-update-days', required=False)

class FeaturesBaseSchema(BaseSchema):
    '''Base schema for all queries'''
    wkt = String(required=False)
    use_latest_collection = Boolean(data_key='use-latest-collection', required=False)

class AbstractHierarchicalSchema(FeaturesBaseSchema):
    '''Abstract schema for hierarchical queries'''
    hierarchical_output = Boolean(data_key='hierarchical-output', required=False)

class LimitSchema(FeaturesBaseSchema):
    '''limit is the maximum number of items to return'''
    request_limit = Integer(data_key='request-limit', required=False)

class GeomSchema(AbstractHierarchicalSchema):
    '''wkt is a well-known text representation of a geometry'''

class ColSchema(AbstractHierarchicalSchema):
    '''col is a list of collections to query'''
    collection = List(String(), required=True)

class LimitGeomSchema(LimitSchema, GeomSchema):
    '''Combining Limit and Geom schemas'''

class LimitColSchema(LimitSchema, ColSchema):
    '''Combining Limit and Col schemas'''

class GeomColSchema(GeomSchema, ColSchema):
    '''Combining Geom and Col schemas'''

class LimitGeomColSchema(LimitSchema, GeomSchema, ColSchema):
    '''Combining Limit, Geom and Col schemas'''
