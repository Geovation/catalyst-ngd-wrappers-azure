import json
import azure.functions as func

from azure.functions import HttpRequest, HttpResponse
from azure.monitor.opentelemetry import configure_azure_monitor
from azure.monitor.events.extension import track_event

from marshmallow import Schema, INCLUDE, EXCLUDE
from marshmallow.fields import Integer, String, Boolean, List
from marshmallow.exceptions import ValidationError

from catalyst_ngd_wrappers import *

if os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING"):
    configure_azure_monitor()

LOG_REQUEST_DETAILS: bool = os.environ.get('LOG_REQUEST_DETAILS', 'True') == 'True'
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

class LatestCollectionsSchema(Schema):
    '''Schema for the latest collections endpoint'''
    flag_recent_updates = Boolean(data_key='flag-recent-updates', required=False)
    recent_update_days = Integer(data_key='recent-update-days', required=False)

    class Meta:
        '''Exclude unknown/extra fields'''
        unknown = EXCLUDE


class BaseSchema(Schema):
    '''Base schema for all queries'''
    wkt = String(required=False)
    use_latest_collection = Boolean(data_key='use-latest-collection', required=False)
    access_token = String(data_key='access-token', required=False)

    class Meta:
        '''Allows additional fields to pass through to query_params'''
        unknown = INCLUDE  # Allows additional fields to pass through to query_params


class AbstractHierarchicalSchema(BaseSchema):
    '''Abstract schema for hierarchical queries'''
    hierarchical_output = Boolean(data_key='hierarchical-output', required=False)


class LimitSchema(BaseSchema):
    '''limit is the maximum number of items to return'''
    limit = Integer(required=False)
    request_limit = Integer(data_key='request-limit', required=False)


class GeomSchema(AbstractHierarchicalSchema):
    '''wkt is a well-known text representation of a geometry'''
    wkt = String(required=True)


class ColSchema(AbstractHierarchicalSchema):
    '''col is a list of collections to query'''
    collection = List(String(), required=True)


class LimitGeomSchema(LimitSchema, GeomSchema):
    '''Combining Limit and Geom schemas'''
    wkt = String(required=True)


class LimitColSchema(LimitSchema, ColSchema):
    '''Combining Limit and Col schemas'''


class GeomColSchema(GeomSchema, ColSchema):
    '''Combining Geom and Col schemas'''
    wkt = String(required=True)


class LimitGeomColSchema(LimitSchema, GeomSchema, ColSchema):
    '''Combining Limit, Geom and Col schemas'''
    wkt = String(required=True)


@app.function_name('http_latest_collections')
@app.route("catalyst/features/latest-collections")
def http_latest_collections(req: HttpRequest) -> HttpResponse:

    if req.method != 'GET':
        code = 405
        error_body = json.dumps({
            "code": code,
            "description": "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
            "errorSource": "Catalyst Wrapper"
        })
        return HttpResponse(
            body=error_body,
            mimetype="application/json",
            status_code=code
        )

    schema = LatestCollectionsSchema()

    params = {**req.params}

    try:
        parsed_params = schema.load(params)
    except ValidationError as e:
        code = 400
        error_body = json.dumps({
            "code": code,
            "description": str(e),
            "errorSource": "Catalyst Wrapper"
        })
        return HttpResponse(
            error_body,
            mimetype="application/json",
            status_code=400
        )

    data = get_latest_collection_versions(**parsed_params)
    json_data = json.dumps(data)

    custom_dimensions = {f'query_params.{str(k)}': str(v) for k, v in parsed_params.items()}
    custom_dimensions.pop('key', None)
    custom_dimensions.pop('access_token', None)
    custom_dimensions.update({
        'method': 'GET',
        'url.path': req.url,
    })

    track_event('HTTP_Request', custom_dimensions=custom_dimensions)

    return HttpResponse(
        body=json_data,
        mimetype="application/json"
    )


@app.function_name('http_latest_single_col')
@app.route("catalyst/features/latest-collections/{collection}")
def http_latest_single_col(req: HttpRequest) -> HttpResponse:

    if req.method != 'GET':
        code = 405
        error_body = json.dumps({
            "code": code,
            "description": "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
            "errorSource": "Catalyst Wrapper"
        })
        return HttpResponse(
            body=error_body,
            mimetype="application/json",
            status_code=code
        )

    schema = LatestCollectionsSchema()
    collection = req.route_params.get('collection')

    params = {**req.params}
    try:
        parsed_params = schema.load(params)
    except ValidationError as e:
        code = 400
        error_body = json.dumps({
            "code": code,
            "description": str(e),
            "errorSource": "Catalyst Wrapper"
        })
        return HttpResponse(
            body=error_body,
            mimetype="application/json",
            status_code=code
        )

    data = get_specific_latest_collections([collection], **parsed_params)
    json_data = json.dumps(data)

    custom_dimensions = {f'query_params.{str(k)}': str(
        v) for k, v in parsed_params.items()}
    custom_dimensions.pop('key', None)
    custom_dimensions.update({
        'method': 'GET',
        'url.path': req.url,
        'url.path_params.collection': collection,
    })

    track_event('HTTP_Request', custom_dimensions=custom_dimensions)

    return HttpResponse(
        body=json_data,
        mimetype="application/json"
    )


def delistify(params: dict) -> None:
    for k, v in params.items():
        if k != 'collection':
            params[k] = v[0]


def construct_response(req: HttpRequest, schema_class: type, func: callable) -> HttpResponse:

    try:
        if req.method != 'GET':
            code = 405
            error_body = json.dumps({
                "code": code,
                "description": "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
                "errorSource": "Catalyst Wrapper"
            })
            return HttpResponse(
                body=error_body,
                mimetype="application/json",
                status_code=code
            )

        schema = schema_class()
        collection = req.route_params.get('collection')

        params = {**req.params}

        if not (collection):
            col = params.get('collection')
            if col:
                params['collection'] = params.get('collection').split(',')
        try:
            parsed_params = schema.load(params)
        except ValidationError as e:
            code = 400
            error_body = json.dumps({
                "code": code,
                "description": str(e),
                "errorSource": "Catalyst Wrapper"
            })
            return HttpResponse(
                body=error_body,
                mimetype="application/json",
                status_code=code
            )

        custom_params = {
            k: parsed_params.pop(k)
            for k in schema.fields.keys()
            if k in parsed_params
        }
        if collection:
            custom_params['collection'] = collection

        headers = req.headers.__dict__.get('__http_headers__')
        data = func(
            query_params=parsed_params,
            headers=headers,
            **custom_params
        )

        descr = data.get('description')
        if data.get('errorSource') and isinstance(descr, str):
            fields = [x.replace('_', '-') for x in schema.fields if x != 'limit']
            attributes = ', '.join(fields)
            data['description'] = descr.format(attr=attributes)

        if LOG_REQUEST_DETAILS:
            custom_dimensions = data.pop('telemetryData', None)
            if custom_dimensions:
                track_event('OS NGD API - Features', custom_dimensions=custom_dimensions)

        json_data = json.dumps(data)
        return HttpResponse(
            body=json_data,
            mimetype="application/json"
        )
    except Exception as e:
        code = 500
        error_string = str(e)
        error_response = json.dumps({
            "code": code,
            "description": error_string,
            "errorSource": "Catalyst Wrapper"
        })
        return HttpResponse(
            body=error_response,
            mimetype="application/json",
            status_code=code
        )


@app.function_name('http_base')
@app.route("catalyst/features/{collection}/items")
def http_base(req: HttpRequest) -> HttpResponse:
    response = construct_response(
        req,
        BaseSchema,
        items
    )
    return response


@app.function_name('http_limit')
@app.route("catalyst/features/{collection}/items/limit")
def http_limit(req: HttpRequest) -> HttpResponse:
    response = construct_response(
        req,
        LimitSchema,
        items_limit
    )
    return response


@app.function_name('http_geom')
@app.route("catalyst/features/{collection}/items/geom")
def http_geom(req: HttpRequest) -> HttpResponse:
    response = construct_response(
        req,
        GeomSchema,
        items_geom
    )
    return response


@app.function_name('http_col')
@app.route("catalyst/features/multi-collection/items/col")
def http_col(req: HttpRequest) -> HttpResponse:
    response = construct_response(
        req,
        ColSchema,
        items_col
    )
    return response


@app.function_name('http_limit_geom')
@app.route("catalyst/features/{collection}/items/limit-geom")
def http_limit_geom(req: HttpRequest) -> HttpResponse:
    response = construct_response(
        req,
        LimitGeomSchema,
        items_limit_geom
    )
    return response


@app.function_name('http_limit_col')
@app.route("catalyst/features/multi-collection/items/limit-col")
def http_limit_col(req: HttpRequest) -> HttpResponse:
    response = construct_response(
        req,
        LimitColSchema,
        items_limit_col
    )
    return response


@app.function_name('http_geom_col')
@app.route("catalyst/features/multi-collection/items/geom-col")
def http_geom_col(req: HttpRequest) -> HttpResponse:
    response = construct_response(
        req,
        GeomColSchema,
        items_geom_col
    )
    return response


@app.function_name('http_limit_geom_col')
@app.route("catalyst/features/multi-collection/items/limit-geom-col")
def http_limit_geom_col(req: HttpRequest) -> HttpResponse:
    response = construct_response(
        req,
        LimitGeomColSchema,
        items_limit_geom_col
    )
    return response
