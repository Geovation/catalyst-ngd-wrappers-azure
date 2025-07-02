import json

import azure.functions as func

from azure.functions import HttpRequest, HttpResponse
from azure.monitor.events.extension import track_event
from azure.monitor.opentelemetry import configure_azure_monitor

from marshmallow.exceptions import ValidationError

from catalyst_ngd_wrappers.ngd_api_wrappers import get_latest_collection_versions, \
    get_specific_latest_collections, items, items_limit, items_geom, items_col, \
    items_limit_geom, items_limit_col, items_geom_col, items_limit_geom_col

from schemas import LatestCollectionsSchema, CatalystBaseSchema, LimitSchema, GeomSchema, \
    ColSchema, LimitGeomSchema, LimitColSchema, GeomColSchema, LimitGeomColSchema
from utils import remove_query_params, handle_error

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

configure_azure_monitor()


@app.function_name('http_latest_single_col')
@app.route("catalyst/features/latest-collections/{collection?}")
def http_latest_collections(req: HttpRequest) -> HttpResponse:
    '''Handles the processing of API requests to retrieve OS NGD collections, either all or a specific one.
    Handles parameter validation and telemetry tracking.'''

    try:
        if req.method != 'GET':
            return handle_error(
                description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
                code = 405
            )

        schema = LatestCollectionsSchema()
        params = {**req.params}
        fail_condition1 = len(params) > 1
        fail_condition2 = len(params) == 1 and not params.get('recent-update-days')
        if fail_condition1 or fail_condition2:
            return handle_error(
                code = 400,
                description = "The only supported query parameter is 'recent-update-days'.",
            )

        collection = req.route_params.get('collection')
        try:
            parsed_params = schema.load(params)
        except ValidationError as e:
            return handle_error(e)

        custom_dimensions = {
            f'query_params.{str(k)}': str(v)
            for k, v in parsed_params.items()
        }
        custom_dimensions.pop('key', None)
        url = remove_query_params(req.url)
        custom_dimensions.update({
            'method': 'GET',
            'url.path': url,
        })
        if collection:
            data = get_specific_latest_collections([collection], **parsed_params)
            custom_dimensions['url.path_params.collection'] = collection
        else:
            data = get_latest_collection_versions(**parsed_params)

        json_data = json.dumps(data)

        track_event('HTTP_Request', custom_dimensions=custom_dimensions)

        return HttpResponse(
            body=json_data,
            mimetype="application/json"
        )
    except Exception as e:
        handle_error(error = e, code = 500)


def get_request_data(req: HttpRequest) -> dict:
    '''
    Extracts the request data from the HttpRequest object.
    Returns a dictionary containing the request parameters and headers.
    '''
    method = req.method
    params = {**req.params}
    route_params = req.route_params
    headers = req.headers.__dict__.get('__http_headers__', {})
    return {
        'method': method,
        'params': params,
        'route_params': route_params,
        'headers': headers
    }


def construct_response(
    req: HttpRequest,
    schema_class: type,
    func_: callable
) -> HttpResponse:
    '''
    Translates the request headers and path and query parameters into a function call.
    Translates the function response into an HTTP response, handling errors and telemetry.
    '''

    try:
        data = get_request_data(req)
        # Handle incorrect HTTP methods
        if data['method'] != 'GET':
            return handle_error(
                description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
                code = 405
            )

        # Load the schema and parse the request parameters
        schema = schema_class()
        multi_collection = isinstance(schema, ColSchema)

        params = data['params']
        if multi_collection:
            col = params.get('collection')
            if col:
                params['collection'] = col.split(',')

        try:
            parsed_params = schema.load(params)
        except ValidationError as e:
            return handle_error(e)

        custom_params = {
            k: parsed_params.pop(k)
            for k in schema.fields.keys()
            if k  in parsed_params
        }
        if not multi_collection:
            custom_params['collection'] = data['route_params'].get('collection')

        data = func_(
            query_params=parsed_params,
            headers=data['headers'],
            **custom_params
        )

        descr = data.get('description')
        if data.get('errorSource') and isinstance(descr, str):
            fields = [
                x.replace('_', '-')
                for x in schema.fields
                if x != 'limit'
            ]
            attributes = ', '.join(fields)
            data['description'] = descr.format(attr=attributes)

        custom_dimensions = data.pop('telemetryData', None)
        if custom_dimensions:
            track_event('OS NGD API - Features', custom_dimensions=custom_dimensions)

        code = data.get('code', 200)
        json_data = json.dumps(data)
        return HttpResponse(
            body=json_data,
            mimetype="application/json",
            status_code=code
        )
    except Exception as e:
        handle_error(error = e, code = 500)


@app.function_name('http_base')
@app.route("catalyst/features/{collection}/items")
def http_base(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, No extensions applied.'''
    response = construct_response(
        req,
        CatalystBaseSchema,
        items
    )
    return response


@app.function_name('http_limit')
@app.route("catalyst/features/{collection}/items/limit")
def http_limit(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Limit'''
    response = construct_response(
        req,
        LimitSchema,
        items_limit
    )
    return response


@app.function_name('http_geom')
@app.route("catalyst/features/{collection}/items/geom")
def http_geom(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Geom'''
    response = construct_response(
        req,
        GeomSchema,
        items_geom
    )
    return response


@app.function_name('http_col')
@app.route("catalyst/features/multi-collection/items/col")
def http_col(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Col'''
    response = construct_response(
        req,
        ColSchema,
        items_col
    )
    return response


@app.function_name('http_limit_geom')
@app.route("catalyst/features/{collection}/items/limit-geom")
def http_limit_geom(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Limit
        - Geom'''
    response = construct_response(
        req,
        LimitGeomSchema,
        items_limit_geom
    )
    return response


@app.function_name('http_limit_col')
@app.route("catalyst/features/multi-collection/items/limit-col")
def http_limit_col(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Limit
        - Col'''
    response = construct_response(
        req,
        LimitColSchema,
        items_limit_col
    )
    return response


@app.function_name('http_geom_col')
@app.route("catalyst/features/multi-collection/items/geom-col")
def http_geom_col(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Geom
        - Col'''
    response = construct_response(
        req,
        GeomColSchema,
        items_geom_col
    )
    return response


@app.function_name('http_limit_geom_col')
@app.route("catalyst/features/multi-collection/items/limit-geom-col")
def http_limit_geom_col(req: HttpRequest) -> HttpResponse:
    '''Azure function, OS NGD API - Features, Exensions applied:
        - Limit
        - Geom
        - Col'''
    response = construct_response(
        req,
        LimitGeomColSchema,
        items_limit_geom_col
    )
    return response
