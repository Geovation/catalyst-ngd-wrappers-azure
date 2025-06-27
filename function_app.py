import json

import azure.functions as func

from azure.functions import HttpRequest, HttpResponse
from azure.monitor.events.extension import track_event
from azure.monitor.opentelemetry import configure_azure_monitor

from marshmallow.exceptions import ValidationError
from schemas import LatestCollectionsSchema, BaseSchema, LimitSchema, GeomSchema, \
    ColSchema, LimitGeomSchema, LimitColSchema, GeomColSchema, LimitGeomColSchema

from catalyst_ngd_wrappers.ngd_api_wrappers import get_latest_collection_versions, \
    get_specific_latest_collections, items, items_limit, items_geom, \
    items_col, items_limit_geom, items_limit_col, items_geom_col, \
    items_limit_geom_col


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

configure_azure_monitor()

def handle_error(
    error: Exception = None,
    description: str = None,
    code: int = 400
) -> HttpResponse:
    """Formats and configures errors, returning a JSON response."""
    assert error or description, "Either error or description must be provided."
    if not description:
        description = str(error)
    error_body = json.dumps({
        "code": code,
        "description": description,
        "errorSource": "Catalyst Wrapper"
    })
    return HttpResponse(
        body = error_body,
        mimetype = "application/json",
        status_code = code
    )


@app.function_name('http_latest_collections')
@app.route("catalyst/features/latest-collections")
def http_latest_collections(req: HttpRequest) -> HttpResponse:

    if req.method != 'GET':
        return handle_error(
            description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
            code = 405
        )

    schema = LatestCollectionsSchema()

    params = {**req.params}

    try:
        parsed_params = schema.load(params)
    except ValidationError as e:
        return handle_error(e)

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
        return handle_error(
            description="The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
            code=405
        )

    schema = LatestCollectionsSchema()
    collection = req.route_params.get('collection')

    params = {**req.params}
    try:
        parsed_params = schema.load(params)
    except ValidationError as e:
        return handle_error(e)

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
    '''Converts list parameters in the params dictionary to single values.'''
    for k, v in params.items():
        if k != 'collection':
            params[k] = v[0]


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
        # Handle incorrect HTTP methods
        if req.method != 'GET':
            return handle_error(
                description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
                code = 405
            )

        # Load the schema and parse the request parameters
        schema = schema_class()
        multi_collection = isinstance(schema, ColSchema)

        params = {**req.params}
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
            if k in parsed_params
        }
        if not multi_collection:
            custom_params['collection'] = req.route_params.get('collection')

        headers = req.headers.__dict__.get('__http_headers__')
        data = func_(
            query_params=parsed_params,
            headers=headers,
            **custom_params
        )

        descr = data.get('description')
        if data.get('errorSource') and isinstance(descr, str):
            fields = [x.replace('_', '-') for x in schema.fields if x != 'limit']
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
