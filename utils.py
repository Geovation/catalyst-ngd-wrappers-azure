import json
from azure.functions import HttpResponse
from marshmallow.exceptions import ValidationError
from schemas import ColSchema
from function_app import AzureRequest

def remove_query_params(url: str) -> str:
    '''Removes query parameters from a URL.'''
    if '?' in url:
        return url.split('?')[0]
    return url


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

def construct_response(
    data: AzureRequest,
    schema_class: type,
    ngd_api_func: callable
) -> dict:
    '''
    Translates the request headers and path and query parameters into a function call.
    Translates the function response into an HTTP response, handling errors and telemetry.
    '''
    # Handle incorrect HTTP methods
    if data.method != 'GET':
        return handle_error(
            description = "The HTTP method requested is not supported. This endpoint only supports 'GET' requests.",
            code = 405
        )

    # Load the schema and parse the request parameters
    schema = schema_class()
    multi_collection = isinstance(schema, ColSchema)

    params = data.params
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
        custom_params['collection'] = data.route_params.get('collection')

    response_data = ngd_api_func(
        query_params=parsed_params,
        headers=data.headers,
        **custom_params
    )

    descr = response_data.get('description')
    if response_data.get('errorSource') and isinstance(descr, str):
        fields = [
            x.replace('_', '-')
            for x in schema.fields
            if x != 'limit'
        ]
        attributes = ', '.join(fields)
        response_data['description'] = descr.format(attr=attributes)

    #custom_dimensions = data.pop('telemetryData', None)
    #if custom_dimensions:
        #track_event('OS NGD API - Features', custom_dimensions=custom_dimensions)

    return response_data
