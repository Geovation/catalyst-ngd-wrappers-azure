import json
from azure.functions import HttpResponse


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
