"""
Facilities to manage the download of a secure-connect-bundle from an Astra DB
token.
"""
import logging
import requests  # type: ignore
from typing import Optional


DEFAULT_GET_BUNDLE_URL_TEMPLATE = (
    "https://api.astra.datastax.com/v2/databases/{database_id}/secureBundleURL"
)


logger = logging.getLogger(__name__)


def get_astra_bundle_url(
    database_id: str, token: str, bundle_url_template: Optional[str] = None
) -> str:
    """
    Given a database ID and a token, provide a (temporarily-valid)
    URL for downloading the secure-connect-bundle.

    Courtesy of @phact (thank you!).
    """
    if not bundle_url_template:
        bundle_url_template = DEFAULT_GET_BUNDLE_URL_TEMPLATE
    url = bundle_url_template.format(database_id=database_id)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    logger.debug(f"Obtaining SCB URL from: {url}")
    response = requests.post(url, headers=headers)
    logger.debug(f"SCB URL response: {response.text}")

    response_json = response.json()
    # Print the response
    if response_json is not None:
        if "errors" in response_json:
            # Handle the errors
            errors = response_json["errors"]
            if any(
                "jwt not valid" in (err.get("message") or "").lower() for err in errors
            ):
                raise ValueError("Invalid or insufficient token.")
            elif any(
                "malformed" in (err.get("message") or "").lower() for err in errors
            ):
                raise ValueError("Invalid or insufficient token.")
            else:
                raise ValueError(
                    "Generic error when fetching the URL to the secure-bundle."
                )
        else:
            return response_json["downloadURL"]
    else:
        raise ValueError(
            "Cannot get the secure-bundle URL. "
            "Check the provided database_id and token."
        )


def download_astra_bundle_url(
    database_id: str,
    token: str,
    out_file_path: str,
    bundle_url_template: Optional[str] = None,
) -> None:
    """
    Obtain the secure-connect-bundle and save it to a specified file.
    """
    bundle_url = get_astra_bundle_url(
        database_id=database_id, token=token, bundle_url_template=bundle_url_template
    )
    logger.debug(f"Downloading SCB from: {bundle_url}")
    bundle_data = requests.get(bundle_url)
    with open(out_file_path, "wb") as f:
        f.write(bundle_data.content)
    return
