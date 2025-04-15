"""Oxylabs Web Reader."""

import json
from typing import Any, List

from httpx import BasicAuth, AsyncClient, Timeout, Client, Response

from llama_index.core.readers.base import BasePydanticReader
from llama_index.core.schema import Document


class OxylabsWebReader(BasePydanticReader):
    """Scrape any website with Oxylabs Scraper.

    Oxylabs API documentation:
    https://developers.oxylabs.io/scraper-apis/web-scraper-api/other-websites

    Args:
        username: Oxylabs username.
        password: Oxylabs password.

    Example:
        .. code-block:: python
        from llama_index.readers.web import OxylabsWebReader


        reader = OxylabsWebReader(
            username="OXYLABS_USERNAME",
            password="OXYLABS_PASSWORD"
        )

        docs = reader.load_data({"url": "https://ip.oxylabs.io"})

        print(docs)
    """

    timeout_s: int = 100
    oxylabs_scraper_url: str = "https://realtime.oxylabs.io/v1/queries"
    auth: BasicAuth

    def __init__(self, username: str, password: str) -> None:
        auth = BasicAuth(username, password)
        super().__init__(auth=auth)

    @classmethod
    def class_name(cls) -> str:
        return "OxylabsWebReader"

    def _validate_payload(self, payload: dict[str, Any]) -> None:
        if "url" not in payload:
            raise ValueError("'url' must be provided!")

    def _get_document_from_response(self, response: Response) -> Document:
        content = response.json()

        if isinstance(content, (dict, list)):
            return Document(text=json.dumps(content))

        return Document(text=str(content))

    async def aload_data(self, payload: dict[str, Any]) -> List[Document]:
        self._validate_payload(payload)

        async with AsyncClient(
            timeout=Timeout(self.timeout_s),
            auth=self.auth,
        ) as client:
            response = await client.post(self.oxylabs_scraper_url, json=payload)

        return [self._get_document_from_response(response)]

    def load_data(self, payload: dict[str, Any]) -> List[Document]:
        self._validate_payload(payload)

        with Client(
            timeout=Timeout(self.timeout_s),
            auth=self.auth,
        ) as client:
            response = client.post(self.oxylabs_scraper_url, json=payload)

        return [self._get_document_from_response(response)]
