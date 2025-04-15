import json
from typing import Any, List

from httpx import AsyncClient, Client, Timeout, BasicAuth, Response
from llama_index.core.readers.base import BasePydanticReader
from llama_index.core.schema import Document


class OxylabsReader(BasePydanticReader):
    """Get the data from different sources with Oxylabs Scraper.

    Oxylabs API documentation:
    https://developers.oxylabs.io/scraper-apis/web-scraper-api

    Args:
        username: Oxylabs username.
        password: Oxylabs password.

    Example:
        .. code-block:: python
        from llama_index.readers.oxylabs import OxylabsReader


        reader = OxylabsReader(
            username="OXYLABS_USERNAME",
            password="OXYLABS_PASSWORD"
        )

        docs = reader.load_data(
            {
                "source": "google_search",
                "parse": True,
                "query": "Iphone 16",
                "geo_location": "Paris, France"
            }
        )

        print(docs)
    """

    timeout_s: int = 100
    oxylabs_scraper_url: str = "https://realtime.oxylabs.io/v1/queries"
    auth: BasicAuth

    def __init__(self, username: str, password: str) -> None:
        auth = BasicAuth(username, password)
        super().__init__(auth=auth)

    def _get_document_from_response(self, response: Response) -> Document:
        content = response.json()

        if isinstance(content, (dict, list)):
            return Document(text=json.dumps(content))

        return Document(text=str(content))

    async def aload_data(self, payload: dict[str, Any]) -> List[Document]:
        async with AsyncClient(
            timeout=Timeout(self.timeout_s),
            auth=self.auth,
        ) as client:
            response = await client.post(self.oxylabs_scraper_url, json=payload)

        return [self._get_document_from_response(response)]

    def load_data(self, payload: dict[str, Any]) -> List[Document]:
        with Client(
            timeout=Timeout(self.timeout_s),
            auth=self.auth,
        ) as client:
            response = client.post(self.oxylabs_scraper_url, json=payload)

        return [self._get_document_from_response(response)]
