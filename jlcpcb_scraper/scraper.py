import os
import logging
import time
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from fake_useragent import UserAgent
from typing import Generator


logger = logging.getLogger(__name__)


JLCPCB_KEY = os.environ.get("JLCPCB_KEY")
JLCPCB_SECRET = os.environ.get("JLCPCB_SECRET")


class JlcpcbScraper:
    def __init__(
        self,
        key: str | None = None,
        secret: str | None = None,
    ):
        # Session configuration
        self.session = requests.Session()
        ua = UserAgent()
        self.session.headers.update(
            {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate",
                "Host": "jlcpcb.com",
                "User-Agent": str(ua.chrome),
            }
        )
        self.session.mount("https://", HTTPAdapter(max_retries=3))

        # State info
        self.last_key = None

        # Token info
        self.token: str | None = None
        self.token_expires: datetime | None = None
        self.key = key or JLCPCB_KEY
        self.secret = secret or JLCPCB_SECRET
        self._obtain_token()

        # Wew!
        logger.info("JlcpcbScraper initialized")

    def _obtain_token(self) -> None:
        if not self.key or not self.secret:
            raise RuntimeError(
                "JLCPCB_KEY and JLCPCB_SECRET environment variables must be set"
            )
        body = {"appKey": self.key, "appSecret": self.secret}
        headers = {
            "Content-Type": "application/json",
        }
        resp = requests.post(
            "https://jlcpcb.com/external/genToken",
            json=body,
            headers=headers,
            timeout=30,
        )

        if resp.status_code != 200:
            raise RuntimeError(f"Cannot obtain token {resp.json()}")
        data = resp.json()
        if data["code"] != 200:
            raise RuntimeError(f"Cannot obtain token {data}")

        self.token = data["data"]
        self.session.headers.update(
            {
                "externalApiToken": self.token,
            }
        )
        self.token_expires = datetime.now() + timedelta(seconds=1800)

    def get_parts(self) -> Generator[dict, None, None]:
        request_count = 0
        while True:
            logger.info("Fetching page %s", request_count)
            request_count += 1
            response = self.session.post(
                "https://jlcpcb.com/external/component/getComponentInfos",
                data={"lastKey": self.last_key} if self.last_key else None,
            )
            if response.status_code != 200:
                logger.error("Cannot obtain parts, status code not 200: %s", response)
                return

            response_data: dict = response.json()
            if not response_data.get("code") == 200:
                logger.error(
                    "Cannot obtain parts, internal status code not 200: %s",
                    response_data,
                )
                return

            if not response_data.get("data", {}).get("componentInfos"):
                logger.info("No more parts to fetch")
                return

            self._parse_pagination(response_data)

            yield from response_data["data"]["componentInfos"]

            if self.token_expires < datetime.now():
                self._obtain_token()

            # Delay to avoid overwhelming the server
            time.sleep(0.2)

    def _parse_pagination(self, response):
        self.last_key = response.get("data", {}).get("lastKey", None)
        if not self.last_key:
            raise RuntimeError("Cannot obtain last key")
