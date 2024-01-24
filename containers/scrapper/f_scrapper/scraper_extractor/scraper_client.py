import requests
import copy

from bs4 import BeautifulSoup

from f_scrapper.processes.scrape_schemas import ScrapeConfig
from f_scrapper.processes.scrape_schemas import HTMLElem


class ScraperClient:
    def __init__(
        self,
        base_url: str,
        headers: dict[str, str] = None,
    ):
        self.base_url = base_url
        self.headers = headers

    def scrape_content(
        self, scrape_cfg: ScrapeConfig, override_url_path: str = None
    ) -> list:
        scrape_elems = scrape_cfg.scrape_elems

        url_path = override_url_path if override_url_path else scrape_cfg.url_path

        content = self._get_content(url_path)
        soup = BeautifulSoup(content, "html.parser")

        rows = ScraperClient.format_content(
            copy.deepcopy(soup),
            scrape_elems.row_search_elems,
            scrape_elems.row_extract_func,
        )

        return rows

    @staticmethod
    def format_content(
        soup: BeautifulSoup, elems: list[HTMLElem], extract_func: callable
    ):
        for search_elem in elems:
            extra_props = ScraperClient.extra_props(search_elem)
            if search_elem.is_find_all:
                soup = soup.find_all(
                    search_elem.type,
                    **extra_props,
                )
                if search_elem.elem_position:
                    soup = soup[search_elem.elem_position]
            else:
                soup = soup.find(
                    search_elem.type,
                    **extra_props,
                )
        return extract_func(soup)

    @staticmethod
    def extra_props(search_elem: HTMLElem):
        if search_elem.property_key:
            return {search_elem.property_key: search_elem.property_value}
        else:
            return {}

    def _get_content(self, path):
        full_url = self._get_full_url(path)
        response = requests.get(full_url, headers=self.headers)
        response.raise_for_status()
        return response.content

    def _get_full_url(self, path):
        return f"{self.base_url}{path}"
