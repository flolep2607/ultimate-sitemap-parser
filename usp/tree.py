"""Helpers to generate a sitemap tree."""

import logging
from collections import deque
from collections.abc import Iterator

from .exceptions import SitemapException
from .fetch_parse import SitemapFetcher, SitemapStrParser
from .helpers import (
    RecurseCallbackType,
    RecurseListCallbackType,
    is_http_url,
    strip_url_to_homepage,
)
from .objects.page import SitemapPage
from .objects.sitemap import (
    AbstractSitemap,
    IndexRobotsTxtSitemap,
    IndexWebsiteSitemap,
    InvalidSitemap,
)
from .web_client.abstract_client import AbstractWebClient
from .web_client.requests_client import RequestsWebClient

log = logging.getLogger(__name__)

_UNPUBLISHED_SITEMAP_PATHS = {
    "sitemap.xml",
    "sitemap.xml.gz",
    "sitemap_index.xml",
    "sitemap-index.xml",
    "sitemap_index.xml.gz",
    "sitemap-index.xml.gz",
    ".sitemap.xml",
    "sitemap",
    "admin/config/search/xmlsitemap",
    "sitemap/sitemap-index.xml",
    "sitemap_news.xml",
    "sitemap-news.xml",
    "sitemap_news.xml.gz",
    "sitemap-news.xml.gz",
}
"""Paths which are not exposed in robots.txt but might still contain a sitemap."""


def sitemap_tree_for_homepage(
    homepage_url: str,
    web_client: AbstractWebClient | None = None,
    use_robots: bool = True,
    use_known_paths: bool = True,
    extra_known_paths: set | None = None,
    recurse_callback: RecurseCallbackType | None = None,
    recurse_list_callback: RecurseListCallbackType | None = None,
    normalize_homepage_url: bool = True,
) -> AbstractSitemap:
    """
    Using a homepage URL, fetch the tree of sitemaps and pages listed in them.

    :param homepage_url: Homepage URL of a website to fetch the sitemap tree for, e.g. "http://www.example.com/".
    :param web_client: Custom web client implementation to use when fetching sitemaps.
        If ``None``, a :class:`~.RequestsWebClient` will be used.
    :param use_robots: Whether to discover sitemaps through robots.txt.
    :param use_known_paths: Whether to discover sitemaps through common known paths.
    :param extra_known_paths: Extra paths to check for sitemaps.
    :param recurse_callback: Optional callback function to determine if a sub-sitemap should be recursed into. See :data:`~.RecurseCallbackType`.
    :param recurse_list_callback: Optional callback function to filter the list of sub-sitemaps to recurse into. See :data:`~.RecurseListCallbackType`.
    :param normalize_homepage_url: Whether to normalize the provided homepage URL to the domain root (default: True),
        e.g. "http://www.example.com/xxx/yyy/" -> "http://www.example.com/".
        Disabling this may prevent sitemap discovery via robots.txt, as robots.txt is typically only available at the domain root.

    :return: Root sitemap object of the fetched sitemap tree.
    """

    if not is_http_url(homepage_url):
        raise SitemapException(f"URL {homepage_url} is not a HTTP(s) URL.")

    extra_known_paths = extra_known_paths or set()

    if normalize_homepage_url:
        stripped_homepage_url = strip_url_to_homepage(url=homepage_url)
        if homepage_url != stripped_homepage_url:
            log.warning(
                f"Assuming that the homepage of {homepage_url} is {stripped_homepage_url}"
            )
            homepage_url = stripped_homepage_url

    if not homepage_url.endswith("/"):
        homepage_url += "/"
    robots_txt_url = homepage_url + "robots.txt"

    sitemaps = []

    sitemap_urls_found_in_robots_txt = set()
    if use_robots:
        robots_txt_fetcher = SitemapFetcher(
            url=robots_txt_url,
            web_client=web_client,
            recursion_level=0,
            parent_urls=set(),
            recurse_callback=recurse_callback,
            recurse_list_callback=recurse_list_callback,
        )
        robots_txt_sitemap = robots_txt_fetcher.sitemap()
        if not isinstance(robots_txt_sitemap, InvalidSitemap):
            sitemaps.append(robots_txt_sitemap)

        if isinstance(robots_txt_sitemap, IndexRobotsTxtSitemap):
            for sub_sitemap in robots_txt_sitemap.all_sitemaps():
                sitemap_urls_found_in_robots_txt.add(sub_sitemap.url)

    if use_known_paths:
        for unpublished_sitemap_path in _UNPUBLISHED_SITEMAP_PATHS | extra_known_paths:
            unpublished_sitemap_url = homepage_url + unpublished_sitemap_path

            # Don't refetch URLs already found in robots.txt
            if unpublished_sitemap_url not in sitemap_urls_found_in_robots_txt:
                unpublished_sitemap_fetcher = SitemapFetcher(
                    url=unpublished_sitemap_url,
                    web_client=web_client,
                    recursion_level=0,
                    parent_urls=sitemap_urls_found_in_robots_txt,
                    quiet_404=True,
                    recurse_callback=recurse_callback,
                    recurse_list_callback=recurse_list_callback,
                )
                unpublished_sitemap = unpublished_sitemap_fetcher.sitemap()

                # Skip the ones that weren't found
                if not isinstance(unpublished_sitemap, InvalidSitemap):
                    sitemaps.append(unpublished_sitemap)

    index_sitemap = IndexWebsiteSitemap(url=homepage_url, sub_sitemaps=sitemaps)

    return index_sitemap


def stream_pages(
    homepage_url: str,
    web_client: AbstractWebClient | None = None,
) -> Iterator[SitemapPage]:
    """
    Yield all sitemap pages for a website one sub-sitemap at a time.

    Uses a BFS queue so that nested index sitemaps are handled without
    recursion and without holding more than one parsed sitemap in memory.
    Already-seen URLs are skipped to avoid cycles.

    :param homepage_url: Homepage URL, e.g. ``"https://www.example.com"``.
    :param web_client: Custom web client; defaults to :class:`~.RequestsWebClient`.
    :return: Iterator of :class:`~.SitemapPage` objects.
    """
    if web_client is None:
        web_client = RequestsWebClient()

    seen_urls: set[str] = set()
    queue: deque[str] = deque()

    def _collect(urls, recursion_level, parent_urls):
        for url in urls:
            if url not in seen_urls:
                seen_urls.add(url)
                queue.append(url)
        return []

    # Discover top-level sub-sitemap URLs via robots.txt and known paths
    sitemap_tree_for_homepage(
        homepage_url,
        web_client=web_client,
        recurse_list_callback=_collect,
    )

    while queue:
        url = queue.popleft()
        sitemap = SitemapFetcher(
            url=url,
            web_client=web_client,
            recursion_level=1,
            parent_urls=set(),
            recurse_list_callback=_collect,
        ).sitemap()
        if not isinstance(sitemap, InvalidSitemap):
            yield from sitemap.all_pages()
        del sitemap


def sitemap_from_str(content: str) -> AbstractSitemap:
    """Parse sitemap from a string.

    Will return the parsed sitemaps, and any sub-sitemaps will be returned as :class:`~.InvalidSitemap`.

    :param content: Sitemap string to parse
    :return: Parsed sitemap
    """
    fetcher = SitemapStrParser(static_content=content)
    return fetcher.sitemap()
