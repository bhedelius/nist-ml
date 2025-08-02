import asyncio
import re
import aiohttp
from logging import getLogger

log = getLogger(__name__)

WEBBOOK_URL = "https://webbook.nist.gov"


async def fetch_html(
    session: aiohttp.ClientSession,
    href: str,
) -> str:
    """Asynchronously fetch HTML content from a URL."""
    url = f"{WEBBOOK_URL}{href}"
    async with session.get(
        url, timeout=10
    ) as response:  # Set a timeout for the request
        response.raise_for_status()  # Raises HTTPError for bad responses
        return await response.text()


class NISTScraper:
    def __init__(self, max_concurrent_requests: int = 40):
        self.max_concurrent_requests = max_concurrent_requests
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)

    async def _fetch_index_hrefs(
        self,
        session: aiohttp.ClientSession,
        href: str,
    ) -> tuple[set[str], set[str], set[str]]:
        """Extract formula and group hrefs from a given page."""
        try:
            async with self.semaphore:
                html = await fetch_html(session, href)
                formula_hrefs: set[str] = set()
                group_hrefs: set[str] = set()
                for line in html.split("\n"):
                    if line.startswith('<li><a href="/cgi/'):
                        sub_href = line.split('"')[1]
                        if "species" in line:
                            group_hrefs.add(sub_href)
                        else:
                            formula_hrefs.add(sub_href)
                return formula_hrefs, group_hrefs, set()
        except Exception as e:
            log.error(f"Failed to process {href}: {str(e)}")
            return set(), set(), {href}

    async def collect_all_formula_hrefs(
        self,
        start_href: str = "/cgi/formula/",
    ) -> set[str]:
        """Collect all formula hrefs starting from the given href."""
        async with aiohttp.ClientSession() as session:
            (
                formula_hrefs,
                groups_to_search,
                failed_hrefs,
            ) = await self._fetch_index_hrefs(session, start_href)

            iteration = 0
            seen_groups: set[str] = set()

            while groups_to_search:
                iteration += 1
                info_msg = f"Iteration {iteration}: {len(formula_hrefs)} entries, {len(groups_to_search)} groups remaining, {len(failed_hrefs)} failed on last attempt"
                log.info(info_msg)

                # Create tasks for all groups to search
                tasks = [
                    self._fetch_index_hrefs(session, group)
                    for group in groups_to_search
                ]

                # Execute all tasks concurrently and gather results
                outputs = await asyncio.gather(*tasks)

                # Process the results
                new_formula_hrefs = set().union(*(e for e, _, _ in outputs))
                new_groups_to_search = set().union(*(g for _, g, _ in outputs))
                failed_hrefs = set().union(*(f for _, _, f in outputs))

                formula_hrefs.update(new_formula_hrefs)

                seen_groups.update(groups_to_search - failed_hrefs)
                groups_to_search = new_groups_to_search | failed_hrefs - seen_groups

                if iteration > 100:
                    msg = (
                        f"Unable to fetch formula hrefs for groups: {groups_to_search}"
                    )
                    log.error(msg)
                    break

            return formula_hrefs

    async def _fetch_mask_href_by_label(
        self,
        session: aiohttp.ClientSession,
        href: str,
        label: str,
    ) -> str | None:
        """Fetch a detail page and return the href of the <a> tag with given label."""
        try:
            async with self.semaphore:
                html = await fetch_html(session, href)
                pattern = rf'<a href="([^"]+)">{re.escape(label)}</a>'
                match = re.search(pattern, html)
                if match:
                    return match.group(1)
        except Exception as e:
            print(f"Error fetching {href}: {type(e).__name__}: {e}")
        return None

    async def collect_mask_hrefs_by_label(
        self,
        hrefs: set[str],
        label: str,
    ) -> set[str]:
        """Collect mask hrefs for all given detail page hrefs with a specific label."""
        results: set[str] = set()

        async with aiohttp.ClientSession() as session:
            tasks = []
            for href in hrefs:
                tasks.append(self._fetch_mask_href_by_label(session, href, label))
            all_results = await asyncio.gather(*tasks)
            results = {r for r in all_results if r}
        return results
