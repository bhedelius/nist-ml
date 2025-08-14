"""Scraper for NIST WebBook to collect chemical formula hrefs and mask hrefs."""

from dataclasses import dataclass, field
import asyncio
import re
import aiohttp
from logging import getLogger

logger = getLogger(__name__)

WEBBOOK_URL = "https://webbook.nist.gov"


@dataclass
class ChemicalMetadata:
    """Data class to hold chemical data."""

    href: str | None = None
    name: str | None = None
    formula: str | None = None
    weight: float | None = None
    inchi: str | None = None
    inchi_key: str | None = None
    cas: str | None = None
    jcamp_hrefs: list[str] = field(default_factory=list)


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
            logger.error(f"Failed to process {href}: {str(e)}")
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
                logger.info(info_msg)

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
                    logger.error(msg)
                    break

            return formula_hrefs

    async def _fetch_spectrum_href_by_label(
        self,
        session: aiohttp.ClientSession,
        href: str,
    ) -> str | None:
        """Fetch a detail page and return the href of the <a> tag with given label."""
        try:
            async with self.semaphore:
                html = await fetch_html(session, href)
                pattern = rf'<a href="([^"]+)">{re.escape("IR Spectrum")}</a>'
                match = re.search(pattern, html)
                if match:
                    return match.group(1)
        except Exception:
            logger.exception(f"Error fetching {href}")
        return None

    async def collect_spectrum_hrefs_by_label(
        self,
        hrefs: set[str],
    ) -> set[str]:
        """Collect mask hrefs for all given detail page hrefs with a specific label."""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for href in hrefs:
                tasks.append(self._fetch_spectrum_href_by_label(session, href))
            all_results = await asyncio.gather(*tasks)
            results = {r for r in all_results if r}
        return results

    async def collect_chemical_metadata(
        self,
        hrefs: set[str],
    ) -> list[ChemicalMetadata]:
        """Collect jcamp hrefs and metadata for all given detail page hrefs."""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for href in hrefs:
                tasks.append(self._fetch_chemical_metadata(session, href))
            all_results = await asyncio.gather(*tasks)
            results = [m for m in all_results if m]
        return results

    async def _fetch_chemical_metadata(
        self,
        session: aiohttp.ClientSession,
        href: str,
    ) -> ChemicalMetadata | None:
        """Fetch a detail page and return the jcamp href and metadata."""
        try:
            async with self.semaphore:
                chemical_metadata = ChemicalMetadata(href=href)
                html = await fetch_html(session, href)
                lines = html.splitlines()
                for i, line in enumerate(lines):
                    if line.startswith("<title>"):
                        chemical_metadata.name = line.removeprefix(
                            "<title>"
                        ).removesuffix("</title>")
                    elif line == ' title="IUPAC definition of empirical formula"':
                        chemical_metadata.formula = (
                            lines[i + 1].split("</strong> ")[1].removesuffix("</li>")
                        )
                    elif (
                        line
                        == ' title="IUPAC definition of relative molecular mass (molecular weight)"'
                    ):
                        chemical_metadata.weight = float(
                            lines[i + 1].split("</strong> ")[1].removesuffix("</li>")
                        )
                    elif line == "<strong>IUPAC Standard InChI:</strong>":
                        chemical_metadata.inchi = (
                            lines[i + 1]
                            .split('"inchi-text">')[1]
                            .removesuffix("</span>")
                        )
                    elif line == "<strong>IUPAC Standard InChIKey:</strong>":
                        chemical_metadata.inchi_key = (
                            lines[i + 1]
                            .split('"inchi-text">')[1]
                            .removesuffix("</span>")
                        )
                    elif "CAS Registry Number" in line:
                        chemical_metadata.cas = line.split("</strong> ")[
                            1
                        ].removesuffix("</li>")
                    elif "Index=" in line:
                        for quote in ("'", '"'):
                            if quote in line:
                                jcamp_href = line.split(quote)[1]
                                break
                        else:
                            continue

                        # Remove unnecessary parts and normalize
                        jcamp_href = jcamp_href.split("&amp;Large=on")[0]
                        for old, new in [
                            ("Spec=", "JCAMP="),
                            ("ID=", "JCAMP="),
                            ("&amp;Type=IR-SPEC", ""),
                            ("&amp;Type=IR", ""),
                            ("#IR-SPEC", ""),
                        ]:
                            jcamp_href = jcamp_href.replace(old, new)

                        # Ensure type is IR
                        jcamp_href += "&amp;Type=IR"
                        if jcamp_href not in chemical_metadata.jcamp_hrefs:
                            chemical_metadata.jcamp_hrefs.append(jcamp_href)
                return chemical_metadata

        except Exception as e:
            logger.error(f"Failed to parse {href}: {str(e)}")
        return None
