import requests
import pathlib
from bs4 import BeautifulSoup
from tqdm import tqdm

import polars as pl
import pandas as pd


class BeautifulSoupHelpers:
    """Helper function to the CapitolTraderScraper"""
    @staticmethod
    def get_table_headers(table) -> list[str]:
        """Scrape the header of the table."""
        headers = []
        for header in table.find_all('th'):
            headers.append(header.text.strip())
        return headers

    @staticmethod
    def get_html_soup(url: str) -> BeautifulSoup:
        """Returns soup of the given url."""
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup


class DataFrameCleaner:
    """Class to clean the dataframe with CapitolTraderScraper"""
    @staticmethod
    def parse_size(size_str: str, minimum_val: bool) -> int | float:
        """
        Function to convert size strings to numeric values. This includes converting strins like '1M–5M' to either
        1_000_000 or 5_000_000 depending if minimum_val is True or False.

        :param size_str: The string to be converted
        :param minimum_val: True, if the size string should be converted to the lower interval. False, if string should
        be converted to the upper interval
        :return:
        """
        def to_number(s):
            if 'K' in s:
                return float(s.replace('K', '')) * 1_000
            elif 'M' in s:
                return float(s.replace('M', '')) * 1_000_000
            return float(s)

        min_size, max_size = size_str.split('–')
        if minimum_val:
            return to_number(min_size)
        else:
            return to_number(max_size)

    def clean_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean the data from table in https://www.capitoltrades.com/trades

        :param df: polars dataframe
        :return: Cleaned polars dataframe
        """
        # Convert date columns to datetime
        df = df.with_columns([
            pl.col('Published').str.to_datetime('%d %b%Y'),
            pl.col('Traded').str.to_datetime('%d %b%Y')
        ])

        # Other type conversions
        df = df.with_columns([
            pl.col('Filed after').str.replace('days', '').str.to_integer(),
            pl.col('Size').apply(self.parse_size, True).alias('Size_min'),
            pl.col('Size').apply(self.parse_size, False).alias('Size_max')
        ])

        return df


class CapitolTraderScraper:
    def __init__(self):
        self.url = "https://www.capitoltrades.com/trades"

        self.bs_helpers = BeautifulSoupHelpers()

    @property
    def number_of_pages(self) -> int | None:
        """Returns total number of pages with data on url."""
        soup = self.bs_helpers.get_html_soup(self.url)

        total_pages = None
        page_info = soup.find('p', class_='hidden leading-7 sm:block')
        if page_info:
            bold_tags = page_info.find_all('b')
            if len(bold_tags) > 1:
                total_pages = bold_tags[1].text.strip()
                print(f"Total pages found in visible text: {total_pages}")

        return int(total_pages)

    def scrape_table(self, soup: BeautifulSoup, headers: list[str] = None) -> tuple[list[list], list[str]]:
        """Scrape the data in a single table"""
        table = soup.find('table', {'class': 'q-table trades-table'})

        if headers is None:
            headers = self.bs_helpers.get_table_headers(table)[:-1]

        rows = []
        if table:
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) > 0:
                    row = [cell.text.strip() for cell in cells]
                    rows.append(row[:-1])

        return rows, headers

    def scrape_trades(self) -> pl.DataFrame:
        """
        Scrapes data from all trades.

        :return: Polars dataframe with trades data.
        """
        total_pages = self.number_of_pages

        all_rows = []
        headers = None
        for page in tqdm(range(1, total_pages + 1)):
            url = f'https://www.capitoltrades.com/trades?page={page}'
            soup = self.bs_helpers.get_html_soup(url)
            page_rows, headers = self.scrape_table(soup, headers)

            if page_rows:
                if len(page_rows[0]) != len(headers):
                    pass
                all_rows.extend(page_rows)

        df = pl.DataFrame(all_rows, schema=headers, orient='row')

        return df


if __name__ == '__main__':
    scraper = CapitolTraderScraper()
    cleaner = DataFrameCleaner()
    print(scraper.number_of_pages)

    df_raw = scraper.scrape_trades()
    path = pathlib.Path(r'data/capitol_trader/capitol_trader_scraped_data.csv')

    # converts df_raw to pandas instead of keeping df in polars to rush completion of code
    # df_raw.write_csv(path, separator=';')
    df_pd = df_raw.to_pandas()
    df_pd.to_csv(path, index=False, sep=';')

    # df_clean = cleaner.clean_dataframe(df_raw)



