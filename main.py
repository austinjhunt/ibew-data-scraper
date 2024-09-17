import requests
import pandas as pd
import logging
import re
from bs4 import BeautifulSoup
import time
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

REQUESTS_TIMEOUT = 10  # seconds


class IBEWDataScraper:
    def __init__(self, states=None):
        if states is None:
            states = ["NY", "CT", "RI", "MA", "VT", "NH", "ME"]
        self.states_to_query = states

        logger.info(
            {
                "action": "UnionDataScraper.__init__",
                "states_to_query": self.states_to_query,
                "message": (
                    f"Initialized IBEWDataScraper; "
                    f"collecting IBEW data for states: "
                    f"{', '.join(self.states_to_query)}"
                ),
            }
        )

    def query_ibew_union_directory_by_state(self, state):
        """
        Queries the IBEW API by state and returns the local unions in that state.
        Example URL: https://ibew.org/ludSearch/DataIO.ashx?action=list-locals-by-state&state=NC&filter=all

        Example response:

        [
            {
                "ID": "65",
                "LU": "80",
                "CharterCity": "---",
                "State": "NC",
                "VP_District": "4"
            },
            ...
            ]
        """
        url = f"https://ibew.org/ludSearch/DataIO.ashx?action=list-locals-by-state&state={state}&filter=all"
        logger.info({"action": "UnionDataScraper.query_by_state", "state": state, "url": url})
        try:
            data = self._fetch_data(url, response_format="json")
            return data
        except Exception as e:
            logger.error({"status": "exception", "message": str(e)})
            return []

    def query_union_directory_by_multiple_states(self, states: list = []):
        """
        Queries the IBEW API for multiple states and returns a full list of local unions for all specified states.

        Args:
            states (list): List of states to query.

        Returns:
            list: List of local unions.
        """
        logger.info(
            {
                "action": "UnionDataScraper.query_union_directory_by_multiple_states",
                "states": states,
            }
        )
        local_unions = []
        for state in states:
            local_unions += self.query_ibew_union_directory_by_state(state)
        supplemented = self._add_supplemental_data_to_unions_list(local_unions)
        return supplemented

    def convert_unions_list_to_dataframe(self, local_unions: list = []):
        """
        Converts a list of local unions to a pandas DataFrame.

        Args:
            local_unions (list): List of local unions.

        Returns:
            pd.DataFrame: DataFrame containing local unions.
        """
        logger.info(
            {
                "action": "UnionDataScraper.convert_unions_list_to_dataframe",
                "local_unions_count": len(local_unions),
            }
        )
        self.local_unions = pd.DataFrame(local_unions)
        logger.info(
            {
                "status": "success",
                "local_unions_shape": self.local_unions.shape,
                "local_unions_fields": self.local_unions.columns.tolist(),
            }
        )
        return self.local_unions

    def _get_union_classifications_by_local_union_id(self, local_union_id: str):
        """
        Queries the UnionFacts API for classifications by local union ID.
        Example URL: https://ibew.org/ludSearch/DataIO.ashx?action=list-local-trade-classes&LocalUnionID=148&_=1726607920330

        Example Response:
        [
            {
                "TradeClass": "Inside (i)"
            }
        ]

        Args:
            local_union_id (str): The local union ID.

        Returns:
            string: Comma-separated string of trade classes.
        """
        url = f"https://ibew.org/ludSearch/DataIO.ashx?action=list-local-trade-classes&LocalUnionID={local_union_id}"
        logger.info(
            {
                "action": "UnionDataScraper.get_union_classifications_by_local_union_id",
                "local_union_id": local_union_id,
                "url": url,
            }
        )
        try:
            data = self._fetch_data(url, response_format="json")
            return ",".join([el["TradeClass"] for el in data])
        except Exception as e:
            logger.error({"status": "exception", "message": str(e)})
            return []

    def _fetch_data(self, url, response_format="json"):
        """
        Fetch data from the given URL.
        """
        logger.info({"action": "UnionDataScraper._fetch_data", "url": url})
        response = requests.get(url, timeout=REQUESTS_TIMEOUT)
        if response.status_code == 200:
            logger.info(
                {
                    "status": "success",
                    "url": url,
                }
            )
            if response_format == "json":
                return response.json()
            elif response_format == "html":
                return BeautifulSoup(response.content, "html.parser")
        else:
            logger.error({"status": "failure", "url": url, "response": response.status_code})
            return []

    def _add_supplemental_data_to_unions_list(self, unions: list = []):
        """
        Adds classifications and counties to the unions list by querying the UnionFacts API for each local union ID using multithreading.
        """

        def add_data_to_union(union):
            local_union_id = union.get("ID")
            union["Classifications"] = self._get_union_classifications_by_local_union_id(
                local_union_id
            )
            union["Counties"] = self._get_counties_by_local_union_id(local_union_id)
            return union

        try:
            logger.info(
                {
                    "action": "UnionDataScraper.add_classifications_to_unions_list",
                    "unions_count": len(unions),
                }
            )
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(add_data_to_union, union) for union in unions]
                results = [future.result() for future in futures]

            logger.info(
                {
                    "status": "success",
                }
            )
            return results
        except Exception as e:
            logger.error({"status": "exception", "message": str(e)})
            return unions

    def _get_counties_by_local_union_id(self, local_union_id: str):
        """
        Queries the UnionFacts API for counties by local union ID.
        Example URL: https://ibew.org/ludSearch/DataIO.ashx?action=list-local-counties&lu=237
        Example Response:
        [
            {
                "CountyName": "Niagara County, NY",
                "District": "3",
                "Population": "219846",
                "LandArea": "522.95",
                "Percentage": "100%",
                "jurisdiction": "I",
                "StateProvince": "NY"
            },
            {
                "CountyName": "Orleans County, NY",
                "District": "3",
                "Population": "44171",
                "LandArea": "391.40",
                "Percentage": "75%",
                "jurisdiction": "I",
                "StateProvince": "NY"
            }
        ]

        Args:
            local_union_id (str): The local union ID.

        Returns:
            list of county jurisdiction objects
        """
        url = (
            f"https://ibew.org/ludSearch/DataIO.ashx?action=list-local-counties&lu={local_union_id}"
        )
        logger.info(
            {
                "action": "UnionDataScraper.get_counties_by_local_union_id",
                "local_union_id": local_union_id,
                "url": url,
            }
        )
        try:
            return self._fetch_data(url, response_format="json")
        except Exception as e:
            logger.error({"status": "exception", "message": str(e)})
            return []

    def get_ibew_locals_directory_from_union_facts_as_dataframe(self):
        """
        UnionFacts does not offer a publicly accessible API. Need to consume and parse an HTML response into usable data.

        The URL to scrape is https://unionfacts.com/locals/International_Brotherhood_of_Electrical_Workers

        Response contains an HTML table of local unions with columns: Union, Unit Name, Location, Members

        Members is a count that we want to collect.

        """
        url = "https://unionfacts.com/locals/International_Brotherhood_of_Electrical_Workers"
        logger.info(
            {
                "action": "UnionDataScraper.get_ibew_locals_directory_from_union_facts_as_dataframe",
                "url": url,
            }
        )
        fields = ["Union", "Unit Name", "Location", "Members"]
        try:
            html = self._fetch_data(url, response_format="html")
            table = html.select("div.tab-content table")
            # get the first table
            if not table:
                logger.warning("No table found in the HTML response.")
                return pd.DataFrame(columns=fields)

            table = table[0]  # Get the first table
            # Extract table data
            data = []

            def process_row(row):
                cols = row.find_all("td")
                union_name = cols[0].text.strip()
                unit_name = cols[1].text.strip()
                location = cols[2].text.strip()
                members = int(cols[3].text.strip().replace(",", ""))
                lu_id = re.search(r"Local (\d+)", union_name)
                if lu_id:
                    lu_id = lu_id.group(1)
                    href_value = cols[0].find("a")["href"]
                    return {
                        "Union": union_name,
                        "Unit Name": unit_name,
                        "Location": location,
                        "Members": members,
                        "LU": lu_id,
                        "URL": f'https://unionfacts.com{href_value}',
                    }
                return None

            rows = table.find("tbody").find_all("tr")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(process_row, row) for row in rows]
                results = [future.result() for future in futures]

            data = [result for result in results if result is not None]
            return pd.DataFrame(data)

        except Exception as e:
            logger.error({"status": "exception", "message": str(e)})
            return pd.DataFrame()

    def merge_unionfacts_with_ibew_on_lu(
        self, unionfacts_directory_df: pd.DataFrame, ibew_df: pd.DataFrame
    ):
        """
        Merges the UnionFacts DataFrame with the IBEW DataFrame on the LU column.
        """
        merged_df = pd.merge(unionfacts_directory_df, ibew_df, on="LU", how="inner")
        logger.info(
            {
                "action": "UnionDataScraper.merge_unionfacts_with_ibew_on_lu",
                "merged_shape": merged_df.shape,
            }
        )
        return merged_df

    def save_df_as_excel(self, df, output_filepath: str = ""):
        """
        Saves the DataFrame as an Excel file.
        """
        logger.info(
            {
                "action": "UnionDataScraper.save_df_as_excel",
                "output_filepath": output_filepath,
            }
        )
        try:
            df.to_excel(output_filepath, index=False)
        except Exception as e:
            logger.error({"status": "exception", "message": str(e)})

    def _flatten_counties(self, unions_df: pd.DataFrame):
        """
        Flatten the Counties column in the unions DataFrame, creating a separate row for each county.

        Args:

            unions_df (pd.DataFrame): The DataFrame containing unions with a Counties column.

        Returns:

            pd.DataFrame: The flattened DataFrame.
        """
        # Ensure that 'Counties' column is filled with lists
        unions_df["Counties"] = unions_df["Counties"].apply(
            lambda x: x if isinstance(x, list) else []
        )

        # Explode the Counties list into multiple rows
        counties_expanded = unions_df.explode("Counties").reset_index(drop=True)

        # Normalize the Counties JSON data into individual columns
        counties_normalized = pd.json_normalize(counties_expanded["Counties"]).add_prefix("County_")

        # Merge the normalized counties back to the main DataFrame
        result_df = pd.concat(
            [counties_expanded.drop(columns=["Counties"]), counties_normalized], axis=1
        )

        return result_df

    def _one_hot_encode_column(self, df, column_name):
        """
        One-hot encodes a specified column in the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to encode.
            column_name (str): The name of the column to encode.

        Returns:
            pd.DataFrame: The DataFrame with the one-hot encoded column.
        """
        logger.info(
            {
                "action": "UnionDataScraper._one_hot_encode_column",
            }
        )
        one_hot = pd.get_dummies(df[column_name], prefix=column_name)
        # remove the original column
        df = df.drop(column_name, axis=1)
        return df.join(one_hot)

    def _cleanup_data(self, df):
        """
        Cleans up the DataFrame by removing duplicates and NaN values.

        Args:

            df (pd.DataFrame): The DataFrame to clean.

        Returns:

                pd.DataFrame: The cleaned DataFrame.
        """
        logger.info({"action": "UnionDataScraper._cleanup_data", "initial_shape": df.shape})

        # One-hot encode the 'Classifications' column
        df = self._one_hot_encode_column(df, "Classifications")
        logger.info({"status": "success", "final_shape": df.shape})

        # flatten counties - we do not want JSON lists in a cell
        df = self._flatten_counties(df)

        df = df.drop_duplicates()
        df = df.dropna()

        return df

    def run(self, output_file: str = "merged_union_data.xlsx"):
        """
        Main method to run the scraper.
        """
        logger.info({"action": "UnionDataScraper.run"})
        start_time = time.time()
        unionfacts_locals_directory_df = (
            self.get_ibew_locals_directory_from_union_facts_as_dataframe()
        )
        local_unions = self.query_union_directory_by_multiple_states(self.states_to_query)
        df_local_unions = self.convert_unions_list_to_dataframe(local_unions)
        data = self.merge_unionfacts_with_ibew_on_lu(
            unionfacts_locals_directory_df, df_local_unions
        )
        data = self._cleanup_data(data)
        self.save_df_as_excel(data, output_file)
        end_time = time.time()

        logger.info({"action": "UnionDataScraper.run", "duration_seconds": end_time - start_time})


def parse_states(states_str):
    """
    Parse a comma-separated string of states.
    """
    return [state.strip() for state in states_str.split(",") if state.strip()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IBEW Data Scraper")
    parser.add_argument(
        "--states",
        type=parse_states,
        required=True,
        help="Comma-separated list of state abbreviations to query, e.g. NY,CT,RI",
    )
    parser.add_argument("--output", required=False, help="Output file name (must end with .xlsx)")
    args = parser.parse_args()

    # Validate output file extension
    if args.output and not args.output.endswith(".xlsx"):
        sys.exit("Error: The output file must have a .xlsx extension.")

    # Initialize and run the scraper
    scraper = IBEWDataScraper(states=args.states)
    if not args.output:
        args.output = "merged_union_data.xlsx"
    scraper.run(output_file=args.output)
