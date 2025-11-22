"""
FRED Delta Lake Query Tool
Query FRED data directly from Azure Delta Lake without Spark
"""

import deltalake as dl
import polars as pl
from datetime import date, datetime
from typing import Optional, List, Union
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)


class FREDQuery:
    """Query FRED data from Delta Lake"""

    STORAGE_ACCOUNT = "gzcstorageaccount"
    CONTAINER = "macroeconomic-maintained-series"
    BASE_PATH = "US_Fred_Data"

    def __init__(self):
        self.storage_options = {
            "account_name": self.STORAGE_ACCOUNT,
            "use_azure_cli": "true",
        }
        self._observations_table = None
        self._metadata_table = None

    @property
    def observations(self) -> dl.DeltaTable:
        """Get series_observations Delta table"""
        if self._observations_table is None:
            uri = f"az://{self.CONTAINER}/{self.BASE_PATH}/series_observations"
            self._observations_table = dl.DeltaTable(uri, storage_options=self.storage_options)
        return self._observations_table

    def info(self) -> dict:
        """Get table info"""
        dt = self.observations
        return {
            "version": dt.version(),
            "files": len(dt.file_uris()),
            "schema": [f.name for f in dt.schema().fields],
        }

    def get_series(
        self,
        series_id: Union[str, List[str]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> pl.DataFrame:
        """
        Query observations for one or more series

        Args:
            series_id: Series ID or list of IDs (e.g., "M2MSL" or ["M2MSL", "UNRATE"])
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            columns: Columns to return (default: all)

        Returns:
            Polars DataFrame
        """
        # Use lazy scan for efficient filtering
        lf = pl.scan_delta(
            f"az://{self.CONTAINER}/{self.BASE_PATH}/series_observations",
            storage_options=self.storage_options,
        )

        # Apply filters using Polars expressions (pushed down to scan)
        if isinstance(series_id, str):
            lf = lf.filter(pl.col("series_id") == series_id)
        else:
            lf = lf.filter(pl.col("series_id").is_in(series_id))

        if start_date:
            lf = lf.filter(pl.col("date") >= pl.lit(start_date).str.to_date())
        if end_date:
            lf = lf.filter(pl.col("date") <= pl.lit(end_date).str.to_date())

        # Select columns
        if columns:
            lf = lf.select(columns)

        # Sort by date and collect
        return lf.sort("date").collect()

    def list_series(self, pattern: Optional[str] = None, limit: int = 100) -> pl.DataFrame:
        """
        List available series IDs

        Args:
            pattern: Filter pattern (e.g., "M2" to find M2MSL, M2MNS, etc.)
            limit: Max results

        Returns:
            DataFrame with unique series_id values
        """
        lf = pl.scan_delta(
            f"az://{self.CONTAINER}/{self.BASE_PATH}/series_observations",
            storage_options=self.storage_options,
        ).select("series_id").unique().sort("series_id")

        if pattern:
            lf = lf.filter(pl.col("series_id").str.contains(pattern.upper()))

        return lf.head(limit).collect()

    def search(self, pattern: str) -> pl.DataFrame:
        """Search for series containing pattern"""
        return self.list_series(pattern=pattern)

    def latest(self, series_id: str, n: int = 1) -> pl.DataFrame:
        """Get latest n observations for a series"""
        df = self.get_series(series_id)
        return df.sort("date", descending=True).head(n)

    def count_by_series(self, limit: int = 50) -> pl.DataFrame:
        """Count observations per series (top N)"""
        return pl.scan_delta(
            f"az://{self.CONTAINER}/{self.BASE_PATH}/series_observations",
            storage_options=self.storage_options,
        ).select("series_id").group_by("series_id").count().sort("count", descending=True).head(limit).collect()

    def to_pandas(self, df: pl.DataFrame):
        """Convert Polars DataFrame to Pandas"""
        return df.to_pandas()


# Convenience functions for quick queries
_query = None

def get_query() -> FREDQuery:
    global _query
    if _query is None:
        _query = FREDQuery()
    return _query

def series(series_id: str, start: str = None, end: str = None) -> pl.DataFrame:
    """Quick query: fred.series('M2MSL', '2020-01-01')"""
    return get_query().get_series(series_id, start_date=start, end_date=end)

def search(pattern: str) -> pl.DataFrame:
    """Quick search: fred.search('GDP')"""
    return get_query().search(pattern)

def latest(series_id: str, n: int = 10) -> pl.DataFrame:
    """Quick latest: fred.latest('M2MSL', 10)"""
    return get_query().latest(series_id, n)

def info() -> dict:
    """Get Delta table info"""
    return get_query().info()


if __name__ == "__main__":
    # Demo
    print("="*60)
    print("FRED Delta Lake Query Tool")
    print("="*60)

    q = FREDQuery()
    print(f"\nTable info: {q.info()}")

    print("\n📊 M2 Money Supply (latest 5):")
    df = q.get_series("M2MSL")
    print(df.sort("date", descending=True).head(5))

    print("\n🔍 Search for series containing 'M2':")
    print(q.search("M2"))
