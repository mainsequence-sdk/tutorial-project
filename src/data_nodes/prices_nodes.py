import json

# Imports should be at the top of the file
import numpy as np
np.NaN = np.nan # Fix for a pandas-ta compatibility issue
from mainsequence.tdag import DataNode, APIDataNode, WrapperDataNode
from mainsequence.client.models_tdag import UpdateStatistics, ColumnMetaData
import mainsequence.client as msc
from typing import Union, Optional, List, Dict, Any
import datetime
import pytz
import pandas as pd
from sklearn.linear_model import ElasticNet
import copy
from pydantic import BaseModel,Field
from abc import ABC, abstractmethod

MARKET_TIME_SERIES_UNIQUE_IDENTIFIER_CATEGORY_PRICES = "simulated_prices_from_category"
TEST_TRANSLATION_TABLE_UID="test_translation_table"



import base64

class SimulatedPricesManager:

    def __init__(self,owner:DataNode):
        self.owner = owner

    @staticmethod
    def _get_last_price(obs_df: pd.DataFrame, unique_id: str, fallback: float) -> float:
        """
        Helper method to retrieve the last price for a given unique_id or return 'fallback'
        if unavailable.

        Args:
            obs_df (pd.DataFrame): A DataFrame with multi-index (time_index, unique_identifier).
            unique_id (str): Asset identifier to look up.
            fallback (float): Value to return if the last price cannot be retrieved.

        Returns:
            float: Last observed price or the fallback value.
        """
        # If there's no historical data at all, return fallback immediately
        if obs_df.empty:
            return fallback

        # Try to slice for this asset and get the last 'close' value
        try:
            slice_df = obs_df.xs(unique_id, level="unique_identifier")["close"]
            return slice_df.iloc[-1]
        except (KeyError, IndexError):
            # KeyError if unique_id not present, IndexError if slice is empty
            return fallback
    def update(self)->pd.DataFrame:
        """
       Mocks price updates for assets with stochastic lognormal returns.
       For each asset, simulate new data starting one hour after its last update
        until yesterday at 00:00 UTC, using the last observed price as the seed.
        The last observation is not duplicated.
        Returns:
            pd.DataFrame: A DataFrame with a multi-index (time_index, unique_identifier)
                          and a single column 'close' containing the simulated prices.
        """
        import numpy as np

        initial_price = 100.0
        mu = 0.0  # drift component for lognormal returns
        sigma = 0.01  # volatility component for lognormal returns

        df_list = []
        update_statistics=self.owner.update_statistics
        # Get the latest historical observations; assumed to be a DataFrame with a multi-index:
        # (time_index, unique_identifier) and a column "close" for the last observed price.
        range_descriptor=update_statistics.get_update_range_map_great_or_equal()
        last_observation = self.owner.get_ranged_data_per_asset(range_descriptor=range_descriptor)
        # Define simulation end: yesterday at midnight (UTC)
        yesterday_midnight = datetime.datetime.now(pytz.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - datetime.timedelta(days=1)
        # Loop over each unique identifier and its last update timestamp.
        for asset in update_statistics.asset_list:
            # Simulation starts one hour after the last update.
            start_time = update_statistics.get_asset_earliest_multiindex_update(asset=asset) + datetime.timedelta(hours=1)
            if start_time > yesterday_midnight:
                continue  # Skip if no simulation period is available.
            time_range = pd.date_range(start=start_time, end=yesterday_midnight, freq='D')
            if len(time_range) == 0:

                continue
             # Use the last observed price for the asset as the starting price (or fallback).
            last_price = self._get_last_price(
            obs_df=last_observation,
            unique_id=asset.unique_identifier,
            fallback=initial_price
                )

            random_returns = np.random.lognormal(mean=mu, sigma=sigma, size=len(time_range))
            simulated_prices = last_price * np.cumprod(random_returns)
            df_asset = pd.DataFrame({asset.unique_identifier: simulated_prices}, index=time_range)
            df_list.append(df_asset)

        if df_list:
            data = pd.concat(df_list, axis=1)
        else:
            return pd.DataFrame()

        # Reshape the DataFrame into long format with a multi-index.
        data.index.name = "time_index"
        data = data.melt(ignore_index=False, var_name="unique_identifier", value_name="close")
        data = data.set_index("unique_identifier", append=True)
        return data



    def get_column_metadata(self):
        from mainsequence.client.models_tdag import ColumnMetaData
        columns_metadata = [ColumnMetaData(column_name="close",
                                           dtype="float",
                                           label="Close ",
                                           description=(
                                               "Simulated close price"
                                           )
                                           ),


                            ]
        return columns_metadata



class PriceSimulConfig(BaseModel):

    asset_list:List[msc.AssetMixin]=Field(...,title="Asset List",description="List of assets to simulate",
                                          ignore_from_storage_hash=True
                                          )

class SimulatedPrices(DataNode):
    """
    Simulates price updates for a specific list of assets provided at initialization.
    """
    OFFSET_START = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)
    def __init__(self, simulation_config: PriceSimulConfig,
                 *args, **kwargs):
        """
        Args:
            asset_list (ModelList): List of asset objects.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.asset_list = simulation_config.asset_list
        self.asset_symbols_filter = [a.unique_identifier for a in self.asset_list]
        super().__init__(*args, **kwargs)

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def update(self):
        update_manager=SimulatedPricesManager(self)
        df=update_manager.update()
        return df


    def get_column_metadata(self):
        """
        Add MetaData information to the DataNode Table
        Returns:

        """
        from mainsequence.client.models_tdag import ColumnMetaData
        columns_metadata = [ColumnMetaData(column_name="close",
                                           dtype="float",
                                           label="Close",
                                           description=(
                                               "Simulated Close Price"
                                           )
                                           ),


                            ]
        return columns_metadata

    def get_table_metadata(self)->msc.TableMetaData:
        """
        REturns the market time serie unique identifier, assets to append , or asset to overwrite
        Returns:

        """

        mts=msc.TableMetaData(identifier="simulated_prices",
                                               data_frequency_id=msc.DataFrequency.one_d,
                                               description="This is a simulated prices time serie from asset category",
                                               )


        return mts
