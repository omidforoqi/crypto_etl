from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.clickhouse import ClickHouse
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_transactions_to_clickhouse(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Clickhouse database.
    Specify your configuration settings in 'io_config.yaml'.

    """
    database = 'crypto_db'
    table_name = 'transactions'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    ClickHouse.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_name=table_name,
        database=database,
        index=False,
        if_exists='replace'
    )
