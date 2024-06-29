import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_pairs_info_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://www.bitstamp.net/api/v2/trading-pairs-info/'
    proxies = {"https": "socks5://127.0.0.1:10808", "http": "socks5://127.0.0.1:10808"}
    response = requests.get(url, proxies=proxies)

    return pd.read_json(io.StringIO(response.text))


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
