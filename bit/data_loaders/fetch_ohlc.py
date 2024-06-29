import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(data, *args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://www.bitstamp.net/api/v2/ohlc/{}/?step=60&limit=60'
    proxies = {"https": "socks5://127.0.0.1:10808", "http": "socks5://127.0.0.1:10808"}
    
    dfs = []
    for symbol in data['url_symbol'].to_list():

        response = requests.get(url.format(symbol), proxies=proxies)
        df = pd.DataFrame(response.json()['data']['ohlc'])

        df.insert(0, "url_symbol", symbol, allow_duplicates=False)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
