from time import sleep
from typing import Optional
import requests
import datetime
import logging
from rich.logging import RichHandler
import clickhouse_connect

logging.basicConfig(
    level="NOTSET", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("rich")


class BitstampClient:
    def __init__(self, api_url: str = "https://www.bitstamp.net/api/v2") -> None:
        self.api_url = api_url
        self.proxies = {"https": "socks5://127.0.0.1:10808", "http": "socks5://127.0.0.1:10808"}

    def fetch_symbols_with_dollar(self) -> list[str]:
        url = f"{self.api_url}/trading-pairs-info/"

        response = requests.request("GET", url, timeout=3, proxies=self.proxies)
        j = response.json()

        symbols_dollar_based = [
            i["url_symbol"] for i in j if "/ U.S. dollar" in i["description"]
        ]

        for currency in response.json():
            if currency["instant_and_market_orders"] == "Disabled":
                log.info(f"{currency['name']} have not instant and market orders")
                continue
            if "/ U.S. dollar" in currency["description"]:
                symbols_dollar_based.append(currency["url_symbol"])
        symbols_dollar_based.sort()
        log.info(f"{len(symbols_dollar_based)} currency name extracted")
        return symbols_dollar_based

    def fetch_transactions(self, market_symbol: str, time: str = "hour"):
        url = f"{self.api_url}/transactions/{market_symbol}"
        params = {"time": time}
        response = requests.request(
            "GET", url, params=params, timeout=10, proxies=self.proxies
        )
        if response.status_code != 200:
            raise Exception(f"Error: {response.status_code} {response.text}")
        data = response.json()
        output = [
            [
                market_symbol,
                float(d["amount"]),
                datetime.datetime.fromtimestamp(int(d["date"])),
                float(d["price"]),
                bool(int(d["type"])),
            ]
            for d in data
        ]
        return output

    def fetch_ohlc(
        self,
        currency_pair: str,
        step: int,
        limit: int,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ):
        url = f"{self.api_url}/ohlc/{currency_pair}/"

        params: dict[str, int] = {
            "step": step,
            "limit": limit,
        }
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        response = requests.request(
            "GET", url, params=params, timeout=5, proxies=self.proxies
        )
        if response.status_code != 200:
            raise Exception(f"Error: {response.status_code} {response.text}")
        data = response.json()
        output = []
        for row in data["data"]["ohlc"]:
            output.append(
                [
                    data["data"]["pair"],
                    row["open"],
                    row["close"],
                    row["high"],
                    row["low"],
                    datetime.datetime.fromtimestamp(int(row["timestamp"])),
                ]
            )
        return output

    def fetch_past_ohlc(self, days: float = 0, hours: float = 0):
        symbols: list[str] = self.fetch_symbols_with_dollar()
        step: int = 60
        limit: int = 1000
        current_datetime = datetime.datetime.now()
        current_timestamp = int(current_datetime.timestamp())
        days_ago = current_datetime - datetime.timedelta(days=days, hours=hours)
        days_ago_timestamp = int(days_ago.timestamp())
        count_of_data_in_one_req = step * (limit - 1)
        starts_timestamp = range(
            days_ago_timestamp, current_timestamp, count_of_data_in_one_req
        )

        for symbol in symbols:
            for start in starts_timestamp:
                try:
                    data = self.fetch_ohlc(
                        currency_pair=symbol, step=step, limit=limit, start=start
                    )
                    log.info(
                        f"Fetch {symbol} from {start} to {start + count_of_data_in_one_req}"
                    )
                    yield data
                except:
                    return None

    def watch_hour_bitstamp(self):
        while True:
            log.info("start watch")
            symbols = self.fetch_symbols_with_dollar()
            for symbol in symbols:
                transactions = self.fetch_transactions(market_symbol=symbol)
                ohlc = self.fetch_ohlc(
                    currency_pair=symbol, step=60, limit=60
                )
                yield {"transactions": transactions, "ohlc": ohlc}
            log.info("start sleeping")
            sleep(3600)


def main():
    bitstamp_client = BitstampClient()
    CLICKHOUSE_CLOUD_HOSTNAME = "localhost"
    CLICKHOUSE_CLOUD_USER = "default"
    clickhouse_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_CLOUD_HOSTNAME,
        port=8123,
        username=CLICKHOUSE_CLOUD_USER,
    )
    EXTRACT_PAST_DATA = True
    if EXTRACT_PAST_DATA:
        for d in bitstamp_client.fetch_past_ohlc(days=7):
            if d:
                clickhouse_client.insert("crypto_db.bitstamp", d, column_names=["coin_name", "open", "close", "high", "low", "timestamp"])
            else:
                log.error("data is empty")
        symbols: list[str] = bitstamp_client.fetch_symbols_with_dollar()
        for symbol in symbols:
            transaction = bitstamp_client.fetch_transactions(market_symbol=symbol, time="day")
            clickhouse_client.insert("crypto_db.transactions", transaction, column_names=["coin_name", "amount", "timestamp", "price", "type"])

if __name__ == "__main__":
    main()