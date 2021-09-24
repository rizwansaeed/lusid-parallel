import argparse
import concurrent.futures
import csv
import json
import logging
import sys
import time
from concurrent.futures import ALL_COMPLETED
from datetime import datetime

import lusid as lu
import pytz as pytz
from lusid.utilities import ApiClientFactory


def setup_logging():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    logging_formatter = logging.Formatter('%(levelname)s %(asctime)s - %(message)s')
    stdout_handler.setFormatter(logging_formatter)
    root_logger.addHandler(stdout_handler)


def exec_in_threads(fn, num, **kwargs):
    start = time.perf_counter()

    futures = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = [
            executor.submit(fn, **{**kwargs, 'i': i})
            for i in range(num)
        ]
        concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)

    end = time.perf_counter()

    logging.info(f"completed in {end - start:0.4f} seconds")

    return futures


def create_portfolio(portfolios_api, scope, i):
    portfolios_api.create_portfolio(
        scope=scope,
        create_transaction_portfolio_request=lu.CreateTransactionPortfolioRequest(
            display_name=f"test-portfolio-{i}",
            code=f"test-portfolio-{i}",
            base_currency="GBP",
            created=datetime(2000, 1, 1, tzinfo=pytz.utc)
        )
    )


def create_portfolios(portfolios_api, num, scope):
    logging.info(f"creating {num} portfolios")
    exec_in_threads(create_portfolio, num, portfolios_api=portfolios_api, scope=scope)
    logging.info(f"created {num} portfolios")


def upload_return_series(portfolios_api, i, scope, return_scope, return_code, returns):
    start = time.perf_counter()

    logging.info(f"sending {i} {return_scope} {return_code}")

    try:
        portfolios_api.upsert_portfolio_returns(
            scope=scope,
            code=f"test-portfolio-{i}",
            return_scope=return_scope,
            return_code=return_code,
            performance_return=returns
        )
    except lu.ApiException as ex:
        logging.error(f"{json.loads(ex.body)['title']}")

    end = time.perf_counter()

    logging.info(f"loaded {i} {return_scope} {return_code} {end - start:0.4f} seconds")


def upload_returns(portfolios_api, num, scope, file, batch_size):
    logging.info(f"uploading returns")

    returns = {}

    with open(file, "r") as f:
        reader = csv.reader(f, delimiter=",")
        next(reader)

        for i, line in enumerate(reader):
            perf_scope = returns.get(line[5], {})
            perf_code = perf_scope.get(line[6], [])
            perf_code.append(lu.PerformanceReturn(
                effective_at=datetime.strptime(line[1], "%d/%m/%Y").replace(tzinfo=pytz.utc),
                rate_of_return=line[3],
                closing_market_value=line[2],
                period="Daily"
            ))

            perf_scope[line[6]] = perf_code
            returns[line[5]] = perf_scope

    batched_rtns = {}

    for rtn_scope, rtn_codes in returns.items():
        for rtn_code, rtns in rtn_codes.items():
            # batch the returns
            rtns_batches = [rtns[i:i + batch_size] for i in range(0, len(rtns), batch_size)]

            batched_scope = batched_rtns.get(rtn_scope, {})
            batched_scope[rtn_code] = rtns_batches
            batched_rtns[rtn_scope] = batched_scope

    start = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        futures = [
            executor.submit(
                upload_return_series,
                portfolios_api=portfolios_api,
                i=i,
                scope=scope,
                return_scope=rtn_scope,
                return_code=rtn_code,
                returns=rtns
            )
            for i in range(num)
            for rtn_scope, rtn_codes in batched_rtns.items()
            for rtn_code, rtns_batch in rtn_codes.items()
            for rtns in rtns_batch
        ]

        concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)

    end = time.perf_counter()

    logging.info(f"completed in {end - start:0.4f} seconds")
    logging.info(f"uploaded returns")


def main(argv):
    setup_logging()

    ap = argparse.ArgumentParser()

    ap.add_argument("-a", "--secrets", type=str, required=True, action="store")
    ap.add_argument("-n", "--num", type=int, required=True, action="store")
    ap.add_argument("-s", "--scope", action="store")
    ap.add_argument("-p", required=False, action="store_true", help="create portfolios")
    ap.add_argument("-t", required=False, action="store_true", help="load returns")
    ap.add_argument("-f", required=False, action="store", help="data file")
    ap.add_argument("-b", required=False, action="store", help="batch size", type=int, default=2000)

    args = ap.parse_args()

    api_factory = ApiClientFactory(api_secrets_filename=args.secrets)
    txns_api = api_factory.build(lu.TransactionPortfoliosApi)
    portfolios_api = api_factory.build(lu.PortfoliosApi)

    num = args.num

    if args.p and args.scope:
        create_portfolios(txns_api, num, args.scope)

    if args.t and args.scope and args.b:
        upload_returns(portfolios_api, num, args.scope, args.f, args.b)


if __name__ == "__main__":
    main(sys.argv)
