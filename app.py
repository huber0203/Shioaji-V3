from flask import Flask, request
import shioaji as sj
import json
import logging
import os
import socket
import sys
import time
import psutil

app = Flask(__name__)

# Logging setup
logger = logging.getLogger('shioaji')
logger.setLevel(logging.INFO)
handler = logging.FileHandler('/tmp/shioaji.log')
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.handlers = [handler]

# Log service IP
try:
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    logger.info(f"Service running on host: {hostname}, IP: {ip_address}")
except Exception as e:
    logger.error(f"Failed to get IP address: {str(e)}")

# Global variables
api = None
contract_cache = {}  # Contract cache

@app.route('/login', methods=['POST'])
def login():
    global api
    try:
        data = request.get_json()
        if not data:
            logger.error("Request body is empty")
            return {"statusCode": 400, "body": json.dumps({"error": "Request body is empty"})}

        api_key = data.get("api_key")
        secret_key = data.get("secret_key")
        ca_path = data.get("ca_path", "/app/Sinopac.pfx")
        ca_password = data.get("ca_password")
        person_id = data.get("person_id")
        simulation_mode = data.get("simulation_mode", False)

        missing_params = []
        if not api_key:
            missing_params.append("api_key")
        if not secret_key:
            missing_params.append("secret_key")
        if not simulation_mode:
            if not ca_password:
                missing_params.append("ca_password")
            if not person_id:
                missing_params.append("person_id")

        if missing_params:
            logger.error(f"Missing parameters: {', '.join(missing_params)}")
            return {"statusCode": 400, "body": json.dumps({"error": f"Missing parameters: {', '.join(missing_params)}"})}

        if not simulation_mode and not os.path.exists(ca_path):
            logger.error(f"CA file not found at {ca_path}")
            return {"statusCode": 500, "body": json.dumps({"error": f"CA file not found at {ca_path}"})}

        logger.info(f"Initializing Shioaji with simulation={simulation_mode}")
        api = sj.Shioaji(simulation=simulation_mode)

        if not simulation_mode:
            logger.info(f"Activating CA with ca_path={ca_path}")
            result = api.activate_ca(ca_path=ca_path, ca_passwd=ca_password, person_id=person_id)
            if not result:
                logger.error("Failed to activate CA")
                return {"statusCode": 500, "body": json.dumps({"error": "Failed to activate CA"})}

        logger.info("Logging into Shioaji")
        accounts = api.login(api_key=api_key, secret_key=secret_key)
        logger.info(f"Login successful, accounts: {json.dumps(accounts, default=str)}")

        logger.info("Fetching contracts data")
        api.fetch_contracts()
        logger.info("Contracts data fetched successfully")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Login successful", "accounts": accounts}, default=str)
        }
    except Exception as e:
        logger.error(f"Error in login: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Error in login: {str(e)}"})}

@app.route('/fetch_all', methods=['GET'])
def fetch_all():
    global api, contract_cache
    try:
        if api is None:
            logger.error("Shioaji API not initialized")
            return {"statusCode": 500, "body": json.dumps({"error": "Shioaji API not initialized"})}

        # Check traffic usage
        usage = api.usage()
        logger.info(f"Usage: {usage}")
        if usage.bytes > 0.8 * usage.limit_bytes:
            logger.warning("Approaching daily traffic limit!")
            return {"statusCode": 429, "body": json.dumps({"error": "Approaching traffic limit"})}

        # Check memory usage
        process = psutil.Process()
        mem_info = process.memory_info()
        logger.info(f"Memory usage: {mem_info.rss / 1024**2:.2f} MB")
        if mem_info.rss > 12 * 1024**3:  # 12GB threshold
            logger.warning("High memory usage detected!")
            return {"statusCode": 429, "body": json.dumps({"error": "High memory usage"})}

        # Populate contract cache
        if not contract_cache:
            logger.info("Populating contract cache")
            # Stocks (TSE, OTC, OES)
            for market, container in [
                ("TSE", api.Contracts.Stocks.TSE),
                ("OTC", api.Contracts.Stocks.OTC),
                ("OES", getattr(api.Contracts.Stocks, "OES", None))
            ]:
                if container is None:
                    logger.info(f"{market} not supported")
                    continue
                for contract in list(container):
                    if hasattr(contract, 'code'):
                        cache_key = f"stock_{contract.code}"
                        contract_cache[cache_key] = contract
                        contract_cache[cache_key + "_market"] = market
                        # Warrants check (assuming warrants in TSE/OTC with specific category)
                        if hasattr(contract, 'category') and contract.category == "Warrant":
                            warrant_key = f"warrant_{contract.code}"
                            contract_cache[warrant_key] = contract
                            contract_cache[warrant_key + "_market"] = f"{market}_Warrant"
            # Futures
            for contract in list(api.Contracts.Futures):
                if hasattr(contract, 'code'):
                    cache_key = f"futures_{contract.code}"
                    contract_cache[cache_key] = contract
                    contract_cache[cache_key + "_market"] = "Futures"
            # Options
            for contract in list(api.Contracts.Options):
                if hasattr(contract, 'code'):
                    cache_key = f"options_{contract.code}"
                    contract_cache[cache_key] = contract
                    contract_cache[cache_key + "_market"] = "Options"
            logger.info(f"Cached {len(contract_cache)//2} contracts")

        # Collect all contracts
        contracts = [v for k, v in contract_cache.items() if not k.endswith("_market")]
        markets = [contract_cache[k + "_market"] for k, v in contract_cache.items() if not k.endswith("_market")]

        # Batch query
        batch_size = 200
        quotes = []
        for i in range(0, len(contracts), batch_size):
            batch = contracts[i:i + batch_size]
            logger.info(f"Fetching batch {i//batch_size + 1}: {len(batch)} contracts")
            try:
                batch_quotes = api.snapshots(batch)
                quotes.extend(batch_quotes)
            except Exception as e:
                logger.error(f"Batch {i//batch_size + 1} failed: {str(e)}")
                continue
            time.sleep(1)  # 1-second interval

        # Format response
        result = [
            {
                "code": q.code,
                "market": markets[i],
                "price": q.close if hasattr(q, 'close') else None,
                "timestamp": q.ts if hasattr(q, 'ts') else None
            } for i, q in enumerate(quotes)
        ]

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Quotes fetched", "quotes": result}, default=str)
        }
    except Exception as e:
        logger.error(f"Error in fetch_all: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Error in fetch_all: {str(e)}"})}

@app.route('/quote', methods=['GET'])
def quote():
    global api
    try:
        code = request.args.get("code")
        type_ = request.args.get("type", "stock")

        if not code:
            logger.error("Missing parameter: code")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing parameter: code"})}

        if api is None:
            logger.error("Shioaji API not initialized")
            return {"statusCode": 500, "body": json.dumps({"error": "Shioaji API not initialized"})}

        logger.info(f"Received quote request: code={code}, type={type_}")

        contract = None
        market = None
        if type_ == "stock":
            for m, c in [("TSE", api.Contracts.Stocks.TSE), ("OTC", api.Contracts.Stocks.OTC), ("OES", getattr(api.Contracts.Stocks, "OES", None))]:
                if c is None:
                    continue
                try:
                    contract = c[code]
                    market = m
                    break
                except KeyError:
                    continue
            # Check for warrants
            if contract and hasattr(contract, 'category') and contract.category == "Warrant":
                market = f"{market}_Warrant"
        elif type_ == "futures":
            contract = api.Contracts.Futures[code]
            market = "Futures"
        elif type_ == "options":
            contract = api.Contracts.Options[code]
            market = "Options"
        else:
            logger.error(f"Unsupported type: {type_}")
            return {"statusCode": 400, "body": json.dumps({"error": f"Unsupported type: {type_}"})}

        if contract is None:
            logger.error(f"Contract not found for code={code}, type={type_}")
            return {"statusCode": 500, "body": json.dumps({"error": f"Contract not found for code={code}"})}

        logger.info(f"Fetching quote for code={code}")
        quote = api.snapshots([contract])[0]

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Quote fetched",
                "quote": {"code": quote.code, "price": quote.close, "timestamp": quote.ts},
                "market": market,
                "type": type_
            }, default=str)
        }
    except Exception as e:
        logger.error(f"Error in quote: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": f"Error in quote: {str(e)}"})}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
