from flask import Flask, request
import shioaji as sj
import json
import logging
import os
import socket
import sys

app = Flask(__name__)

# 設置日誌，寫入 /tmp/shioaji.log
logger = logging.getLogger('shioaji')
logger.setLevel(logging.INFO)
handler = logging.FileHandler('/tmp/shioaji.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.handlers = [handler]

# 記錄服務的 IP 位址
try:
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    logger.info(f"Service running on host: {hostname}, IP: {ip_address}")
except Exception as e:
    logger.error(f"Failed to get IP address: {str(e)}")

# 全局變數，用於儲存 Shioaji API 實例
api = None

@app.route('/login', methods=['POST'])
def login():
    global api
    try:
        data = request.get_json()
        if not data:
            error_msg = "Request body is empty"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

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
            error_msg = f"Missing required parameters: {', '.join(missing_params)}"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        if not simulation_mode:
            if not os.path.exists(ca_path):
                error_msg = f"CA file not found at {ca_path}"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}
            logger.info(f"CA file found at {ca_path}")

        logger.info(f"Received login request: api_key={api_key[:4]}****, secret_key={secret_key[:4]}****")
        logger.info(f"Initializing Shioaji with simulation={simulation_mode}")
        api = sj.Shioaji(simulation=simulation_mode)

        if not simulation_mode:
            logger.info(f"Activating CA with ca_path={ca_path}, person_id={person_id}")
            result = api.activate_ca(ca_path=ca_path, ca_passwd=ca_password, person_id=person_id)
            if not result:
                error_msg = "Failed to activate CA"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}
            logger.info("CA activated successfully")

        logger.info("Logging into Shioaji")
        accounts = api.login(api_key=api_key, secret_key=secret_key)
        logger.info(f"Login successful, accounts: {json.dumps(accounts, default=str)}")

        logger.info("Fetching contracts data")
        api.fetch_contracts()
        logger.info("Contracts data fetched successfully")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Login successful", "accounts": accounts}, default=str, ensure_ascii=False)
        }

    except Exception as e:
        error_msg = f"Error in login: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception traceback: {sys.exc_info()}")
        return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

@app.route('/quote', methods=['GET'])
def quote():
    global api
    try:
        code = request.args.get("code")  # 使用官方參數 "code"
        type_ = request.args.get("type", "stock")  # 預設為 stock

        if not code:
            error_msg = "Missing required parameter: code"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        if api is None:
            error_msg = "Shioaji API not initialized. Please login first."
            logger.error(error_msg)
            return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        logger.info(f"Received quote request: code={code}, type={type_}")

        # 根據 type 選擇合約類型
        contract = None
        market = None

        if type_ == "stock":
            # 嘗試從 TSE 查詢股票合約
            logger.info(f"Fetching contract for code={code} from TSE")
            contract = api.Contracts.Stocks.TSE[code]
            market = "TSE"

            # 如果 TSE 中找不到，嘗試從 OTC 查詢
            if contract is None:
                logger.info(f"Contract not found in TSE, trying OTC for code={code}")
                contract = api.Contracts.Stocks.OTC[code]
                market = "OTC"

            # 如果 OTC 中找不到，嘗試從 OES（興櫃）查詢
            if contract is None:
                logger.info(f"Contract not found in OTC, trying OES for code={code}")
                try:
                    contract = api.Contracts.Stocks.OES[code]
                    market = "OES"
                except AttributeError:
                    logger.info("OES market not supported or accessible in this context")

            if contract is None:
                error_msg = f"Contract not found for code={code} in TSE, OTC, or OES"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        elif type_ == "futures":
            # 查詢期貨合約
            logger.info(f"Fetching futures contract for code={code}")
            contract = api.Contracts.Futures[code]
            market = "Futures"

            if contract is None:
                logger.info(f"Contract not found for code={code}, listing available futures contracts")
                available_futures = {c.code: c.__dict__ for c in list(api.Contracts.Futures) if hasattr(c, 'code')}
                logger.info(f"Available futures contracts: {json.dumps(available_futures, default=str)}")
                error_msg = f"Futures contract not found for code={code}"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        elif type_ == "options":
            # 查詢選擇權合約
            logger.info(f"Fetching options contract for code={code}")
            contract = api.Contracts.Options[code]
            market = "Options"

            if contract is None:
                error_msg = f"Options contract not found for code={code}"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        elif type_ == "index":
            # 查詢指數合約
            logger.info(f"Fetching index contract for code={code} from TSE")
            contract = api.Contracts.Indexs.TSE[code]
            market = "Index"

            if contract is None:
                error_msg = f"Index contract not found for code={code} in TSE"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        else:
            error_msg = f"Unsupported type: {type_}. Supported types are: stock, futures, options, index"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        # 記錄合約資料
        logger.info(f"Contract found in {market}: {json.dumps(contract.__dict__, default=str)}")

        # 查詢快照資料
        logger.info(f"Fetching quote for code={code} (type={type_})")
        quote = api.snapshots([contract])[0]
        logger.info(f"Quote fetched successfully: {json.dumps(quote, default=str)}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Quote fetched",
                "quote": quote,
                "market": market,
                "type": type_
            }, default=str, ensure_ascii=False)
        }

    except KeyError as ke:
        error_msg = f"Contract not found for code={code} (type={type_}, KeyError: {str(ke)})"
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}
    except Exception as e:
        error_msg = f"Error in quote: {str(e)} (type={type_})"
        logger.error(error_msg)
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception traceback: {sys.exc_info()}")
        return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

@app.route('/contracts', methods=['GET'])
def get_contracts():
    global api
    try:
        if api is None:
            error_msg = "Shioaji API not initialized. Please login first."
            logger.error(error_msg)
            return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        # 獲取 TSE 股票合約資料
        logger.info("Fetching TSE contracts")
        tse_contracts = {}
        for contract in list(api.Contracts.Stocks.TSE):
            if hasattr(contract, 'code'):
                tse_contracts[contract.code] = contract.__dict__
        logger.info("TSE contracts fetched successfully")

        # 獲取 OTC 股票合約資料
        logger.info("Fetching OTC contracts")
        otc_contracts = {}
        for contract in list(api.Contracts.Stocks.OTC):
            if hasattr(contract, 'code'):
                otc_contracts[contract.code] = contract.__dict__
        logger.info("OTC contracts fetched successfully")

        # 獲取 OES（興櫃）股票合約資料
        logger.info("Fetching OES contracts")
        oes_contracts = {}
        try:
            for contract in list(api.Contracts.Stocks.OES):
                if hasattr(contract, 'code'):
                    oes_contracts[contract.code] = contract.__dict__
            logger.info("OES contracts fetched successfully")
        except AttributeError:
            logger.info("OES market not supported or accessible in this context")
            oes_contracts = {}

        # 獲取期貨合約資料
        logger.info("Fetching Futures contracts")
        futures_contracts = {}
        for contract in list(api.Contracts.Futures):
            if hasattr(contract, 'code'):
                futures_contracts[contract.code] = contract.__dict__
        logger.info("Futures contracts fetched successfully")

        # 獲取選擇權合約資料
        logger.info("Fetching Options contracts")
        options_contracts = {}
        for contract in list(api.Contracts.Options):
            if hasattr(contract, 'code'):
                options_contracts[contract.code] = contract.__dict__
        logger.info("Options contracts fetched successfully")

        # 獲取指數合約資料
        logger.info("Fetching Index contracts")
        index_contracts = {}
        for contract in list(api.Contracts.Indexs.TSE):
            if hasattr(contract, 'code'):
                index_contracts[contract.code] = contract.__dict__
        logger.info("Index contracts fetched successfully")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Contracts fetched",
                "tse_contracts": tse_contracts,
                "otc_contracts": otc_contracts,
                "oes_contracts": oes_contracts,
                "futures_contracts": futures_contracts,
                "options_contracts": options_contracts,
                "index_contracts": index_contracts
            }, default=str, ensure_ascii=False)
        }

    except Exception as e:
        error_msg = f"Error in contracts: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception traceback: {sys.exc_info()}")
        return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

@app.route('/contract', methods=['GET'])
def get_contract():
    global api
    try:
        code = request.args.get("code")  # 使用官方參數 "code"
        type_ = request.args.get("type", "stock")  # 預設為 stock

        if not code:
            error_msg = "Missing required parameter: code"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        if api is None:
            error_msg = "Shioaji API not initialized. Please login first."
            logger.error(error_msg)
            return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        logger.info(f"Received contract request: code={code}, type={type_}")

        # 根據 type 選擇合約類型
        contract = None
        market = None

        if type_ == "stock":
            # 嘗試從 TSE 查詢股票合約
            logger.info(f"Fetching contract for code={code} from TSE")
            contract = api.Contracts.Stocks.TSE[code]
            market = "TSE"

            # 如果 TSE 中找不到，嘗試從 OTC 查詢
            if contract is None:
                logger.info(f"Contract not found in TSE, trying OTC for code={code}")
                contract = api.Contracts.Stocks.OTC[code]
                market = "OTC"

            # 如果 OTC 中找不到，嘗試從 OES（興櫃）查詢
            if contract is None:
                logger.info(f"Contract not found in OTC, trying OES for code={code}")
                try:
                    contract = api.Contracts.Stocks.OES[code]
                    market = "OES"
                except AttributeError:
                    logger.info("OES market not supported or accessible in this context")

            if contract is None:
                error_msg = f"Contract not found for code={code} in TSE, OTC, or OES"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        elif type_ == "futures":
            # 查詢期貨合約
            logger.info(f"Fetching futures contract for code={code}")
            # 嘗試直接使用 code 查找
            contract = api.Contracts.Futures[code]
            market = "Futures"

            # 如果找不到，嘗試列出所有期貨合約進行診斷
            if contract is None:
                logger.info(f"Contract not found for code={code}, listing available futures contracts")
                available_futures = {c.code: c.__dict__ for c in list(api.Contracts.Futures) if hasattr(c, 'code')}
                logger.info(f"Available futures contracts: {json.dumps(available_futures, default=str)}")
                error_msg = f"Futures contract not found for code={code}"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        elif type_ == "options":
            # 查詢選擇權合約
            logger.info(f"Fetching options contract for code={code}")
            contract = api.Contracts.Options[code]
            market = "Options"

            if contract is None:
                error_msg = f"Options contract not found for code={code}"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        elif type_ == "index":
            # 查詢指數合約
            logger.info(f"Fetching index contract for code={code} from TSE")
            contract = api.Contracts.Indexs.TSE[code]
            market = "Index"

            if contract is None:
                error_msg = f"Index contract not found for code={code} in TSE"
                logger.error(error_msg)
                return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        else:
            error_msg = f"Unsupported type: {type_}. Supported types are: stock, futures, options, index"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

        # 返回商品檔資訊
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Contract fetched",
                "contract": {
                    "exchange": contract.exchange.value if hasattr(contract, 'exchange') else None,
                    "code": contract.code if hasattr(contract, 'code') else None,
                    "symbol": contract.symbol if hasattr(contract, 'symbol') else None,
                    "name": contract.name if hasattr(contract, 'name') else None,
                    "category": contract.category if hasattr(contract, 'category') else None,
                    "unit": contract.unit if hasattr(contract, 'unit') else None,
                    "limit_up": contract.limit_up if hasattr(contract, 'limit_up') else None,
                    "limit_down": contract.limit_down if hasattr(contract, 'limit_down') else None,
                    "reference": contract.reference if hasattr(contract, 'reference') else None,
                    "update_date": contract.update_date if hasattr(contract, 'update_date') else None,
                    "day_trade": contract.day_trade.value if hasattr(contract, 'day_trade') else None
                },
                "market": market,
                "type": type_
            }, default=str, ensure_ascii=False)
        }

    except KeyError as ke:
        error_msg = f"Contract not found for code={code} (type={type_}, KeyError: {str(ke)})"
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}
    except Exception as e:
        error_msg = f"Error in contract: {str(e)} (type={type_})"
        logger.error(error_msg)
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception traceback: {sys.exc_info()}")
        return {"statusCode": 500, "body": json.dumps({"error": error_msg}, ensure_ascii=False)}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
