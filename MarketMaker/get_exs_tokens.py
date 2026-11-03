import motor.motor_asyncio as motor
import aiohttp,aiohttp_socks
import asyncio
import diskcache as dc
import random
import json,gzip,pickle
from datetime import datetime, timezone
from urllib.parse import urlencode
import hmac
import base64
cache = dc.Cache('../tmp/cache')
base = motor.AsyncIOMotorClient("mongodb://mongo:27017", serverSelectionTimeoutMS=10000)
sem = asyncio.Semaphore(200)
ex_tokens = {}
proxies = [None]

class OKXClient:
    def __init__(self, api_key, secret_key, passphrase, base_url='https://www.okx.com'):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.base_url = base_url
        
    def _generate_timestamp(self):
        current_time = datetime.now(timezone.utc)
        return current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    def _sign(self, timestamp, method, request_path, body=''):
        message = f'{timestamp}{method}{request_path}{body}'
        mac = hmac.new(
            bytes(self.secret_key, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        return base64.b64encode(mac.digest()).decode('utf-8')

    def _get_headers(self, timestamp, sign):
        return {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': sign,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'
        }

    async def get_orders_history(self, inst_type='SPOT', limit=50, inst_id=''):
        endpoint = '/api/v5/trade/orders-history'
        params = {
            'instType': inst_type,
            'limit': limit
        }
        if inst_id:
            params['instId'] = inst_id
            
        query_string = '?' + urlencode(params)
        request_path = endpoint + query_string
        
        timestamp = self._generate_timestamp()
        sign = self._sign(timestamp, 'GET', request_path)
        headers = self._get_headers(timestamp, sign)
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=aiohttp_socks.ProxyConnector.from_url(random.choice(proxies))) as session:
            try:
                async with session.get(
                    url=f"{self.base_url}{request_path}",
                    headers=headers
                ) as response:
                    if response.status == 200:
                        return await response.json()
            except Exception as e:
                return f"Error occurred: {str(e)}"


tokens = base.renderBase.tradePairs
url = {'MEXC Global':'https://api.mexc.com/api/v3/trades?limit=50&symbol=',
       'OKX':'https://www.okx.com/api/v5/trade/orders-history?instType=SPOT&limit=50&instId=',
       'HTX':'https://api.huobi.pro/market/history/trade?size=50&symbol=',
       'Kucoin':'https://api.kucoin.com/api/v1/market/histories?limit=50&symbol=',
       'Gate':'https://api.gateio.ws/api/v4/spot/trades?limit=50&currency_pair=',
       'BingX':'https://open-api.bingx.com/openApi/spot/v1/market/trades?limit=50&symbol=',
       'Bybit':'https://api.bybit.com/v5/market/recent-trade?category=spot&limit=50&symbol=',
       'Bitget':'https://api.bitget.com/api/v2/spot/market/fills?limit=50&symbol=',
       'Bitmart':'https://api-cloud.bitmart.com/spot/quotation/v3/trades?limit=50&symbol=',
       'CoinEx':'https://api.coinex.com/v2/spot/deals?limit=50&market='}

def sign(message, secret):
        mac = hmac.new(bytes(secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        return base64.b64encode(d).decode()


async def __main__():
    results = {}
    async def collect_trades(token:str,exchange:str):
                async with sem:
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=aiohttp_socks.ProxyConnector.from_url(random.choice(proxies))) as client:
                        try:
                            if token.upper().endswith('USDT'):
                                if not exchange=='OKX':
                                    response = await client.get(f'{url[exchange]}{token}')
                                    if response.status == 200:
                                        result = await response.json()
                                        end_arr = []
                                        match exchange:
                                            case 'MEXC Global':
                                                for obj in result:
                                                    trade_type = 'buy' if obj['tradeType']=='BID' else 'sell'
                                                    end_arr.append([float(obj['price']),float(obj['qty']),obj['time'],trade_type])
                                            case 'HTX':
                                                if result['status']=='error':
                                                    return
                                                for obj in result['data']:
                                                    end_arr.append([float(obj['data'][0]['price']),float(obj['data'][0]['amount']),obj['data'][0]['ts'],obj['data'][0]['direction'].lower()])
                                            case 'Gate':
                                                for obj in result:
                                                    end_arr.append([float(obj['price']),float(obj['amount']),obj['create_time'],obj['side'].lower()])
                                            case 'BingX':
                                                if result['code']==100204:
                                                    return
                                                for obj in result['data']:
                                                    trade_type = 'buy' if obj['buyerMaker'] else 'sell'
                                                    end_arr.append([float(obj['price']),float(obj['qty']),obj['time'],trade_type])
                                            case 'Bitget':
                                                for obj in result['data']:
                                                    end_arr.append([float(obj['price']),float(obj['size']),obj['ts'],obj['side'].lower()])
                                            case 'Bybit':
                                                if result['retCode']==10001:
                                                    return
                                                for obj in result['result']['list']:
                                                    end_arr.append([float(obj['price']),float(obj['size']),obj['time'],obj['side'].lower()])
                                            case 'Kucoin':
                                                for obj in result['data']:
                                                    end_arr.append([float(obj['price']),float(obj['size']),obj['time'],obj['side'].lower()])
                                            case 'Bitmart':
                                                for obj in result['data']:
                                                    end_arr.append([float(obj[2]),float(obj[3]),obj[1],obj[4].lower()])
                                            case 'CoinEx':
                                                if result['code']==4004:
                                                    return
                                                for obj in result['data']:
                                                    end_arr.append([float(obj['price']),float(obj['amount']),obj['created_at'],obj['side'].lower()])
                                            case 'OKX':
                                                pass
                                        
                                        if len(end_arr)==0:
                                            return
                                        
                                        token = {
                                                'OKX': token.upper().replace('-USDT', '/USDT'),
                                                'Kucoin': token.upper().replace('-USDT', '/USDT'),
                                                'Gate': token.upper().replace('_USDT', '/USDT'),
                                                'MEXC Global': token.upper().replace('USDT', '/USDT'),
                                                'Bitget': token.upper().replace('USDT', '/USDT'),
                                                'Bybit': token.upper().replace('USDT', '/USDT'),
                                                'BingX': token.upper().replace('-USDT', '/USDT'),
                                                'HTX': token.upper().replace('USDT', '/USDT'),
                                                'CoinEx': token.upper().replace('USDT', '/USDT'),
                                                'Bitmart': token.upper().replace('_USDT', '/USDT')
                                            }[exchange]
                                        if token not in results.keys():
                                            results[token]={'OKX':None,
                                                            'Kucoin':None,
                                                            'Gate':None, 
                                                            'MEXC Global':None, 
                                                            'Bitget':None,
                                                            'Bybit':None, 
                                                            'BingX':None, 
                                                            'HTX':None, 
                                                            'CoinEx':None,
                                                            'Bitmart':None}
                                            
                                            results[token][exchange]=end_arr
                                        else:
                                            results[token][exchange]=end_arr
                                else:
                                    pass
                                    # client = OKXClient(
                                    #     api_key='',
                                    #     secret_key='',
                                    #     passphrase=''
                                    # )
                                    # result = await client.get_orders_history(inst_id=token)
                                    # print(123)
                            
                        except Exception as e :
                            print(e)
                            pass
    
    all_tokens = await tokens.find().to_list(length=None)
    collect_tasks = []
    for exchange in all_tokens:
        for token in exchange['tokens']:
            task = asyncio.create_task(collect_trades(token,exchange['id']))
            collect_tasks.append(task)

    await asyncio.gather(*collect_tasks)
    print(len(results))
    

asyncio.run(__main__())
            

