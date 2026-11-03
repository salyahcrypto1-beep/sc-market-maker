from portable.access import pair_user
import asyncio
from datetime import datetime, timedelta
import logging
from typing import Dict, List
import gzip
import pickle
import sys
import os
from collections import OrderedDict, defaultdict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import telebot
from telebot.async_telebot import AsyncTeleBot
import motor.motor_asyncio as motor
import math  # Added for pagination calculations
from pymemcache.client import base
import aiohttp_socks
import aiohttp
import random
import json
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient


sem = asyncio.Semaphore(100)  # Разрешить 3 одновременных доступа

sys.path.append('/app/newbot/exchangeMethods')
sys.path.append('/app/newbot/exchangeMethods/manage')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from exchanges import exchanges

# Инициализация логирования
os.makedirs('logs', exist_ok=True)
logging.basicConfig(filename='logs/process_main.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

#BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "") - test vers
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("OWNER_CHAT_ID", "")

cache = base.Client(('memcached', 11211))

base_db = motor.AsyncIOMotorClient("mongodb://mongo:27017", serverSelectionTimeoutMS=10000)

async def looper(func, sem, name):
    # Раньше здесь был `while param is None or param == {}` — бесконечный ретрай.
    # Одна нерабочая биржа (Bybit 403 из дата-центра или мёртвый прокси)
    # навсегда подвешивала fetch_ticker_with_timing, из-за чего asyncio.gather
    # в detect_mm_signals не завершался, final_signals не обновлялся и бот
    # показывал «Нет активных ММ». Теперь — ограниченные попытки + таймаут на
    # попытку; недоступная биржа просто пропускается на этот цикл.
    for attempt in range(2):
        try:
            param = await asyncio.wait_for(func(sem), timeout=20)
        except asyncio.TimeoutError:
            print(f'{name}: timeout (попытка {attempt + 1}/2)')
            continue
        except Exception as e:
            print(f'{name}: ошибка {e} (попытка {attempt + 1}/2)')
            continue
        if param:
            return param, name
        await asyncio.sleep(1)
    print(f'{name}: биржа недоступна, пропускаем цикл')
    return {}, name

async def fetch_ticker_with_timing(exchange, sem):
    start_time = datetime.now()
    result = await looper(exchange.fetchTickers, sem, exchange.name)
    end_time = datetime.now()
    return {
        'data': result,
        'exchange': exchange.name,
        'time': end_time - start_time
    }

class DataTokensCollector:
    def __init__(self):
        self.allExchanges = ['Kucoin', 'Gate', 'MEXC', 'Bitget', 'Bybit', 'BingX', 'HTX', 'CoinEx', 'Bitmart']
        self.results = defaultdict(lambda: defaultdict(list))
        self.local_orderbooks = {}
        self.tokens_exs = {}
        self.globalBlackList: List[str] = []
        

        self.client = base_db
        self.db_renderBase = self.client.renderBase
        self.scheduler = AsyncIOScheduler(timezone='UTC')
        self.black_list = self.client.renderBase.black_list

        self.five_minute_data = {}
        self.active_mm_signals = {}
        self.users = []
        self.mm_users = []
        self.mm_users_ids = []
        self.user_pages = {}
        self.current_signals = defaultdict(list)
        self.now_signals = {}
        self.final_signals = {}
        self.user_filter_mode = {}
        self.user_search_query = {}

        self.active_users = []
        self.local_black_list = ['FORM/USDT', 'TALK/USDT']


        self.bot = AsyncTeleBot(BOT_TOKEN)

        self.bot.register_message_handler(self.start, commands=['start'])
        

        self.bot.register_message_handler(self.active_mm_list, func=lambda message: message.text == 'Активные Ⓜ️Ⓜ️')
        self.bot.register_message_handler(self.monitoring_menu, func=lambda message: message.text == 'Мониторинг 💻')
        self.bot.register_message_handler(self.show_favorites, func=lambda message: message.text == 'Избранное ⭐')
        self.bot.register_callback_query_handler(self.show_full_message, func=lambda call: call.data.startswith('show_'))
        self.bot.register_callback_query_handler(self.navigate_pages, func=lambda call: call.data.startswith('page_'))  # Handler for navigation
        self.bot.register_callback_query_handler(self.save_delete_callback, func=lambda call: call.data.startswith('save_') or call.data.startswith('delete_'))
        self.bot.register_callback_query_handler(self.monitoring_callback, func=lambda call: call.data in ['monitoring_on', 'monitoring_off'])
        self.bot.register_callback_query_handler(self.navigate_favorites, func=lambda call: call.data in ['fav_prev', 'fav_next'])
        self.bot.register_callback_query_handler(self.delete_favorite, func=lambda call: call.data.startswith('deletefav_'))
        self.bot.register_callback_query_handler(self.filter_mm_list, lambda call: call.data.startswith('filter_'))
        self.bot.register_callback_query_handler(lambda call: self.bot.answer_callback_query(call.id), func=lambda call: call.data == 'noop')
        self.bot.register_callback_query_handler(self.update_signal, func=lambda call: call.data.startswith('update_'))
        self.bot.register_callback_query_handler(self.spread_limit_callback, func=lambda call: call.data.startswith('spreadCall_'))
        self.bot.register_callback_query_handler(self.handle_spread_minus, lambda c: c.data.startswith('spread_minus_'))
        self.bot.register_callback_query_handler(self.handle_spread_plus, lambda c: c.data.startswith('spread_plus_'))
        self.bot.register_callback_query_handler(self.add_token_callback, func=lambda call: call.data.startswith('add_token'))
        self.bot.register_message_handler(self.show_blacklist, func=lambda message: message.text == 'Черный список 🚫')
        self.bot.register_callback_query_handler(self.navigate_blacklist, func=lambda call: call.data in ['blacklist_prev', 'blacklist_next'])
        self.bot.register_callback_query_handler(self.delete_blacklisted, func=lambda call: call.data.startswith('deleteblack_'))
        self.bot.register_callback_query_handler(self.add_blacklist_token_callback, func=lambda call: call.data == 'add_blacklist_token')

        self.bot.register_message_handler(
            self.text_handler,
            content_types=['text']
        )
        self.bot.register_callback_query_handler(
            self.search_mm_list, 
            func=lambda call: call.data == 'search'
        )
        
        self.bot.register_callback_query_handler(
            self.clear_search, 
            func=lambda call: call.data == 'clear_search'
        )


        self.waiting_for_token_input = {}
        self.waiting_for_search_input = {}
        self.active_mm_message_id = {}
        self.waiting_for_blacklist_input = {}



        # Структуры для хранения истории стаканов и сделок
        self.orderbook_history = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))  # exchange -> symbol -> type (asks/bids) -> list of snapshots
        self.trades_history = defaultdict(lambda: defaultdict(list))  # exchange -> symbol -> list of snapshots

        self.potential_market_makers = {}
        self.mm_list = defaultdict(list)

        self.scheduler.add_job(
            self.set_current_users,
            'interval',
            seconds=10
        )

        self.scheduler.start()

        self.proxies = [None]


    async def text_handler(self, message):
        await self.handle_text(message)

    async def get_recent_trades(self, exchange, symbol, lookback_period):
        """Получение недавних торгов для конкретной биржи и символа за указанный период"""
        now = datetime.now().timestamp()
        cutoff_time = now - lookback_period
        
        recent_trades = []

        last_trades = await self.collect_trades_all_exs(symbol, exchange)
        
        if not (last_trades is None):
            for trade in last_trades:
                trade_time = int(trade['time']) / 1000  # перевод в секунды
                if trade_time >= cutoff_time:
                    recent_trades.append(trade)
        
        return recent_trades
    
    async def check_trade_frequency(self, exchange, symbol):
        """Определение частоты торгов для биржи и символа"""
        # Получаем торги за последний час (3600 секунд)
        trades_last_hour = await self.get_recent_trades(exchange, symbol, 3600)
        
        # Получаем торги за последние 10 минут (600 секунд)
        trades_last_10min = await self.get_recent_trades(exchange, symbol, 600)
        
        # Определяем частоту на основе количества торгов
        if len(trades_last_10min) >= 10:  # Высокая активность
            return 1
        elif len(trades_last_hour) >= 10:  # Средняя активность
            return 2
        else:  # Низкая активность
            return 3


    async def get_all_symbols_data(self) -> Dict[str, str]:
        all_symbols_data =  await self.db_renderBase.AllSymbolsData.find().to_list(length=None)
        
        return {item['symbol']: item['currency'] for item in all_symbols_data}
    
    async def update_local_orderbooks(self):
        while True:
            print("Обновление локальных ордербуков из кэша")
            try:
                for exchange in self.allExchanges:
                    key = f'orderbook_{exchange}_full_snapshot'
                    value = cache.get(key)
                    
                    if value:
                        data = pickle.loads(gzip.decompress(value))
                        self.local_orderbooks[exchange] = data

                await asyncio.sleep(5)
            except Exception as e:
                #print(f"Ошибка при обновлении локальных ордербуков: {e}")
                await asyncio.sleep(5) 

    async def get_socket_orderbook(self, exchange, symbol):
        """Получение ордербука для заданной биржи и токена"""
        try:
            orderbook = self.local_orderbooks.get(exchange, {}).get(symbol)
            if orderbook:
                return {
                    'bids': list(orderbook['bids'].items()) if isinstance(orderbook['bids'], OrderedDict) else orderbook['bids'],
                    'asks': list(orderbook['asks'].items()) if isinstance(orderbook['asks'], OrderedDict) else orderbook['asks'],
                    'lastUpdate': orderbook['lastUpdate']
                }
            
            print('Есть сигнал, нет сокет стакана!')
            return None
        except Exception as e:
            print('Есть сигнал, нет сокет стакана!', e)
            return None

    async def update_mm_list(self):
        last_save_time = datetime.now()
        while True:
            print("Обновление сигналов из кэша")
            try:
                key = f'mm_symbols'
                key2 = f'mm_symbols_active'
                data1 = cache.get(key)
                data2 = cache.get(key2)
                print(data1, data2)
                
                if data1:
                    obj1 = pickle.loads(gzip.decompress(data1))
                    try:
                        for key, value in obj1.items():
                            for item in value:
                                if item not in self.mm_list[key]:  # Проверка на дубликаты
                                    self.mm_list[key].append(item)
                    except Exception as e:
                        print(f"Ошибка obj1: {e}")
                        pass
                    print('Обновлен список маркет-мекеров', len(self.mm_list))
                
                if data2:
                    obj2 = pickle.loads(gzip.decompress(data2))
                    try:
                        for key, value in obj2.items():
                            for item in value:
                                if item not in self.mm_list[key]:  # Проверка на дубликаты
                                    self.mm_list[key].append(item)
                    except Exception as e:
                        print(f"Ошибка obj2: {e}")
                        pass
                    print('Обновлен список маркет-мекеров', len(self.mm_list))
                
                # Добавляем новую проверку: получаем актуальные данные из MongoDB
                try:
                    # Получаем актуальный объект токенов из базы MongoDB
                    mongo_data = await base_db.userdata.mm_data.find_one({'_id': 'mm_list'})
                    if mongo_data and 'data' in mongo_data:
                        mongo_mm_list = mongo_data['data']
                        
                        # Проверяем есть ли в MongoDB токены (ключи), которых нет в self.mm_list
                        for key, value in mongo_mm_list.items():
                            # Если ключа нет в self.mm_list, добавляем его
                            if key not in self.mm_list:
                                self.mm_list[key] = value
                            # Если ключ есть, но в MongoDB содержатся элементы, которых нет в self.mm_list
                            else:
                                for item in value:
                                    if item not in self.mm_list[key]:
                                        self.mm_list[key].append(item)
                        
                        print('Обновлен список маркет-мекеров из MongoDB', len(self.mm_list))
                except Exception as e:
                    print(f"Ошибка при получении данных из MongoDB: {e}")
                
                current_time = datetime.now()
                if (current_time - last_save_time).total_seconds() >= 300:
                    await self.save_mm_list_to_mongodb()
                    last_save_time = current_time
                    
                await asyncio.sleep(60)
            except Exception as e:
                print(f"Ошибка update_mm_list: {e}")
                await asyncio.sleep(60)


    async def save_mm_list_to_mongodb(self):
        try:
            print("Сохранение mm_list в MongoDB...")
            
            # Получение существующего документа из MongoDB (если есть)
            existing_data = await base_db.userdata.mm_data.find_one({'_id': 'mm_list'})
            
            if existing_data:
                mongo_mm_list = existing_data.get('data', {})
                
                # Проверяем, что обновление не уменьшит количество ключей
                if len(self.mm_list) < len(mongo_mm_list):
                    print("Количество ключей в self.mm_list меньше, чем в базе. Отмена обновления.")
                    return
                
                # Обновляем существующие данные
                updated_data = mongo_mm_list.copy()
                
                for key, value in self.mm_list.items():
                    # Добавляем новые ключи
                    if key not in updated_data:
                        updated_data[key] = value
                    # Обновляем значения, если в self.mm_list их больше
                    elif len(value) > len(updated_data[key]):
                        updated_data[key] = value
                
                # Обновляем документ в MongoDB
                await base_db.userdata.mm_data.update_one(
                    {'_id': 'mm_list'},
                    {'$set': {'data': updated_data}}
                )
                
            else:
                # Создаем новый документ, если не существует
                await base_db.userdata.mm_data.insert_one({
                    '_id': 'mm_list',
                    'data': self.mm_list
                })
                
            print("mm_list успешно сохранен в MongoDB")
            
        except Exception as e:
            print(f"Ошибка при сохранении mm_list в MongoDB: {e}")
  
    
    async def process_exchanges(self, symbol, exchanges_detected, exBuy, exSell, tickers, allSymbolsData):
        try:
            if (exBuy == 'CoinEx' and exSell == 'Bitmart') or (exBuy == 'Bitmart' and exSell == 'CoinEx') or exBuy == 'OKX' or exSell == 'OKX' or symbol in self.local_black_list:
                return None
            

            isNetworkBoth = False
            networkBoth = None
            feeVal = 0

            try:
                if exBuy == 'MEXC':
                    exBuy = 'MEXC Global'
                if exSell == 'MEXC':
                    exSell = 'MEXC Global'
                ex_networks_asks = allSymbolsData[symbol][exBuy]['networks']
                ex_networks_bids = allSymbolsData[symbol][exSell]['networks']
                
                for network in ex_networks_asks.keys():
                    if network in ex_networks_bids.keys():
                        withdraw = ex_networks_asks.get(network, {}).get('withdraw', 0)
                        deposit = ex_networks_bids.get(network, {}).get('deposit', 0)
                        if withdraw and deposit and ex_networks_asks.get(network, {}).get('fee') is not None:
                            isNetworkBoth = True
                            networkBoth = network
                            feeVal = ex_networks_asks.get(network, {}).get('fee', 0)
            except Exception as e:
                #print(f"Error process_exchanges 1 {e}")
                pass


            if isNetworkBoth:
                symbol_formatted_min = {
                    'OKX': symbol.replace('/', '-'),
                    'Kucoin': symbol.replace('/', '-'),
                    'Gate': symbol.replace('/', '_'),
                    'MEXC Global': symbol.replace('/', ''),
                    'Bitget': symbol.replace('/', ''),
                    'Bybit': symbol.replace('/', ''),
                    'BingX': symbol.replace('/', '-'),
                    'HTX': symbol.replace('/', '').lower(),
                    'CoinEx': symbol.replace('/', ''),
                    'Bitmart': symbol.replace('/', '_'),
                }[exBuy]
                symbol_formatted_max = {
                    'OKX': symbol.replace('/', '-'),
                    'Kucoin': symbol.replace('/', '-'),
                    'Gate': symbol.replace('/', '_'),
                    'MEXC Global': symbol.replace('/', ''),
                    'Bitget': symbol.replace('/', ''),
                    'Bybit': symbol.replace('/', ''),
                    'BingX': symbol.replace('/', '-'),
                    'HTX': symbol.replace('/', '').lower(),
                    'CoinEx': symbol.replace('/', ''),
                    'Bitmart': symbol.replace('/', '_')
                }[exSell]

                
                buy_ticker_start = datetime.now()
                minAsksSTR = tickers.get(exBuy, {}).get(symbol_formatted_min, {}).get('ask', 0) if tickers.get(exBuy, {}).get(symbol_formatted_min, {}).get('ask') is not None else None
                buy_ticker_time = datetime.now() - buy_ticker_start
                #print(f"Время получения тикера {exBuy} {symbol_formatted_min}: {buy_ticker_time.total_seconds():.3f} сек")
                
                
                sell_ticker_start = datetime.now()
                maxBidSTR = tickers.get(exSell, {}).get(symbol_formatted_max, {}).get('bid', 0) if tickers.get(exSell, {}).get(symbol_formatted_max, {}).get('bid') is not None else None
                sell_ticker_time = datetime.now() - sell_ticker_start

                if symbol in self.globalBlackList:
                    return

                if exBuy == 'MEXC Global':
                    exBuy = 'MEXC'
                if exSell == 'MEXC Global':
                    exSell = 'MEXC'

                if minAsksSTR == "" or maxBidSTR == "":
                    return
                
                if minAsksSTR is None or maxBidSTR is None:
                    return
                    
                minAsks = float(minAsksSTR)
                maxBid = float(maxBidSTR)
                

                if maxBid <= 0 or minAsks <= 0:
                    return
                
                
                
                profit = maxBid / minAsks * 100 - 100

                if profit > 1:

                    orderbook_min = await self.get_socket_orderbook(exBuy, symbol_formatted_min)
                    orderbook_max = await self.get_socket_orderbook(exSell, symbol_formatted_max)

                    has_price_gap_min = False
                    has_price_gap_max = False

                    total_volume_above_ask = 0

                    if orderbook_max:
                        # Вычисляем общий объем в ордерах bids с биржи max с ценой выше ask0_price
                        bids_orders_max = orderbook_max.get('bids', [])
                        
                        for bid in bids_orders_max:
                            bid_price = float(bid[0])
                            bid_quantity = float(bid[1])
                            
                            # Проверяем, что цена бида выше, чем ask0_price
                            if bid_price > minAsks:
                                # Вычисляем объем как цену ордера, умноженную на количество токенов
                                volume = bid_price * bid_quantity
                                total_volume_above_ask += volume

                    
                    if orderbook_min and orderbook_max:
                        asks_orders_min = orderbook_min.get('asks', [])
                        bids_orders_min = orderbook_min.get('bids', [])

                        asks_orders_max = orderbook_max.get('asks', [])
                        bids_orders_max = orderbook_max.get('bids', [])

                        if len(asks_orders_min) >= 2 :
                            ask0_price = float(asks_orders_min[0][0])
                            ask1_price = float(asks_orders_min[1][0])
                            
                            # Вычисляем разницу между 2-м ордером asks и 2-м ордером bids
                            price_gap = (ask1_price / ask0_price * 100 - 100) if ask1_price  > 0 and ask0_price > 0 else 0
                            
                            # Если разница больше 10%, устанавливаем has_price_gap в True
                            has_price_gap_min = price_gap > 5
                        
                        if len(bids_orders_max) >= 2 :
                            bids0_price = float(bids_orders_max[0][0])
                            bids1_price = float(bids_orders_max[1][0])
                            
                            # Вычисляем разницу между 2-м ордером asks и 2-м ордером bids
                            price_gap = (bids0_price / bids1_price * 100 - 100) if bids0_price  > 0 and bids1_price > 0 else 0
                            
                            # Если разница больше 10%, устанавливаем has_price_gap в True
                            has_price_gap_max = price_gap > 10

                        

                    buy_frequency = await self.check_trade_frequency(exBuy, symbol)
    
                    sell_frequency = await self.check_trade_frequency(exSell, symbol)

                    print(symbol, exBuy, buy_frequency, exSell, sell_frequency)

                    feeVolDol = feeVal * minAsks
                    signal_arr = [symbol, round(profit, 2), exBuy, exSell, exchanges_detected, networkBoth, has_price_gap_min, has_price_gap_max, feeVal, feeVolDol, buy_frequency, sell_frequency, total_volume_above_ask]
                    self.now_signals[f'{symbol}|{exBuy}|{exSell}|{networkBoth}'] = signal_arr
                    #await self.bot.send_message(CHAT_ID, token_message, parse_mode='Markdown', disable_web_page_preview=True)

        except Exception as e:
            print(f"Error process_exchanges {e}")
            pass

    async def process_symbol(self, symbol, exchanges_detected, tickers, allSymbolsData):
        tasks = [asyncio.create_task(self.process_exchanges(symbol, exchanges_detected, exBuy, exSell, tickers, allSymbolsData)) for exBuy in self.allExchanges for exSell in self.allExchanges if exBuy != exSell and (exBuy in exchanges_detected or exSell in exchanges_detected)]
        await asyncio.gather(*tasks)
    

    async def detect_mm_signals(self):
        while True:
            try:
                self.now_signals = {}
                if len(self.mm_list) > 0:

                    self.globalBlackList = await self.black_list.find().to_list(length=None)
                    self.globalBlackList = self.globalBlackList[0]['list']
                    print(f'Black list collected. Len: {len(self.globalBlackList)}')

                    allSymbolsData = await self.get_all_symbols_data()

                    tasks = [asyncio.create_task(fetch_ticker_with_timing(ex, sem)) for ex in exchanges]
                    tickers_data = await asyncio.gather(*tasks)

                    tickers = {ticker['data'][1]: ticker['data'][0] for ticker in tickers_data}
                    
                    # Вывод времени для каждой биржи
                    for ticker_info in tickers_data:
                        print(f"Exchange {ticker_info['exchange']}: {ticker_info['time']}")
                    
                    # Преобразование в нужный формат для дальнейшей обработки
                    tickers = {ticker['data'][1]: ticker['data'][0] for ticker in tickers_data}

                    tasks = [asyncio.create_task(self.process_symbol(symbol, exchanges_detected, tickers, allSymbolsData)) for symbol, exchanges_detected in self.mm_list.items()]
                    await asyncio.gather(*tasks)

                    if len(self.now_signals)>0:
                        self.final_signals = self.now_signals
                    
                else:
                    await asyncio.sleep(60)


            except Exception as e:
                print(f"Error detect_mm_signals {e}")
                await asyncio.sleep(1)


    async def save_delete_callback(self, call):
        try:
            user_id = call.message.chat.id
            data = call.data
            if data.startswith('save_'):
                token_key = data[5:]
                # Add token_key to user's saveList
                user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
                save_list = user_model.get('saveList', [])
                if token_key not in save_list:
                    token_name = token_key.split('|')[0] if '|' in token_key else token_key
                    if token_name not in save_list:
                        save_list.append(token_name)
                    await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'saveList': save_list}})
                    await base_db.userdata.user.update_one({'id': int(user_id)}, {'$set': {'save_mm_signals': save_list}})
                    # Update the button to 'Удалить'
                    markup = telebot.types.InlineKeyboardMarkup()
                    button = telebot.types.InlineKeyboardButton('Удалить ❌', callback_data=f'delete_{token_key}')

                    markup.add(button)
                    await self.bot.edit_message_reply_markup(user_id, call.message.message_id, reply_markup=markup)
                await self.bot.answer_callback_query(call.id, text='Токен сохранен.')
            elif data.startswith('delete_'):
                token_key = data[7:]
                # Remove token_key from user's saveList
                user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
                save_list = user_model.get('saveList', [])
                if token_key in save_list:
                    token_name = token_key.split('|')[0] if '|' in token_key else token_key
                    if token_name in save_list:
                        save_list.remove(token_name)
                    await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'saveList': save_list}})
                    await base_db.userdata.user.update_one({'id': int(user_id)}, {'$set': {'save_mm_signals': save_list}})
                    # Update the button to 'Отслеживать'
                    markup = telebot.types.InlineKeyboardMarkup()
                    button = telebot.types.InlineKeyboardButton('Отслеживать ✅', callback_data=f'save_{token_key}')
                    
                    markup.add(button)
                    await self.bot.edit_message_reply_markup(user_id, call.message.message_id, reply_markup=markup)
                await self.bot.answer_callback_query(call.id, text='Токен удален.')
        except Exception as e:
            print(f"Ошибка в save_delete_callback: {e}")
            await self.bot.answer_callback_query(call.id, text='Произошла ошибка.', show_alert=True)

    async def show_full_message(self, call):
        try:
            key = call.data[5:]  # Remove 'show_'
            if key in self.final_signals:
                user_id = call.message.chat.id
                user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
                save_list = user_model.get('saveList', [])
                value = self.final_signals[key]

                symbol = value[0]

                symbol_formatted_min = {
                    'OKX': symbol.replace('/', '-'),
                    'Kucoin': symbol.replace('/', '-'),
                    'Gate': symbol.replace('/', '_'),
                    'MEXC': symbol.replace('/', ''),
                    'Bitget': symbol.replace('/', ''),
                    'Bybit': symbol.replace('/', ''),
                    'BingX': symbol.replace('/', ''),
                    'HTX': symbol.replace('/', '').lower(),
                    'CoinEx': symbol.replace('/', ''),
                    'Bitmart': symbol.replace('/', '_'),
                }[value[2]]
                symbol_formatted_max = {
                    'OKX': symbol.replace('/', '-'),
                    'Kucoin': symbol.replace('/', '-'),
                    'Gate': symbol.replace('/', '_'),
                    'MEXC': symbol.replace('/', ''),
                    'Bitget': symbol.replace('/', ''),
                    'Bybit': symbol.replace('/', ''),
                    'BingX': symbol.replace('/', ''),
                    'HTX': symbol.replace('/', '').lower(),
                    'CoinEx': symbol.replace('/', ''),
                    'Bitmart': symbol.replace('/', '_')
                }[value[3]]


                link_formatted_min = {
                                'Kucoin': f"https://www.kucoin.com/ru/trade/{symbol_formatted_min}",
                                'Gate': f"https://www.gate.io/ru/trade/{symbol_formatted_min}",
                                'BingX': f"https://bingx.com/ru-ru/spot/{symbol_formatted_min}/",
                                'HTX': f"https://www.htx.com/ru-ru/trade/{symbol_formatted_min.replace('usdt', '_usdt')}?type=spot",
                                'Bitmart': f"https://www.bitmart.com/trade/ru-RU?symbol={symbol_formatted_min}",
                                'MEXC': f"https://www.mexc.com/ru-RU/exchange/{symbol_formatted_min.replace('USDT', '_USDT')}",
                                'Bitget': f"https://www.bitget.com/ru/spot/{symbol_formatted_min}",
                                'Bybit': f"https://www.bybit.com/en/trade/spot/{symbol_formatted_min.replace('USDT', '')}/USDT",
                                'CoinEx': f"https://www.coinex.com/ru/exchange/{symbol_formatted_min.replace('USDT', '-USDT')}",
                            }[value[2]]
                link_formatted_max = {
                                'Kucoin': f"https://www.kucoin.com/ru/trade/{symbol_formatted_max}",
                                'Gate': f"https://www.gate.io/ru/trade/{symbol_formatted_max}",
                                'BingX': f"https://bingx.com/ru-ru/spot/{symbol_formatted_max}/",
                                'HTX': f"https://www.htx.com/ru-ru/trade/{symbol_formatted_max.replace('usdt', '_usdt')}?type=spot",
                                'Bitmart': f"https://www.bitmart.com/trade/ru-RU?symbol={symbol_formatted_max}",
                                'MEXC': f"https://www.mexc.com/ru-RU/exchange/{symbol_formatted_max.replace('USDT', '_USDT')}",
                                'Bitget': f"https://www.bitget.com/ru/spot/{symbol_formatted_max}",
                                'Bybit': f"https://www.bybit.com/en/trade/spot/{symbol_formatted_max.replace('USDT', '')}/USDT",
                                'CoinEx': f"https://www.coinex.com/ru/exchange/{symbol_formatted_max.replace('USDT', '-USDT')}",
                            }[value[3]]

                feeVol = round(value[8],2)
                feeVolDol = round(value[9],2)

                buy_licuid = '🟢' if int(value[10]) == 1 else '🟡' if int(value[10]) == 2 else '🔴'
                sell_licuid = '🟢' if int(value[11]) == 1 else '🟡' if int(value[11]) == 2 else '🔴'
                
                message_text = (
                        f"Токен: {value[0]}\n\n"
                        f"Курсовая разница: {value[1]}%\n\n"
                        f"Биржа покупки: [{value[2]}]({link_formatted_min}) {buy_licuid} { 'Ⓜ️Ⓜ️' if value[2] in value[4] else '' } {'↕️' if value[6] else ''}\n"
                        f"Биржа продажи: [{value[3]}]({link_formatted_max}) {sell_licuid} { 'Ⓜ️Ⓜ️' if value[3] in value[4] else '' } {'↕️' if value[7] else ''}\n\n"
                        f"Комиссия: {feeVolDol}$, {feeVol} | Сеть: {value[5]}\n\n"
                        f"{f'Разрыв более 5% на биржах: {value[2]}, {value[3]}' if value[6] and value[7] else f'Разрыв более 5% на бирже: {value[2]}' if value[6] else f'Разрыв более 5% цен на бирже: {value[3]}' if value[7] else ''}\n"
                        f"{f'Доступный объем: {round(value[12], 2)}$' if float(value[12]) > 0 else ''}"
                    )

                mm_ex = value[2] if value[2] in value[4] else value[3] if value[3] in value[4] else ""

                token_key = f'{value[0]}|{mm_ex}'

                if token_key in save_list:
                    button_text = 'Удалить ❌'
                    callback_data = f'delete_{token_key}'
                else:
                    button_text = 'Отслеживать ✅'
                    callback_data = f'save_{token_key}'

                markup = telebot.types.InlineKeyboardMarkup()
                button = telebot.types.InlineKeyboardButton(button_text, callback_data=callback_data)
                update_button = telebot.types.InlineKeyboardButton("Обновить", callback_data=f"update_{value[0]}|{value[2]}|{value[3]}|{value[5]}")
                markup.add(button, update_button)

                await self.bot.send_message(
                    user_id,
                    message_text,
                    parse_mode='Markdown',
                    disable_web_page_preview=True,
                    reply_markup=markup
                )
                await self.bot.answer_callback_query(call.id)
            else:
                await self.bot.answer_callback_query(
                    call.id,
                    text='Информация недоступна.',
                    show_alert=True
                )
        except Exception as e:
            print(f"Ошибка в show_full_message: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при получении информации.',
                show_alert=True
            )


    async def update_signal(self, call):
        try:
            # Извлекаем данные о токене и биржах из callback_data
            _, data = call.data.split('_', 1)
            symbol, exBuy, exSell, network = data.split('|')
            
            if exBuy == 'MEXC':
                exBuy = 'MEXC Global'
            if exSell == 'MEXC':
                exSell = 'MEXC Global'
            # Получаем актуальные данные с бирж
            result1 = {}
            result2 = {}
            for ex in exchanges:
                if ex.name == exBuy:
                    result1 = await fetch_ticker_with_timing(ex, sem)

                if ex.name == exSell:
                    result2 = await fetch_ticker_with_timing(ex, sem)
                
            tickers_buy = result1['data'][0]
            tickers_sell = result2['data'][0]

            # Форматируем символ для каждой биржи
            symbol_formatted_buy = {
                'OKX': symbol.replace('/', '-'),
                'Kucoin': symbol.replace('/', '-'),
                'Gate': symbol.replace('/', '_'),
                'MEXC': symbol.replace('/', ''),
                'MEXC Global': symbol.replace('/', ''),
                'Bitget': symbol.replace('/', ''),
                'Bybit': symbol.replace('/', ''),
                'BingX': symbol.replace('/', '-'),
                'HTX': symbol.replace('/', '').lower(),
                'CoinEx': symbol.replace('/', ''),
                'Bitmart': symbol.replace('/', '_')
            }[exBuy]
            
            symbol_formatted_sell = {
                'OKX': symbol.replace('/', '-'),
                'Kucoin': symbol.replace('/', '-'),
                'Gate': symbol.replace('/', '_'),
                'MEXC': symbol.replace('/', ''),
                'MEXC Global': symbol.replace('/', ''),
                'Bitget': symbol.replace('/', ''),
                'Bybit': symbol.replace('/', ''),
                'BingX': symbol.replace('/', '-'),
                'HTX': symbol.replace('/', '').lower(),
                'CoinEx': symbol.replace('/', ''),
                'Bitmart': symbol.replace('/', '_')
            }[exSell]
            
            # Получаем актуальные цены
            min_ask = tickers_buy.get(symbol_formatted_buy, {}).get('ask', 0)
            max_bid = tickers_sell.get(symbol_formatted_sell, {}).get('bid', 0)
            
            # Рассчитываем новую курсовую разницу и доступный объем
            spread = 0
            available_volume = 0
            
            if min_ask is not None and max_bid is not None and float(min_ask) > 0:
                # Преобразуем полученные значения в float
                if min_ask is not None and isinstance(min_ask, str):
                    min_ask = float(min_ask)
                if max_bid is not None and isinstance(max_bid, str):
                    max_bid = float(max_bid)

                # Рассчитываем новую курсовую разницу и доступный объем
                spread = 0
                available_volume = 0

                spread = round((max_bid / min_ask - 1) * 100, 2)
                
                # Получаем orderbook для расчета доступного объема
                orderbook_max = await self.get_socket_orderbook(exSell, symbol_formatted_sell)
                
                # Расчет доступного объема (если есть данные orderbook)
                if orderbook_max:
                    bids_orders_max = orderbook_max.get('bids', [])
                    total_volume_above_ask = 0
                    
                    for bid in bids_orders_max:
                        bid_price = float(bid[0])
                        bid_quantity = float(bid[1])
                        
                        if bid_price > min_ask:
                            volume = bid_price * bid_quantity
                            total_volume_above_ask += volume 
                    
                    available_volume = round(total_volume_above_ask, 2)
            
            # Получаем текущий текст сообщения и обновляем необходимые части
            current_message = call.message.text
            lines = current_message.split('\n\n')
            updated_lines = []
            have_none_msg = False
            for line in lines:
                if line.startswith("Курсовая разница:"):
                    spread_line = f"Курсовая разница: {spread}%"
                    updated_lines.append(spread_line)
                elif "❌ Неактуальный спред" in line:
                    have_none_msg = True
                    continue
                elif "Доступный объем:" in line:
                    updated_lines.append(f"Доступный объем: {available_volume}$")
                else:
                    updated_lines.append(line)


            if spread <= 0:
                if not (have_none_msg):
                    updated_message += "\n\n❌ Неактуальный спред"

            # Собираем обновленное сообщение
            updated_message = '\n\n'.join(filter(None, updated_lines))
            
            # Создаем новую клавиатуру (сохраняем старую структуру)
            markup = telebot.types.InlineKeyboardMarkup()

            if exBuy == 'MEXC Global':
                exBuy = 'MEXC'
            if exSell == 'MEXC Global':
                exSell = 'MEXC'
            
            # Проверяем, есть ли токен в отслеживаемых
            mm_ex = ""
            for line in lines:
                if "Ⓜ️Ⓜ️" in line:
                    if exBuy in line:
                        mm_ex = exBuy
                    elif exSell in line:
                        mm_ex = exSell
                    break
                    
            token_key = f'{symbol}|{mm_ex}'
            user_model = await base_db.userdata.mm_user.find_one({'id': int(call.from_user.id)})
            save_list = user_model.get('saveList', [])
            
            if token_key in save_list:
                button_text = 'Удалить ❌'
                callback_data = f'delete_{token_key}'
            else:
                button_text = 'Отслеживать ✅'
                callback_data = f'save_{token_key}' 
            
            button = telebot.types.InlineKeyboardButton(button_text, callback_data=callback_data)
            update_button = telebot.types.InlineKeyboardButton("Обновить", callback_data=f"update_{symbol}|{exBuy}|{exSell}|{network}")
            
            markup.add(button, update_button)
            
            # Редактируем сообщение
            await self.bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=updated_message,
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=markup
            )
            
            # Отображаем уведомление пользователю
            status_message = "Данные обновлены"
            if spread <= 0:
                status_message += " (спред неактуален)"
                
            await self.bot.answer_callback_query(call.id, text=status_message)
            
        except Exception as e:
            print(f"Ошибка в update_signal: {e}")
            await self.bot.answer_callback_query(call.id, text="Значения не изменились")



    async def navigate_pages(self, call):
        """Handler for pagination navigation buttons."""
        try:
            user_id = call.message.chat.id
            data = call.data.split('_')
            if len(data) == 2:
                action = data[1]
                current_page = self.user_pages.get(user_id, 1)
                total_items = len(self.final_signals)
                items_per_page = 10
                total_pages = math.ceil(total_items / items_per_page)
                if action == 'next':
                    if current_page < total_pages:
                        current_page += 1
                elif action == 'prev':
                    if current_page > 1:
                        current_page -= 1
                self.user_pages[user_id] = current_page
                await self.generate_active_mm_list(user_id, page=current_page, call=call, filter_mode=self.user_filter_mode.get(user_id, 1))

                

        except Exception as e:
            print(f"Ошибка в navigate_pages: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при навигации по страницам.',
                show_alert=True
            )

    async def navigate_favorites(self, call):
        try:
            user_id = call.message.chat.id
            action = call.data
            current_page = self.user_pages.get(user_id, 1)
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            total_items = len(user_model.get('saveList', []))
            items_per_page = 10
            total_pages = math.ceil(total_items / items_per_page)
            if action == 'fav_next':
                if current_page < total_pages:
                    current_page += 1
            elif action == 'fav_prev':
                if current_page > 1:
                    current_page -= 1
            self.user_pages[user_id] = current_page
            await self.generate_favorites_list(user_id, page=current_page, call=call)
        except Exception as e:
            print(f"Ошибка в navigate_favorites: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при навигации по страницам.',
                show_alert=True
            )

    async def delete_favorite(self, call):
        try:
            user_id = call.message.chat.id
            token = call.data[10:]  # Remove 'deletefav_'
            
            # Remove token from user's saveList
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            save_list = user_model.get('saveList', [])
            
            print('remove', token, save_list)
            if token in save_list:
                
                save_list.remove(token)
                await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'saveList': save_list}})
                await base_db.userdata.user.update_one({'id': int(user_id)}, {'$set': {'save_mm_signals': save_list}})
            
            # Refresh the favorites list
            current_page = self.user_pages.get(user_id, 1)
            await self.generate_favorites_list(user_id, page=current_page, call=call)
        except Exception as e:
            print(f"Ошибка в delete_favorite: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при удалении токена.',
                show_alert=True
            )


    async def active_mm_list(self, message):
        try:
            if str(message.chat.id) in self.users:
                self.user_pages[message.chat.id] = 1  # Initialize page number
                await self.generate_active_mm_list(message.chat.id, page=1, message=message, filter_mode=self.user_filter_mode.get(message.chat.id, 1))
            else:
                await self.bot.send_message(message.chat.id, 'У вас нет доступа')
        except Exception as e:
            print(f"Ошибка в active_mm_list: {e}")
            await self.bot.send_message(
                message.chat.id,
                'Произошла ошибка при получении списка активных ММ.'
            )

    async def filter_mm_list(self, call):
        """Handler for filtering buttons."""
        try:
            user_id = call.message.chat.id
            data = call.data.split('_')
            
            if len(data) == 2 and data[0] == 'filter':
                filter_mode = int(data[1])
                self.user_filter_mode = self.user_filter_mode or {}
                self.user_filter_mode[user_id] = filter_mode
                self.user_pages[user_id] = 1  # Сбрасываем на первую страницу при смене фильтра
                
                await self.generate_active_mm_list(user_id, page=1, call=call, filter_mode=filter_mode)
        except Exception as e:
            print(f"Ошибка в filter_mm_list: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при фильтрации списка.',
                show_alert=True
            )


    async def generate_active_mm_list(self, chat_id, page=1, message=None, call=None, filter_mode=1, search_query=None):
        """Generates the active MM list with pagination, filtering options and search."""
        try:
            # Get the user's saveList
            user_model = await base_db.userdata.mm_user.find_one({'id': int(chat_id)})
            save_list = user_model.get('saveList', [])
            black_list = user_model.get('blackList', [])

            # Создаем разметку для инлайн-клавиатуры
            markup = telebot.types.InlineKeyboardMarkup(row_width=1)
            now = datetime.now()
            to_remove = []

             # Применяем поисковый фильтр, если он указан
            filtered_signals = self.final_signals
            if search_query:
                search_query = search_query.upper()
                filtered_signals = {}
                print('123')
                for k, v in self.final_signals.items():
                    token_name = v[0].upper()
                    # Убираем суффикс "/USDT" для сравнения
                    base_token = token_name.split('/')[0] if '/' in token_name else token_name
                    if search_query in base_token and (base_token not in black_list and token_name not in black_list):
                        filtered_signals[k] = v
            else:
                filtered_signals = {}
                for k, v in self.final_signals.items():
                    token_name = v[0].upper()
                    # Убираем суффикс "/USDT" для сравнения
                    base_token = token_name.split('/')[0] if '/' in token_name else token_name
                    if (base_token not in black_list and token_name not in black_list):
                        filtered_signals[k] = v

            filtered_signals = {k: v for k, v in filtered_signals.items() if float(v[1]) > 0}
            total_items = len(filtered_signals)
            print('321', total_items)
            
            if total_items == 0:
                if search_query:
                    message_text = f'По запросу "{search_query}" ничего не найдено.'
                else:
                    message_text = 'Нет активных ММ.'
                    
                if call:
                    await self.bot.edit_message_text(
                        message_text,
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id
                    )
                    await self.bot.answer_callback_query(call.id)
                else:
                    await self.bot.send_message(chat_id, message_text)
                return

            # Сортировка в зависимости от выбранного режима фильтрации
            if filter_mode == 1:
                # По курсовой разнице (изначальный вариант)
                sorted_items = list(sorted(filtered_signals.items(), key=lambda item: item[1][1], reverse=True))
            elif filter_mode == 2:
                # По ликвидности
                def liquidity_rank(item):
                    buy_liquid = item[1][10]
                    sell_liquid = item[1][11]
                    return buy_liquid + sell_liquid
                sorted_items = list(sorted(filtered_signals.items(), key=liquidity_rank))
            elif filter_mode == 3:
                # По доступному объему (от большего к меньшему)
                sorted_items = list(sorted(filtered_signals.items(), key=lambda item: float(item[1][12]), reverse=True))
            elif filter_mode == 4:
                # % разница + ликвидность
                def combined_rank_percent_liquidity(item):
                    buy_liquid = item[1][10]
                    sell_liquid = item[1][11]
                    liquidity_score = buy_liquid + sell_liquid
                    percent_diff = item[1][1]
                    return (liquidity_score, -percent_diff)
                sorted_items = list(sorted(filtered_signals.items(), key=combined_rank_percent_liquidity))
            elif filter_mode == 5:
                # Объем + ликвидность
                def combined_rank_volume_liquidity(item):
                    buy_liquid = item[1][10]
                    sell_liquid = item[1][11]
                    liquidity_score = buy_liquid + sell_liquid
                    volume = float(item[1][12])
                    return (liquidity_score, -volume)
                sorted_items = list(sorted(filtered_signals.items(), key=combined_rank_volume_liquidity))

            items_per_page = 10
            total_pages = math.ceil(total_items / items_per_page)
            
            if page > total_pages:
                page = total_pages
            elif page < 1:
                page = 1
                
            start_index = (page - 1) * items_per_page
            end_index = start_index + items_per_page
            
            # Получаем элементы для текущей страницы
            page_signals_items = sorted_items[start_index:end_index]
            page_signals = dict(page_signals_items)
            
            # Добавляем кнопки элементов
            for key, value in page_signals.items():
                buy_licuid = '🟢' if int(value[10]) == 1 else '🟡' if int(value[10]) == 2 else '🔴'
                sell_licuid = '🟢' if int(value[11]) == 1 else '🟡' if int(value[11]) == 2 else '🔴'
                
                button_text = f"{value[0]}: {value[2]} {buy_licuid} {value[3]} {sell_licuid} | {value[1]}%"
                
                token_key = f'{value[0]}|{value[4]}'
                
                for mm_ex in value[4]:
                    token_key = f'{value[0]}|{mm_ex}'
                    if token_key in save_list:
                        button_text += ' ⭐'
                        break
                        
                if value[6] or value[7]:
                    button_text += " ↕️"
                    
                if float(value[12]) > 0:
                    button_text += f' {round(value[12], 2)}$'
                    
                if value[2] in value[4] and value[3] in value[4]:
                    button_text += " 2️⃣"
                    
                button = telebot.types.InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"show_{key}"
                )
                markup.add(button)
            
            # Добавляем кнопки фильтрации
            filter_button1 = telebot.types.InlineKeyboardButton('💹 По % разнице', callback_data='filter_1')
            filter_button2 = telebot.types.InlineKeyboardButton('⚡️ По ликвидности', callback_data='filter_2')
            filter_button3 = telebot.types.InlineKeyboardButton('💰 По объему', callback_data='filter_3')
            filter_button4 = telebot.types.InlineKeyboardButton('💹+⚡️', callback_data='filter_4')
            filter_button5 = telebot.types.InlineKeyboardButton('💰+⚡️', callback_data='filter_5')
            
            # Обновить словарь текстов для активных фильтров
            filter_texts = {
                1: '💹 По % разнице ✓',
                2: '⚡️ По ликвидности ✓',
                3: '💰 По объему ✓',
                4: '💹+⚡️ ✓',
                5: '💰+⚡️ ✓'
            }
            
            if filter_mode in filter_texts:
                if filter_mode == 1:
                    filter_button1 = telebot.types.InlineKeyboardButton(filter_texts[1], callback_data='filter_1')
                elif filter_mode == 2:
                    filter_button2 = telebot.types.InlineKeyboardButton(filter_texts[2], callback_data='filter_2')
                elif filter_mode == 3:
                    filter_button3 = telebot.types.InlineKeyboardButton(filter_texts[3], callback_data='filter_3')
                elif filter_mode == 4:
                    filter_button4 = telebot.types.InlineKeyboardButton(filter_texts[4], callback_data='filter_4')
                elif filter_mode == 5:
                    filter_button5 = telebot.types.InlineKeyboardButton(filter_texts[5], callback_data='filter_5')
                    
            markup.row(filter_button1, filter_button2, filter_button3)
            markup.row(filter_button4, filter_button5)
            
            # Добавляем кнопку поиска и очистки поиска
            search_row = []
            search_button = telebot.types.InlineKeyboardButton('🔍 Поиск', callback_data='search')
            search_row.append(search_button)
            
            if search_query:
                clear_search_button = telebot.types.InlineKeyboardButton('❌ Очистить поиск', callback_data='clear_search')
                search_row.append(clear_search_button)
                
            markup.row(*search_row)
            
            # Кнопки навигации
            navigation_buttons = []
            
            if page > 1:
                navigation_buttons.append(telebot.types.InlineKeyboardButton('⬅️', callback_data='page_prev'))
                
            navigation_buttons.append(telebot.types.InlineKeyboardButton(f'{page}/{total_pages}', callback_data='null'))
            
            if page < total_pages:
                navigation_buttons.append(telebot.types.InlineKeyboardButton('➡️', callback_data='page_next'))
                
            if navigation_buttons:
                markup.row(*navigation_buttons)
            
            # Сообщение с номером страницы
            message_text = f'Активные ММ:'
            if search_query:
                message_text = f'Активные ММ (поиск: {search_query}):'
                
            if message:
                await self.bot.send_message(
                    chat_id,
                    message_text,
                    reply_markup=markup
                )
            elif call:
                await self.bot.edit_message_text(
                    message_text,
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=markup
                )
                await self.bot.answer_callback_query(call.id)
                
        except Exception as e:
            print(f"Ошибка в generate_active_mm_list: {e}")
            if call:
                await self.bot.answer_callback_query(
                    call.id,
                    text='Произошла ошибка при обновлении списка.',
                    show_alert=True
                )

    async def search_mm_list(self, call):
        """Handler for search button."""
        try:
            user_id = call.message.chat.id
            
            # Отправляем сообщение с запросом на ввод поискового запроса
            msg = await self.bot.send_message(
                user_id,
                "Введите название токена для поиска:"
            )
            
            # Создаем запись в словаре ожидания поискового запроса
            self.waiting_for_search_input = self.waiting_for_search_input or {}
            self.waiting_for_search_input[user_id] = True
            
            # Сохраняем ID сообщения с активными ММ, чтобы обновить его после поиска
            self.active_mm_message_id = self.active_mm_message_id or {}
            self.active_mm_message_id[user_id] = call.message.message_id
            
            await self.bot.answer_callback_query(call.id)
            
        except Exception as e:
            print(f"Ошибка в search_mm_list: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при запуске поиска.',
                show_alert=True
            )

    async def clear_search(self, call):
        """Handler for clearing search results."""
        try:
            user_id = call.message.chat.id
            
            # Очищаем поисковый запрос
            if user_id in self.user_search_query:
                del self.user_search_query[user_id]
            
            # Обновляем список без поиска
            await self.generate_active_mm_list(
                user_id, 
                page=self.user_pages.get(user_id, 1), 
                call=call, 
                filter_mode=self.user_filter_mode.get(user_id, 1)
            )
            
        except Exception as e:
            print(f"Ошибка в clear_search: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при очистке поиска.',
                show_alert=True
            )



    async def show_favorites(self, message):
        user_id = message.chat.id
        if str(user_id) in self.users:
            user_id = message.chat.id
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            save_list = user_model.get('saveList', [])

            if not save_list:
                await self.bot.send_message(user_id, 'У вас нет сохраненных токенов.')
                return

            self.user_pages[user_id] = 1  # Initialize page number
            await self.generate_favorites_list(user_id, page=1)
        else:
            await self.bot.send_message(message.chat.id, 'У вас нет доступа')
    
    async def spread_limit_callback(self, call):
        try:
            user_id = call.message.chat.id
            token_key = call.data.replace('spreadCall_', '')
            
            # Получаем текущее значение курсовой разницы
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            spread_limits = user_model.get('spreadLimits', {})
            current_limit = spread_limits.get(token_key, 0)
            
            # Создаем клавиатуру с кнопками + и -
            markup = telebot.types.InlineKeyboardMarkup(row_width=3)
            buttons = []
            
            # Кнопка - добавляется только если текущий лимит > 0
            if current_limit > 0:
                minus_button = telebot.types.InlineKeyboardButton('-', callback_data=f'spread_minus_{token_key}')
                buttons.append(minus_button)
            
            # Текущее значение
            value_button = telebot.types.InlineKeyboardButton(f'{current_limit}%', callback_data=f'noop')
            buttons.append(value_button)
            # Кнопка +
            plus_button = telebot.types.InlineKeyboardButton('+', callback_data=f'spread_plus_{token_key}')
            buttons.append(plus_button)
            # Добавляем кнопки в клавиатуру
            
            markup.row(*buttons)
            
            # Отправляем сообщение с клавиатурой
            await self.bot.send_message(
                user_id,
                f"Минимальная курсовая разница для токена: {current_limit}%\n"
                f"Используйте кнопки для изменения значения:",
                reply_markup=markup
            )
            
            await self.bot.answer_callback_query(call.id)
        except Exception as e:
            print(f"Ошибка в spread_limit_callback: {e}")

    async def handle_spread_minus(self, call):
        try:
            user_id = call.message.chat.id
            token_key = call.data.replace('spread_minus_', '')
            
            # Получаем текущее значение
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            spread_limits = user_model.get('spreadLimits', {})
            current_limit = spread_limits.get(token_key, 0)
            
            # Уменьшаем значение на 1%
            new_limit = max(0, current_limit - 1)
            
            # Обновляем значение в базе
            await base_db.userdata.mm_user.update_one(
                {'id': int(user_id)},
                {'$set': {f'spreadLimits.{token_key}': new_limit}}
            )
            
            # Обновляем сообщение с новыми кнопками
            await self.update_spread_message(call.message, token_key, new_limit)
            await self.bot.answer_callback_query(call.id)
        except Exception as e:
            print(f"Ошибка в handle_spread_minus: {e}")

    async def handle_spread_plus(self, call):
        try:
            user_id = call.message.chat.id
            token_key = call.data.replace('spread_plus_', '')
            
            # Получаем текущее значение
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            spread_limits = user_model.get('spreadLimits', {})
            current_limit = spread_limits.get(token_key, 0)
            
            # Увеличиваем значение на 1%
            new_limit = current_limit + 1
            
            # Обновляем значение в базе
            await base_db.userdata.mm_user.update_one(
                {'id': int(user_id)},
                {'$set': {f'spreadLimits.{token_key}': new_limit}}
            )
            
            # Обновляем сообщение с новыми кнопками
            await self.update_spread_message(call.message, token_key, new_limit)
            await self.bot.answer_callback_query(call.id)
        except Exception as e:
            print(f"Ошибка в handle_spread_plus: {e}")

    async def update_spread_message(self, message, token_key, current_limit):
        try:
            # Создаем новую клавиатуру
            markup = telebot.types.InlineKeyboardMarkup(row_width=3)
            buttons = []

            
            # Кнопка - добавляется только если текущий лимит > 0
            if current_limit > 0:
                minus_button = telebot.types.InlineKeyboardButton('-', callback_data=f'spread_minus_{token_key}')
                buttons.append(minus_button)
            
            # Текущее значение
            value_button = telebot.types.InlineKeyboardButton(f'{current_limit}%', callback_data=f'noop')
            buttons.append(value_button)
            # Кнопка +
            plus_button = telebot.types.InlineKeyboardButton('+', callback_data=f'spread_plus_{token_key}')
            buttons.append(plus_button)

            markup.row(*buttons)
            
            # Обновляем сообщение
            await self.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=message.message_id,
                text=f"Минимальная курсовая разница для токена: {current_limit}%\n"
                    f"Используйте кнопки для изменения значения:",
                reply_markup=markup
            )
        except Exception as e:
            print(f"Ошибка в update_spread_message: {e}")



    async def set_spread_limit(self, message, token_key):
        user_id = message.chat.id
        try:
            spread_limit = float(message.text.replace(',', '.'))
            if spread_limit < 0:
                await self.bot.send_message(user_id, "Лимит не может быть отрицательным. Пожалуйста, попробуйте снова.")
                return
            
            # Обновляем лимит в базе данных
            await base_db.userdata.mm_user.update_one(
                {'id': int(user_id)},
                {'$set': {f'spreadLimits.{token_key}': spread_limit}}
            )
            
            await self.bot.send_message(
                user_id, 
                f"Установлен минимальный лимит курсовой разницы {spread_limit}% для выбранного токена."
            )
            
            # Обновляем список избранного
            await self.generate_favorites_list(user_id, page=self.user_pages.get(user_id, 1))
            
        except ValueError:
            await self.bot.send_message(
                user_id, 
                "Неверный формат. Пожалуйста, введите число (например: 1.5)."
            )

    """ async def process_user(user):
        try:
            user_id = user['id']
            # Получаем список сохраненных токенов пользователя
            save_list = user.get('saveList', [])
            # Получаем лимиты спреда пользователя
            spread_limits = user.get('spreadLimits', {})
            
            # Инициализируем словарь для хранения последних отправленных значений для пользователя
            if user_id not in user_last_sent_values:
                user_last_sent_values[user_id] = {}
                
            # Создаем список заданий для отправки сигналов
            tasks = []
            
            # Проверяем актуальные сигналы
            for key, signal in self.final_signals.items():
                # Определяем биржу с маркет-мейкером
                mm_ex = signal[2] if signal[2] in signal[4] else signal[3] if signal[3] in signal[4] else ""
                token_key = f'{signal[0]}|{mm_ex}'
                
                # Если токен в списке сохраненных пользователя
                if token_key in save_list:
                    current_price_diff = signal[1]
                    token_key_abs = f'{signal[0]}|{signal[5]}&{signal[2]}|{signal[3]}'
                    
                    # Проверяем минимальный лимит спреда для этого токена
                    min_spread_limit = spread_limits.get(token_key, 0)

                    
                    # Сравниваем текущую курсовую разницу с минимальным лимитом
                    if current_price_diff < min_spread_limit:
                        continue  # Пропускаем этот сигнал, если разница меньше лимита
                        
                    # Если это первое появление сигнала после его отсутствия или первая отправка
                    if token_key_abs not in user_last_sent_values[user_id]:
                        # Создаем задачу для отправки сигнала
                        tasks.append(asyncio.create_task(send_signal_to_user(user_id, signal, token_key)))
                        user_last_sent_values[user_id][token_key_abs] = current_price_diff
            
            # Остальной код функции остается без изменений
        except Exception as e:
            print(f"Ошибка в process_user: {e}") """


    async def generate_favorites_list(self, user_id, page=1, call=None):
        try:
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            save_list = user_model.get('saveList', [])
            spread_limits = user_model.get('spreadLimits', {})
            
            items_per_page = 10
            total_items = len(save_list)
            total_pages = math.ceil(total_items / items_per_page)
            
            if page > total_pages:
                page = total_pages
            elif page < 1:
                page = 1
                
            start_index = (page - 1) * items_per_page
            end_index = start_index + items_per_page
            
            page_tokens = save_list[start_index:end_index]
            
            markup = telebot.types.InlineKeyboardMarkup(row_width=3)
            
            # Добавляем кнопки для токенов (новый формат - только название токена)
            for token in page_tokens:
                token = token.split('|')[0] if '|' in token else token
                token_button = telebot.types.InlineKeyboardButton(token, callback_data='noop')
                spread_limit = spread_limits.get(token, 0)
                spread_button = telebot.types.InlineKeyboardButton(f'{spread_limit}%', callback_data=f'spreadCall_{token}')
                delete_button = telebot.types.InlineKeyboardButton('❌', callback_data=f'deletefav_{token}')
                
                markup.add(token_button, spread_button, delete_button)
            
            # Добавляем кнопку "Добавить токен"
            add_button = telebot.types.InlineKeyboardButton('Добавить токен ➕', callback_data='add_token')
            markup.row(add_button)
            
            # Кнопки пагинации
            navigation_buttons = []
            if page > 1:
                navigation_buttons.append(telebot.types.InlineKeyboardButton('⬅️', callback_data='fav_prev'))
            
            navigation_buttons.append(telebot.types.InlineKeyboardButton(f'{page}/{total_pages}', callback_data='null'))
            
            if page < total_pages:
                navigation_buttons.append(telebot.types.InlineKeyboardButton('➡️', callback_data='fav_next'))
            
            if navigation_buttons:
                markup.row(*navigation_buttons)
            
            message_text = f'Избранное (Стр {page}/{total_pages}):'
            
            if call:
                await self.bot.edit_message_text(
                    message_text,
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=markup
                )
                await self.bot.answer_callback_query(call.id)
            else:
                await self.bot.send_message(
                    user_id,
                    message_text,
                    reply_markup=markup
                )
        except Exception as e:
            print(f"Ошибка в generate_favorites_list: {e}")


    async def add_token_callback(self, call):
        try:
            user_id = call.message.chat.id
            
            # Отправляем сообщение с запросом на ввод пары токен/биржа
            msg = await self.bot.send_message(
                user_id,
                "Введите токен в формате: ТОКЕН/USDT\nНапример: BTC/USDT"
            )
            
            # Создаем запись в словаре ожидания ответа (вместо register_next_step_handler)
            self.waiting_for_token_input[user_id] = True
            
            await self.bot.answer_callback_query(call.id)
            
        except Exception as e:
            print(f"Ошибка в add_token_callback: {e}")

    # Обработчик всех текстовых сообщений
    async def handle_text(self, message):
        user_id = message.chat.id
        
        # Если пользователь в режиме добавления токена
        if user_id in self.waiting_for_token_input and self.waiting_for_token_input[user_id]:
            # Удаляем флаг ожидания ввода
            self.waiting_for_token_input[user_id] = False
            
            # Обрабатываем введенный токен
            await self.process_new_token(message)
        # Если пользователь в режиме добавления токена в черный список
        elif user_id in self.waiting_for_blacklist_input and self.waiting_for_blacklist_input[user_id]:
            # Удаляем флаг ожидания ввода
            self.waiting_for_blacklist_input[user_id] = False
            
            # Обрабатываем введенный токен для черного списка
            await self.process_new_blacklist_token(message)
        
        # Если пользователь в режиме поиска
        # Если пользователь в режиме поиска
        elif user_id in self.waiting_for_search_input and self.waiting_for_search_input[user_id]:
            # Удаляем флаг ожидания поискового запроса
            self.waiting_for_search_input[user_id] = False
            
            # Сохраняем поисковый запрос
            search_query = message.text.strip()
            self.user_search_query[user_id] = search_query
            
            # Устанавливаем первую страницу для результатов поиска
            self.user_pages[user_id] = 1
            
            # Сначала удаляем сообщение с запросом
            await self.bot.delete_message(user_id, message.message_id)
            
            # Затем генерируем новое сообщение со списком результатов
            await self.generate_active_mm_list(
                user_id,
                page=1,
                message=message,  # Передаем сообщение, чтобы был отправлен новый список
                filter_mode=self.user_filter_mode.get(user_id, 1),
                search_query=search_query
            )

            
            # Удаляем сообщение с запросом на ввод поискового запроса
            await self.bot.delete_message(user_id, message.message_id)
        
        else:
            # Здесь обработка других текстовых сообщений
            pass


    async def navigate_active_mm(self, call):
        """Handler for pagination buttons."""
        try:
            user_id = call.message.chat.id
            
            # Проверяем, что пользователь имеет страницу
            if user_id not in self.user_pages:
                self.user_pages[user_id] = 1
                
            current_page = self.user_pages[user_id]
            
            # Определяем направление пагинации
            direction = call.data.split('_')[1]
            
            if direction == 'prev':
                if current_page > 1:
                    self.user_pages[user_id] = current_page - 1
            elif direction == 'next':
                self.user_pages[user_id] = current_page + 1
                
            # Обновляем список с учетом поискового запроса
            search_query = self.user_search_query.get(user_id, None)
            
            await self.generate_active_mm_list(
                user_id, 
                page=self.user_pages[user_id], 
                call=call, 
                filter_mode=self.user_filter_mode.get(user_id, 1),
                search_query=search_query
            )
            
        except Exception as e:
            print(f"Ошибка в navigate_active_mm: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при навигации по списку.',
                show_alert=True
            )

    async def process_new_token(self, message):
        try:
            user_id = message.chat.id
            token_input = message.text.strip()
            
            # Если пользователь ввел в старом формате "токен|биржа", извлекаем только токен
            token_name = token_input.split('|')[0] if '|' in token_input else token_input
            
            # Получаем текущий список избранных токенов пользователя
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            save_list = user_model.get('saveList', [])
            
            # Проверяем, нет ли уже такого токена в списке
            if token_name in save_list:
                await self.bot.send_message(user_id, "Этот токен уже в вашем списке избранного.")
                return
            
            # Добавляем токен в список избранного
            save_list.append(token_name)
            await base_db.userdata.mm_user.update_one(
                {'id': int(user_id)},
                {'$set': {'saveList': save_list}}
            )
            
            await base_db.userdata.user.update_one(
                {'id': int(user_id)},
                {'$set': {'save_mm_signals': save_list}}
            )
            
            await self.bot.send_message(
                user_id,
                f"Токен {token_name} успешно добавлен в избранное."
            )
            
            # Обновляем список избранного
            await self.generate_favorites_list(user_id)
        except Exception as e:
            print(f"Ошибка в process_new_token: {e}")
            await self.bot.send_message(user_id, "Произошла ошибка при добавлении токена.")




    async def process_new_blacklist_token(self, message):
        try:
            user_id = message.chat.id
            token = message.text.strip()
            
            # Проверяем и форматируем токен (добавляем /USDT, если отсутствует)
            if '/' not in token:
                token = f"{token}/USDT"
            
            # Получаем текущий черный список пользователя
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            
            # Создаем поле blackList, если оно отсутствует
            if 'blackList' not in user_model:
                await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'blackList': []}})
                user_model['blackList'] = []
            
            black_list = user_model.get('blackList', [])
            
            # Проверяем, нет ли уже такого токена в списке
            if token in black_list:
                await self.bot.send_message(user_id, "Этот токен уже в вашем черном списке.")
                return
            
            # Добавляем токен в черный список
            black_list.append(token)
            await base_db.userdata.mm_user.update_one(
                {'id': int(user_id)},
                {'$set': {'blackList': black_list}}
            )
            
            await self.bot.send_message(
                user_id,
                f"Токен {token} успешно добавлен в черный список."
            )
            
            # Обновляем черный список
            await self.generate_blacklist(user_id)
            
        except Exception as e:
            print(f"Ошибка в process_new_blacklist_token: {e}")
            await self.bot.send_message(user_id, "Произошла ошибка при добавлении токена в черный список.")


    async def add_blacklist_token_callback(self, call):
        try:
            user_id = call.message.chat.id
            
            # Отправляем сообщение с запросом на ввод токена
            msg = await self.bot.send_message(
                user_id,
                "Введите название токена для добавления в черный список:"
            )
            
            # Создаем запись в словаре ожидания ввода
            self.waiting_for_blacklist_input[user_id] = True
            
            await self.bot.answer_callback_query(call.id)
            
        except Exception as e:
            print(f"Ошибка в add_blacklist_token_callback: {e}")


    async def delete_blacklisted(self, call):
        try:
            user_id = call.message.chat.id
            token = call.data[12:]  # Remove 'deleteblack_'
            
            # Remove token from user's blackList
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            black_list = user_model.get('blackList', [])
            
            if token in black_list:
                black_list.remove(token)
                await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'blackList': black_list}})
                
            # Refresh the blacklist
            current_page = self.user_pages.get(user_id, 1)
            await self.generate_blacklist(user_id, page=current_page, call=call)
            
        except Exception as e:
            print(f"Ошибка в delete_blacklisted: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при удалении токена.',
                show_alert=True
            )


    async def navigate_blacklist(self, call):
        try:
            user_id = call.message.chat.id
            action = call.data
            current_page = self.user_pages.get(user_id, 1)
            
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            total_items = len(user_model.get('blackList', []))
            items_per_page = 10
            total_pages = math.ceil(total_items / items_per_page)
            
            if action == 'blacklist_next':
                if current_page < total_pages:
                    current_page += 1
            elif action == 'blacklist_prev':
                if current_page > 1:
                    current_page -= 1
                    
            self.user_pages[user_id] = current_page
            await self.generate_blacklist(user_id, page=current_page, call=call)
            
        except Exception as e:
            print(f"Ошибка в navigate_blacklist: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при навигации по страницам.',
                show_alert=True
            )

        
    async def generate_blacklist(self, user_id, page=1, call=None):
        try:
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            
            # Убедимся, что поле blackList существует
            if 'blackList' not in user_model:
                await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'blackList': []}})
                user_model['blackList'] = []
                
            black_list = user_model.get('blackList', [])
            
            items_per_page = 10
            total_items = len(black_list)
            total_pages = math.ceil(total_items / items_per_page)
            
            if page > total_pages:
                page = total_pages
            elif page < 1:
                page = 1
                
            start_index = (page - 1) * items_per_page
            end_index = start_index + items_per_page
            
            page_tokens = black_list[start_index:end_index]
            
            markup = telebot.types.InlineKeyboardMarkup(row_width=2)
            
            # Добавляем кнопки для существующих токенов
            for token in page_tokens:
                token_button = telebot.types.InlineKeyboardButton(token, callback_data='noop')
                delete_button = telebot.types.InlineKeyboardButton('❌', callback_data=f'deleteblack_{token}')
                markup.row(token_button, delete_button)
            
            # Добавляем кнопку "Добавить токен"
            add_button = telebot.types.InlineKeyboardButton('Добавить токен ➕', callback_data='add_blacklist_token')
            markup.row(add_button)
            
            # Кнопки пагинации
            navigation_buttons = []
            if page > 1:
                navigation_buttons.append(telebot.types.InlineKeyboardButton('⬅️', callback_data='blacklist_prev'))
                
            navigation_buttons.append(telebot.types.InlineKeyboardButton(f'{page}/{total_pages}', callback_data='null'))
            
            if page < total_pages:
                navigation_buttons.append(telebot.types.InlineKeyboardButton('➡️', callback_data='blacklist_next'))
                
            if navigation_buttons:
                markup.row(*navigation_buttons)
                
            message_text = f'Черный список (Стр {page}/{total_pages}):'
            
            if call:
                await self.bot.edit_message_text(
                    message_text,
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=markup
                )
                await self.bot.answer_callback_query(call.id)
            else:
                await self.bot.send_message(
                    user_id,
                    message_text,
                    reply_markup=markup
                )
        except Exception as e:
            print(f"Ошибка в generate_blacklist: {e}")

    
    async def show_blacklist(self, message):
        user_id = message.chat.id
        if str(user_id) in self.users:
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})

            # Создаем поле blackList, если оно отсутствует
            if 'blackList' not in user_model:
                await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'blackList': []}})
                user_model['blackList'] = []

            black_list = user_model.get('blackList', [])

            markup = telebot.types.InlineKeyboardMarkup(row_width=2)
            add_button = telebot.types.InlineKeyboardButton('Добавить токен ➕', callback_data='add_blacklist_token')
            markup.row(add_button)

            if not black_list:
                # Если черный список пуст, просто отправляем сообщение с кнопкой добавления
                message_text = 'У вас нет токенов в черном списке.'
                await self.bot.send_message(user_id, message_text, reply_markup=markup)
            else:
                # Иначе генерируем список с токенами и всё равно добавляем кнопку
                self.user_pages[user_id] = 1  # Initialize page number
                await self.generate_blacklist(user_id, page=1)

        else:
            await self.bot.send_message(message.chat.id, 'У вас нет доступа')


    async def start(self, message):
        if not await pair_user(message, self.bot):
            return
        await self.set_current_users()
        try:
            user_id = str(message.chat.id)
            # Check if user exists in mm_user collection
            if str(user_id) in self.users:
                user_in_mm_user = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
                if not user_in_mm_user:
                    # Create user model
                    user_model = {
                        'id': int(user_id),
                        'monitoring': True,
                        'saveList': [],
                        'blackList': []  # Добавляем пустой черный список
                    }
                    await base_db.userdata.mm_user.insert_one(user_model)
                    self.mm_users_ids.append(user_id)
                    self.mm_users.append(user_model)

                # Build the reply keyboard
                markup = telebot.types.ReplyKeyboardMarkup(row_width=2)
                item1 = telebot.types.KeyboardButton('Избранное ⭐')
                item2 = telebot.types.KeyboardButton('Мониторинг 💻')
                item3 = telebot.types.KeyboardButton('Активные Ⓜ️Ⓜ️')
                item4 = telebot.types.KeyboardButton('Черный список 🚫')  # Новая кнопка
                markup.add(item1, item2, item3, item4)  # Добавляем в разметку

                await self.bot.send_message(message.chat.id, 'Выберите опцию:', reply_markup=markup)
                self.active_users.append(message.chat.id)
            else:
                await self.bot.send_message(message.chat.id, 'У вас нет доступа')
            
        except Exception as e:
            print(f"Ошибка в start: {e}")

    async def monitoring_menu(self, message):
        user_id = message.chat.id
        if str(user_id) in self.users:
            # Get the user's monitoring status
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            monitoring = user_model.get('monitoring', True)
            # Build the inline keyboard with "ON" and "OFF" buttons
            markup = telebot.types.InlineKeyboardMarkup()
            if monitoring:
                button_on = telebot.types.InlineKeyboardButton('ВКЛ ✅', callback_data='monitoring_on')
                button_off = telebot.types.InlineKeyboardButton('ВЫКЛ', callback_data='monitoring_off')
            else:
                button_on = telebot.types.InlineKeyboardButton('ВКЛ', callback_data='monitoring_on')
                button_off = telebot.types.InlineKeyboardButton('ВЫКЛ ✅', callback_data='monitoring_off')
            markup.add(button_on, button_off)
            await self.bot.send_message(user_id, 'Настройки мониторинга:', reply_markup=markup)
        else:
            await self.bot.send_message(message.chat.id, 'У вас нет доступа')
    async def monitoring_callback(self, call):
        user_id = call.message.chat.id
        if call.data == 'monitoring_on':
            # Set monitoring to True
            await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'monitoring': True}})
            # Update the buttons
            markup = telebot.types.InlineKeyboardMarkup()
            button_on = telebot.types.InlineKeyboardButton('ВКЛ ✅', callback_data='monitoring_on')
            button_off = telebot.types.InlineKeyboardButton('ВЫКЛ', callback_data='monitoring_off')
            markup.add(button_on, button_off)
            await self.bot.edit_message_reply_markup(user_id, call.message.message_id, reply_markup=markup)
            await self.bot.answer_callback_query(call.id, text='Мониторинг включен.')
        elif call.data == 'monitoring_off':
            # Set monitoring to False
            await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'monitoring': False}})
            # Update the buttons
            markup = telebot.types.InlineKeyboardMarkup()
            button_on = telebot.types.InlineKeyboardButton('ВКЛ', callback_data='monitoring_on')
            button_off = telebot.types.InlineKeyboardButton('ВЫКЛ ✅', callback_data='monitoring_off')
            markup.add(button_on, button_off)
            await self.bot.edit_message_reply_markup(user_id, call.message.message_id, reply_markup=markup)
            await self.bot.answer_callback_query(call.id, text='Мониторинг выключен.')

    async def set_current_users(self):
        try:
            users_ids = []
            mm_users_ids = []
            users_data = await base_db.userdata.user.find().to_list(length=None)
            users_mm = await base_db.userdata.mm_user.find().to_list(length=None)
            for user in users_data:
                users_ids.append(str(user['id']))

            for user in users_mm:
                mm_users_ids.append(str(user['id']))

            self.mm_users_ids = mm_users_ids
            self.users = users_ids
            self.mm_users = users_mm
        except Exception as e:
            print(f"Ошибка в set_current_users: {e}")

    async def migrate_favorites_format(self):
        """Преобразует существующий формат избранных токенов (токен|биржа) в новый формат (только токен)"""
        try:
            # Получаем всех пользователей
            all_users = await base_db.userdata.mm_user.find().to_list(length=None)
            
            for user in all_users:
                user_id = user.get('id')
                save_list = user.get('saveList', [])
                new_save_list = []
                
                # Для каждого токена в списке избранного
                for token_key in save_list:
                    if '|' in token_key:
                        # Извлекаем только имя токена (убираем биржу)
                        token_name = token_key.split('|')[0]
                        # Добавляем только имя токена, если его еще нет в списке
                        if token_name not in new_save_list:
                            new_save_list.append(token_name)
                    else:
                        # Если токен уже в новом формате, просто добавляем его
                        if token_key not in new_save_list:
                            new_save_list.append(token_key)
                
                # Обновляем список избранного пользователя
                await base_db.userdata.mm_user.update_one(
                    {'id': int(user_id)},
                    {'$set': {'saveList': new_save_list}}
                )
                
                # Обновляем список избранного в другой коллекции
                await base_db.userdata.user.update_one(
                    {'id': int(user_id)},
                    {'$set': {'save_mm_signals': new_save_list}}
                )
                
            print("Миграция списков избранного завершена")
            
        except Exception as e:
            print(f"Ошибка в migrate_favorites_format: {e}")

    
    async def start_monitoring(self):
        # Словарь для хранения последних отправленных значений курсовой разницы для каждого пользователя и токена
        # Формат: {user_id: {token_key: last_price_diff}}
        user_last_sent_values = {}
        while True:
            try:
                async def send_signal_to_user(user_id, signal, token_key):
                    try:
                        # Проверка включен ли мониторинг у пользователя
                        user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
                        if not user_model.get('monitoring', True):
                            return

                        # Создаем кнопку "Удалить", т.к. этот токен в списке сохраненных
                        markup = telebot.types.InlineKeyboardMarkup()
                        button = telebot.types.InlineKeyboardButton('Удалить ❌', callback_data=f'delete_{token_key}')
                        markup.add(button)

                        # Создаем сообщение с информацией о сигнале
                        symbol = signal[0]
                        symbol_formatted_min = {
                            'OKX': symbol.replace('/', '-'),
                            'Kucoin': symbol.replace('/', '-'),
                            'Gate': symbol.replace('/', '_'),
                            'MEXC': symbol.replace('/', ''),
                            'Bitget': symbol.replace('/', ''),
                            'Bybit': symbol.replace('/', ''),
                            'BingX': symbol.replace('/', ''),
                            'HTX': symbol.replace('/', '').lower(),
                            'CoinEx': symbol.replace('/', ''),
                            'Bitmart': symbol.replace('/', '_')
                        }.get(signal[2], symbol)

                        symbol_formatted_max = {
                            'OKX': symbol.replace('/', '-'),
                            'Kucoin': symbol.replace('/', '-'),
                            'Gate': symbol.replace('/', '_'),
                            'MEXC': symbol.replace('/', ''),
                            'Bitget': symbol.replace('/', ''),
                            'Bybit': symbol.replace('/', ''),
                            'BingX': symbol.replace('/', ''),
                            'HTX': symbol.replace('/', '').lower(),
                            'CoinEx': symbol.replace('/', '-'),
                            'Bitmart': symbol.replace('/', '_')
                        }.get(signal[3], symbol)

                        # Формируем ссылки на биржи
                        link_formatted_min = {
                            'OKX': f"https://www.okx.com/ru/trade-spot/{symbol_formatted_min}",
                            'Kucoin': f"https://www.kucoin.com/ru/trade/{symbol_formatted_min}",
                            'Gate': f"https://www.gate.io/ru/trade/{symbol_formatted_min}",
                            'MEXC': f"https://www.mexc.com/ru-RU/exchange/{symbol_formatted_min}",
                            'Bitget': f"https://www.bitget.com/ru/spot/{symbol_formatted_min}_USDT",
                            'Bybit': f"https://www.bybit.com/en/trade/spot/{symbol_formatted_min}/USDT",
                            'BingX': f"https://bingx.com/en-us/spot/{symbol_formatted_min}/",
                            'HTX': f"https://www.htx.com/ru-ru/trade/{symbol_formatted_min}_usdt/",
                            'CoinEx': f"https://www.coinex.com/ru/exchange/{symbol_formatted_min}-USDT",
                            'Bitmart': f"https://www.bitmart.com/trade/ru-RU?symbol={symbol_formatted_min}",
                        }.get(signal[2])

                        link_formatted_max = {
                            'OKX': f"https://www.okx.com/ru/trade-spot/{symbol_formatted_max}",
                            'Kucoin': f"https://www.kucoin.com/ru/trade/{symbol_formatted_max}",
                            'Gate': f"https://www.gate.io/ru/trade/{symbol_formatted_max}",
                            'MEXC': f"https://www.mexc.com/ru-RU/exchange/{symbol_formatted_max}",
                            'Bitget': f"https://www.bitget.com/ru/spot/{symbol_formatted_max}_USDT",
                            'Bybit': f"https://www.bybit.com/en/trade/spot/{symbol_formatted_max.replace('USDT', '')}/USDT",
                            'CoinEx': f"https://www.coinex.com/ru/exchange/{symbol_formatted_max.replace('USDT', '-USDT')}",
                            'BingX': f"https://bingx.com/en-us/spot/{symbol_formatted_max}/",
                            'HTX': f"https://www.htx.com/ru-ru/trade/{symbol_formatted_max}_usdt/",
                            'Bitmart': f"https://www.bitmart.com/trade/ru-RU?symbol={symbol_formatted_max}",
                        }.get(signal[3])

                        feeVol = round(signal[8],2)
                        feeVolDol = round(signal[9],2)

                        buy_licuid = '🟢' if int(signal[10]) == 1 else '🟡' if int(signal[10]) == 2 else '🔴'
                        sell_licuid = '🟢' if int(signal[11]) == 1 else '🟡' if int(signal[11]) == 2 else '🔴'


                        message_text = (
                            f"Токен: {signal[0]}\n\n"
                            f"Курсовая разница: {signal[1]}%\n\n"
                            f"Биржа покупки: [{signal[2]}]({link_formatted_min}) {buy_licuid} {'Ⓜ️Ⓜ️' if signal[2] in signal[4] else ''} {'↕️' if signal[6] else ''}\n"
                            f"Биржа продажи: [{signal[3]}]({link_formatted_max}) {sell_licuid} {'Ⓜ️Ⓜ️' if signal[3] in signal[4] else ''} {'↕️' if signal[7] else ''}\n\n"
                            f"Комиссия: {feeVolDol}$, {feeVol} | Сеть: {signal[5]}\n\n"
                            f"{f'Разрыв более 5% на биржах: {signal[2]}, {signal[3]}' if signal[6] and signal[7] else f'Разрыв более 5% на бирже: {signal[2]}' if signal[6] else f'Разрыв более 5% цен на бирже: {signal[3]}' if signal[7] else ''}\n"
                            f"{f'Доступный объем: {round(signal[12], 2)}$' if float(signal[12]) > 0 else ''}"
                        )
                        
                        # Отправляем сообщение
                        await self.bot.send_message(
                            user_id,
                            message_text,
                            parse_mode='Markdown',
                            disable_web_page_preview=True,
                            reply_markup=markup
                        )
                    except Exception as e:
                        print(f"Ошибка в send_signal_to_user: {e}")
                        pass

                async def process_user(user):
                    try:
                        user_id = user['id']
                        # Получаем список сохраненных токенов пользователя
                        save_list = user.get('saveList', [])
                        spread_limits = user.get('spreadLimits', {})
                        # Проверяем минимальный лимит спреда для этого токена
                        
                        
                        # Инициализируем словарь для хранения последних отправленных значений для пользователя
                        if user_id not in user_last_sent_values:
                            user_last_sent_values[user_id] = {}
                        
                        # Создаем список заданий для отправки сигналов
                        tasks = []
                        
                        # Проверяем актуальные сигналы
                        for key, signal in self.final_signals.items():
                            token_name = signal[0]  # Используем только имя токена без биржи
                            if float(signal[1]) <= 0:  # Проверка на положительную курсовую разницу
                                continue  
                            # Если токен в списке сохраненных пользователя
                            if token_name in save_list:
                                current_price_diff = signal[1]
                                token_key_abs = f'{signal[0]}|{signal[5]}&{signal[2]}|{signal[3]}'

                                min_spread_limit = spread_limits.get(token_name, 0)
                        
                                # Сравниваем текущую курсовую разницу с минимальным лимитом
                                if current_price_diff < min_spread_limit:
                                    continue  # Пропускаем этот сигнал, если разница меньше лимита
                                
                                # Если это первое появление сигнала после его отсутствия или первая отправка
                                if token_key_abs not in user_last_sent_values[user_id]:
                                    # Создаем задачу для отправки сигнала
                                    tasks.append(asyncio.create_task(send_signal_to_user(user_id, signal, token_key_abs)))
                                    user_last_sent_values[user_id][token_key_abs] = current_price_diff
                                else:
                                    # Получаем последнюю отправленную курсовую разницу
                                    last_price_diff = user_last_sent_values[user_id][token_key_abs]
                                    
                                    # Проверяем, изменилась ли курсовая разница более чем на 1%
                                    if abs(current_price_diff - last_price_diff) >= 1:
                                        # Создаем задачу для отправки сигнала
                                        tasks.append(asyncio.create_task(send_signal_to_user(user_id, signal, token_key_abs)))
                                        user_last_sent_values[user_id][token_key_abs] = current_price_diff
                        
                        # Проверяем исчезнувшие сигналы
                        # Создаем список актуальных token_key из final_signals
                        current_token_keys = []
                        for signal in self.final_signals.values():
                            mm_ex = signal[2] if signal[2] in signal[4] else signal[3] if signal[3] in signal[4] else ""
                            current_token_keys.append(f'{signal[0]}|{mm_ex}')
                        
                        # Находим токены из сохраненных, которых нет в актуальных сигналах
                        for token_key in list(user_last_sent_values[user_id].keys()):
                            result_key = token_key.split('&')[0]
                            if result_key in save_list and result_key not in current_token_keys:
                                # Удаляем запись о последней курсовой разнице
                                del user_last_sent_values[user_id][token_key]
                        
                        # Если есть задачи для выполнения, выполняем их все одновременно
                        if tasks:
                            await asyncio.gather(*tasks)
                            
                    except Exception as e:
                        print(f"Ошибка при обработке пользователя {user['id']}: {e}")
                        pass
                
                # Создаем задачи для всех пользователей и ожидаем их выполнения
                user_tasks = [asyncio.create_task(process_user(user)) for user in self.mm_users]
                await asyncio.gather(*user_tasks)
                
                # Пауза перед следующей проверкой
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"Ошибка в start_monitoring: {e}")
                await asyncio.sleep(1)  # Также проверка каждую секунду в случае ошибки

    async def collect_trades_all_exs(self, symbol, exchange):
        try: 
            symbol_formatted = {
                    'OKX': symbol.replace('/', '-'),
                    'Kucoin': symbol.replace('/', '-'),
                    'Gate': symbol.replace('/', '_'),
                    'MEXC': symbol.replace('/', ''),
                    'Bitget': symbol.replace('/', ''),
                    'Bybit': symbol.replace('/', ''),
                    'BingX': symbol.replace('/', '-'),
                    'HTX': symbol.replace('/', '').lower(),
                    'CoinEx': symbol.replace('/', ''),
                    'Bitmart': symbol.replace('/', '_'),
                }[exchange]

            url_formatted = {
                'Kucoin': 'https://api.kucoin.com/api/v1/market/histories?limit=50&symbol=', 
                'Gate': 'https://api.gateio.ws/api/v4/spot/trades?limit=50&currency_pair=',
                'BingX': 'https://open-api.bingx.com/openApi/spot/v1/market/trades?limit=50&symbol=',
                'HTX': 'https://api.huobi.pro/market/history/trade?size=50&symbol=',
                'Bitmart': 'https://api-cloud.bitmart.com/spot/quotation/v3/trades?limit=50&symbol=',
                'MEXC': 'https://api.mexc.com/api/v3/trades?symbol=',
                'Bitget': 'https://api.bitget.com/api/v2/spot/market/fills?limit=50&symbol=',
                'Bybit': 'https://api.bybit.com/v5/market/recent-trade?category=spot&limit=50&symbol=',
                'CoinEx': 'https://api.coinex.com/v2/spot/deals?limit=50&market=',
            }[exchange]

            proxy = random.choice(self.proxies)

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20), connector=(aiohttp_socks.ProxyConnector.from_url(proxy) if proxy else None)) as session:
                async with session.get(f'{url_formatted}{symbol_formatted}') as response:
                    if response.status == 200:
                        match exchange:
                            case 'Kucoin':
                                try: 
                                    result = await response.json()
                                    formatted_trades = [
                                        {
                                            'price': obj['price'],
                                            'qty': obj['size'],
                                            'time': str(int(float(obj['time'])*1000)),
                                            'orderbook_type': 'asks' if obj['side'] == 'buy' else 'bids'
                                            
                                        } for obj in result['data']
                                    ]
                                    return formatted_trades 
                                except Exception as e:
                                    pass

                            case 'Gate':
                                try: 
                                    result = await response.json()
                                    formatted_trades = [
                                        {
                                            'price': obj['price'],
                                            'qty': obj['amount'],
                                            'time': str(int(float(obj['create_time'])*1000)),
                                            'orderbook_type': 'asks' if obj['side'] == 'buy' else 'bids'
                                        } for obj in result
                                    ]
                                    return formatted_trades
                                except Exception as e:
                                    pass

                            case 'BingX':
                                try: 
                                    result = await response.json()
                                    if result['code']==100204:
                                        return
                                    formatted_trades = [
                                        {
                                            'price': obj['price'],
                                            'qty': obj['qty'],
                                            'time': str(obj['time']),
                                            'orderbook_type': 'asks' if not obj['buyerMaker'] else 'bids'
                                        } for obj in result['data']
                                    ]
                                    return formatted_trades
                                except Exception as e:
                                    pass

                            case 'HTX':
                                try: 
                                    result = await response.json()
                                    
                                    formatted_trades = [
                                        {
                                            'price': float(obj['data'][0]['price']),
                                            'qty': float(obj['data'][0]['amount']),
                                            'time': obj['data'][0]['ts'],
                                            'orderbook_type': 'asks' if obj['data'][0]['direction'] == 'buy' else 'bids'
                                        } for obj in result['data']
                                    ]

                                    return formatted_trades 
                                except Exception as e:
                                    pass

                            case 'Bitmart':
                                try: 
                                    result = await response.json()
                                    formatted_trades = [
                                        {
                                            'price': obj[2],
                                            'qty': obj[3],
                                            'time': str(obj[1]),
                                            'orderbook_type': 'asks' if obj[4] == 'buy' else 'bids'
                                        } for obj in result['data']
                                    ]

                                    return formatted_trades
                                except Exception as e:
                                    #print(f"{exchange}: {e}")
                                    pass

                            case 'MEXC': 
                                try: 
                                    result = await response.json()
                                    formatted_trades = [
                                        {
                                            'price': obj['price'],
                                            'qty': obj['qty'],
                                            'time': obj['time'],
                                            'orderbook_type': 'bids' if obj['tradeType'] == 'ASK' else 'asks'
                                        } for obj in result
                                    ]
                                    return formatted_trades
                                except Exception as e:
                                    pass

                            case 'Bitget':
                                try: 
                                    result = await response.json()
                                    formatted_trades = [
                                        {
                                            'price': obj['price'],
                                            'qty': obj['size'],
                                            'time': str(obj['ts']),
                                            'orderbook_type': 'bids' if obj['side'] == 'sell' else 'asks'
                                        } for obj in result['data'][:50]
                                    ]
                                    return formatted_trades
                                except Exception as e:
                                    #print(f"{exchange}: {e}")
                                    pass

                            case 'Bybit': 
                                try: 
                                    result = await response.json()
                                    formatted_trades = [
                                        {
                                            'price': obj['price'],
                                            'qty': obj['size'],
                                            'time': str(int(float(obj['time'])*1000)),
                                            'orderbook_type': 'asks' if obj['side'] == 'Buy' else 'bids'
                                        } for obj in result['result']['list']
                                    ]
                                    return formatted_trades
                                except Exception as e:
                                    #print(f"{exchange}: {e}")
                                    pass

                            case 'CoinEx':
                                try: 
                                    result = await response.json()
                                    if result['code']==4004:
                                        return
                                    formatted_trades = [
                                        {
                                            'price': obj['price'],
                                            'qty': obj['amount'],
                                            'time': str(int(float(obj['created_at'])*1000)),
                                            'orderbook_type': 'asks' if obj['side'] == 'buy' else 'bids'
                                        } for obj in result['data'][:50]
                                    ]

                                    return formatted_trades
                                except Exception as e:
                                    print(f"11{exchange}: {e}")
                                    return None
                                    pass
                    else:
                        print(f"22{exchange}: {response}")
                        return None
                        pass


        except Exception as e:
            print(f"33{exchange}: {e}")
            return None
            pass



                    



async def main():
    collector = DataTokensCollector()
    
    try:
        tasks = [
            asyncio.create_task(collector.update_local_orderbooks()),
            asyncio.create_task(collector.update_mm_list()),
            asyncio.create_task(collector.detect_mm_signals()),
            asyncio.create_task(collector.start_monitoring()),
            asyncio.create_task(collector.bot.infinity_polling())
        ]
        await asyncio.gather(*tasks)


        
    except KeyboardInterrupt:
        print("Получен сигнал остановки, завершаем работу...")



if __name__ == "__main__":
    asyncio.run(main())