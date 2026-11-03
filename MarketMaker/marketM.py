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

sem = asyncio.Semaphore(100)  # Разрешить 3 одновременных доступа

# Инициализация логирования
os.makedirs('logs', exist_ok=True)
logging.basicConfig(filename='logs/process_main.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


base_db = motor.AsyncIOMotorClient("mongodb://mongo:27017", serverSelectionTimeoutMS=10000)
tokens = base_db.renderBase.tradePairs

cache = base.Client(('memcached', 11211))

class DataTokensCollector:
    def __init__(self):
        self.allExchanges = ['Kucoin', 'Gate', 'MEXC', 'Bitget', 'Bybit', 'BingX', 'HTX', 'CoinEx', 'Bitmart']
        self.results = defaultdict(lambda: defaultdict(list))
        self.local_orderbooks = {}
        self.tokens_exs = {}
        self.current_signals = defaultdict(list)

        # Структуры для хранения истории стаканов и сделок
        self.orderbook_history = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))  # exchange -> symbol -> type (asks/bids) -> list of snapshots
        self.trades_history = defaultdict(lambda: defaultdict(list))  # exchange -> symbol -> list of snapshots

        self.potential_market_makers = {}

        self.proxies = [None]
    
    async def update_local_orderbooks(self):
        while True:
            #print("Обновление локальных ордербуков из кэша")
            try:
                for exchange in self.allExchanges:
                    key = f'orderbook_{exchange}_full_snapshot'
                    value = cache.get(key)
                    
                    if value:
                        data = pickle.loads(gzip.decompress(value))
                        self.local_orderbooks[exchange] = data

                        for symbol, orderbook in data.items():
                            try:
                                symbol_formatted = {
                                    'Kucoin': symbol.replace('-USDT', '/USDT'), 
                                    'Gate': symbol.replace('_USDT', '/USDT'),
                                    'BingX': symbol.replace('-USDT', '/USDT'),
                                    'HTX': symbol.replace('usdt', '/USDT').upper(),
                                    'Bitmart': symbol.replace('_USDT', '/USDT'),
                                    'MEXC': symbol.replace('USDT', '/USDT'),
                                    'Bitget': symbol.replace('USDT', '/USDT'),
                                    'Bybit': symbol.replace('USDT', '/USDT'),
                                    'CoinEx': symbol.replace('USDT', '/USDT'),
                                }[exchange]

                                for order_type in ['asks', 'bids']:
                                    snapshot = {
                                        'data': orderbook[order_type],
                                        'timestamp': datetime.now().timestamp(),
                                        'lastUpdate': orderbook['lastUpdate']
                                    }
                                    
                                    self.orderbook_history[exchange][symbol_formatted][order_type].append(snapshot)
                                    
                                    # Оставляем только 18 последних снепшотов
                                    if len(self.orderbook_history[exchange][symbol_formatted][order_type]) > 18:
                                        self.orderbook_history[exchange][symbol_formatted][order_type].pop(0)
                            except Exception as e:
                                ##print(f"Ошибка при распаковке ордербуков: {e}")
                                pass
                        

                await asyncio.sleep(5)
            except Exception as e:
                ##print(f"Ошибка при обновлении локальных ордербуков: {e}")
                await asyncio.sleep(5) 
    
    """ async def update_local_orderbooks(self):
        while True:
            #print("Обновление локальных ордербуков из кэша")
            try:
                for exchange in self.allExchanges:
                    key = f'orderbook_{exchange}_full_snapshot'
                    value = cache.get(key)
                    
                    if value:
                        data = pickle.loads(gzip.decompress(value))
                        self.local_orderbooks[exchange] = data
                        
                        # Сохраняем историю стаканов для каждого токена
                        for symbol, orderbook in data.items():
                            try:
                                symbol_formatted = {
                                    'Kucoin': symbol.replace('-USDT', '/USDT'), 
                                    'Gate': symbol.replace('_USDT', '/USDT'),
                                    'BingX': symbol.replace('-USDT', '/USDT'),
                                    'HTX': symbol.replace('usdt', '/USDT').upper(),
                                    'Bitmart': symbol.replace('_USDT', '/USDT'),
                                    'MEXC': symbol.replace('USDT', '/USDT'),
                                    'Bitget': symbol.replace('USDT', '/USDT'),
                                    'Bybit': symbol.replace('USDT', '/USDT'),
                                    'CoinEx': symbol.replace('USDT', '/USDT'),
                                }[exchange]
                                # Для каждого типа стакана (asks/bids)
                                for order_type in ['asks', 'bids']:
                                    # Добавляем новый снепшот
                                    snapshot = {
                                        'data': orderbook[order_type],
                                        'timestamp': datetime.now().timestamp(),
                                        'lastUpdate': orderbook.get('lastUpdate')
                                    }
                                    
                                    self.orderbook_history[exchange][symbol_formatted][order_type].append(snapshot)
                                    
                                    # Оставляем только 18 последних снепшотов
                                    if len(self.orderbook_history[exchange][symbol_formatted][order_type]) > 18:
                                        self.orderbook_history[exchange][symbol_formatted][order_type].pop(0)
                            except Exception as e:
                                #print(f"Ошибка при распаковке ордербуков: {e}")

                
                await asyncio.sleep(4)  # Обновление каждые 5 секунд
            except Exception as e:
                #print(f"Ошибка при обновлении локальных ордербуков: {e}")
                await asyncio.sleep(4) """


    async def collect_trades_all_exs(self, symbol_formatted, exchange):
        try:
            async with sem:
 
                symbol = {
                    'Kucoin': symbol_formatted.replace('-USDT', '/USDT'), 
                    'Gate': symbol_formatted.replace('_USDT', '/USDT'),
                    'BingX': symbol_formatted.replace('-USDT', '/USDT'),
                    'HTX': symbol_formatted.replace('usdt', '/USDT').upper(),
                    'Bitmart': symbol_formatted.replace('_USDT', '/USDT'),
                    'MEXC': symbol_formatted.replace('USDT', '/USDT'),
                    'Bitget': symbol_formatted.replace('USDT', '/USDT'),
                    'Bybit': symbol_formatted.replace('USDT', '/USDT'),
                    'CoinEx': symbol_formatted.replace('USDT', '/USDT'),
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
                                        self.results[exchange][symbol] = formatted_trades 
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
                                        self.results[exchange][symbol] = formatted_trades
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
                                        self.results[exchange][symbol] = formatted_trades
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

                                        self.results[exchange][symbol] = formatted_trades 
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

                                        self.results[exchange][symbol] = formatted_trades
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
                                        self.results[exchange][symbol] = formatted_trades
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
                                        self.results[exchange][symbol] = formatted_trades
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
                                        self.results[exchange][symbol] = formatted_trades 
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
                                        self.results[exchange][symbol] = formatted_trades 
                                    except Exception as e:
                                        #print(f"{exchange}: {e}")
                                        pass
                        else:
                            #print(f"{exchange}: {response}")
                            pass

                if exchange in self.results and symbol in self.results[exchange]:
                    # Создаем снепшот с временной меткой
                    snapshot = {
                        'trades': self.results[exchange][symbol],
                        'timestamp': datetime.now().timestamp()
                    }
                    
                    # Добавляем снепшот в историю
                    self.trades_history[exchange][symbol].append(snapshot)
                    
                    # Оставляем только 2 последних снепшота
                    if len(self.trades_history[exchange][symbol]) > 2:
                        self.trades_history[exchange][symbol].pop(0)

        except Exception as e:
            ##print(f"{exchange}: {e}")
            pass

    async def detect_market_makers(self): 
        while True:
            try:
                # Создаем список задач для асинхронной обработки бирж
                exchange_tasks = []
                
                for exchange in self.allExchanges:
                    # Проверяем наличие токенов у биржи
                    if exchange not in self.trades_history:
                        continue
                        
                    exchange_tasks.append(asyncio.create_task(self.process_exchange(exchange)))
                
                # Запускаем все задачи параллельно
                await asyncio.gather(*exchange_tasks)

                if len(self.current_signals) > 0:
                    compressed_snap = gzip.compress(pickle.dumps(self.current_signals))
                    cache.set('mm_symbols', compressed_snap, 300)
                    print('Список успешно загружен', self.current_signals)

                # Ждем перед следующей проверкой
                await asyncio.sleep(90)  # Проверка каждые 90 секунд
                
            except Exception as e:
                ##print(f"Ошибка в detect_market_makers: {e}")
                await asyncio.sleep(5)
    
    async def process_exchange(self, exchange):
        """Асинхронная обработка одной биржи"""
        symbol_tasks = []
        
        for symbol in self.trades_history[exchange]:
            # Проверяем наличие как минимум 2 снепшотов сделок
            if len(self.trades_history[exchange][symbol]) < 2:
                continue
                        

            symbol_tasks.append(asyncio.create_task(self.process_symbol(exchange, symbol)))
        
        # Запускаем обработку всех символов параллельно
        await asyncio.gather(*symbol_tasks)

                       
    async def process_symbol(self, exchange, symbol):
        """Асинхронная обработка одного символа"""
        # Получаем последние два снепшота сделок
        old_snapshot = self.trades_history[exchange][symbol][0]
        new_snapshot = self.trades_history[exchange][symbol][1]
        
        # Находим новые сделки (которые есть только в новом снепшоте)
        old_trades_times = {trade['time'] for trade in old_snapshot['trades']}
        new_trades = [trade for trade in new_snapshot['trades'] if trade['time'] not in old_trades_times]
        
        # Создаем задачи для обработки всех новых сделок
        trade_tasks = []
        for trade in new_trades:
            # Передаем информацию о текущих сигналах
            trade_tasks.append(asyncio.create_task(
                self.process_trade(exchange, symbol, trade)
            ))
        
        # Запускаем обработку всех сделок параллельно
        await asyncio.gather(*trade_tasks)
        
    async def process_trade(self, exchange, symbol, trade):
        """Асинхронная обработка одной сделки"""
        # Получаем цену и объем текущей сделки
        trade_price = float(trade['price'])
        trade_volume = float(trade['qty'])
        
        # Проверяем объем сделки (> $100)
        trade_volume_usd = trade_price * trade_volume
        if trade_volume_usd <= 50:
            return
        
        # Получаем временную метку сделки
        trade_time = int(trade['time']) / 1000  # Переводим в секунды
        
        # Проверяем наличие стаканов
        if symbol not in self.orderbook_history[exchange]:
            return
        
        order_type = trade['orderbook_type']  # asks или bids
        orderbook_snapshots = self.orderbook_history[exchange][symbol][order_type]
        
        if len(orderbook_snapshots) < 2:
            return
        
        # Ищем снепшоты стаканов до и после сделки
        before_snapshot = None
        min_before_diff = float('inf')
        after_snapshot = None
        min_after_diff = float('inf')
        
        for snapshot in orderbook_snapshots:
            # Для снепшотов до сделки
            if snapshot['timestamp'] < trade_time:
                time_diff = trade_time - snapshot['timestamp']
                if time_diff < min_before_diff:
                    min_before_diff = time_diff
                    before_snapshot = snapshot
            # Для снепшотов после сделки
            elif snapshot['timestamp'] > trade_time:
                time_diff = snapshot['timestamp'] - trade_time
                if time_diff < min_after_diff:
                    min_after_diff = time_diff
                    after_snapshot = snapshot
        
        # Если не нашли подходящие снепшоты, пропускаем
        if not before_snapshot or not after_snapshot:
            return
        
        # Проверяем, идентичны ли снепшоты до и после сделки
        orderbook_unchanged = True
        
        try:
            first_order_price_after = float(after_snapshot['data'][0][0])
            first_order_volume_before = float(before_snapshot['data'][0][1])
            
            if trade_price == first_order_price_after and first_order_volume_before > trade_volume:
                return
        except Exception as e:
            #print(f"Ошибка при проверке первого ордера: {e}")
            pass

        if len(before_snapshot['data']) != len(after_snapshot['data']):
            orderbook_unchanged = False
        else:
            # Проверяем каждый уровень цены
            for i in range(len(before_snapshot['data'])):
                before_price = float(before_snapshot['data'][i][0])
                before_volume = float(before_snapshot['data'][i][1])
                after_price = float(after_snapshot['data'][i][0])
                after_volume = float(after_snapshot['data'][i][1])
                
                # Если цена или объем изменились, стакан изменился
                if before_price != after_price or before_volume != after_volume:
                    orderbook_unchanged = False
                    break
        
        # Если стакан не изменился после сделки объемом более $100, это маркет-мейкер
        try:
            orderbook_spread = 0

            asks_book = self.orderbook_history[exchange][symbol]['asks'][-1]['data'][0][0]
            bids_book = self.orderbook_history[exchange][symbol]['bids'][-1]['data'][0][0]
            orderbook_spread_math = ((asks_book/bids_book)-1)*100
    

            if orderbook_unchanged and orderbook_spread_math > 1:
                ##print(f"Обнаружен потенциальный маркет-мейкер: биржа={exchange}, символ={symbol}, сделка={trade}")
                
                # Добавляем информацию о причине обнаружения в market_maker_info
                market_maker_info = {
                    "exchange": exchange,
                    "symbol": symbol,
                    "trade_price": trade_price,
                    "trade_volume": trade_volume,
                    "trade_time": trade_time,
                    "detection_reason": "orderbook_unchanged",
                    "detected_at": datetime.now().timestamp()
                }
                key = [exchange, symbol, datetime.now().timestamp()]
                
                try:
                    #key = f"market_maker_{exchange}_{symbol}_{trade_time}"
                    #await self.save_to_cache(key, market_maker_info)
                    # Добавляем сигнал в текущие сигналы
                    if exchange not in self.current_signals[symbol]:
                        self.current_signals[symbol].append(exchange)
                    
                except Exception as e:
                    ##print(f"Ошибка при сохранении информации о маркет-мейкере: {e}")
                    pass
        except Exception as e:
            #print(f"orderbook_spread_math error: {e}")
            pass



    async def check_market_maker_signals(self):
        await asyncio.sleep(270)
        """Проверка накопленных сигналов и отправка уведомлений"""
        while True:
            for key, signals in self.potential_market_makers.items():
                if len(signals) >= 3:
                    
                    # Сохраняем информацию о маркет-мейкере
                    market_maker_info = {
                        "key": key,
                        "signal": signals[2],
                        "detected_at": datetime.now().timestamp()
                    }
                    
                    try:
                        #await self.save_to_cache(f"market_maker_alert_{key}", market_maker_info)
                        #print(f"Signal {key}: {market_maker_info}")
                        # Очищаем сигналы после отправки уведомления
                        self.potential_market_makers[key] = []
                    except Exception as e:
                        ##print(f"Ошибка при сохранении информации о маркет-мейкере: {e}")
                        pass
            

            await asyncio.sleep(270)



    async def save_to_cache(self, key, data):
        """Асинхронное сохранение в кэш"""
        # Используем асинхронный клиент кэша, если доступен
        # Иначе оборачиваем синхронную операцию в executor
        return await asyncio.to_thread(
            cache.set, 
            key, 
            json.dumps(data), 
            expire=86400  # Хранить 24 часа
        )


    async def run(self):
        while True:
            try:
                start = datetime.now()
                tasks = []
                for exchange in self.allExchanges:
                    symbols = self.local_orderbooks.get(exchange, {}).keys()

                    if len(symbols)>0:
                        for symbol in symbols:
                            tasks.append(asyncio.create_task(self.collect_trades_all_exs(symbol, exchange)))


                await asyncio.gather(*tasks)   
                end = datetime.now()     


                await asyncio.sleep(1)        
            except Exception as e:
                #print(f"Error run {e}")
                await asyncio.sleep(1)


async def main():
    collector = DataTokensCollector()
    
    try:
        tasks = [
            asyncio.create_task(collector.update_local_orderbooks()),
            asyncio.create_task(collector.detect_market_makers()),
            asyncio.create_task(collector.run())
        ]
        await asyncio.gather(*tasks)

        
    except KeyboardInterrupt:
        #print("Получен сигнал остановки, завершаем работу...")
        pass



if __name__ == "__main__":
    asyncio.run(main())