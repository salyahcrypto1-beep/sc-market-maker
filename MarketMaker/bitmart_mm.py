import os
import aiohttp.client_exceptions
import aiohttp.connector
import aiohttp.http_exceptions
import motor.motor_asyncio as motor
import aiohttp,aiohttp_socks
import asyncio
import diskcache as dc
import random
import json,gzip,pickle
from telebot import asyncio_filters
import telebot.async_telebot as telebot
from telebot.async_telebot import types
import contextlib
from contextlib import AsyncExitStack
import requests
import time
from pymemcache.client import base

memcache = base.Client(('memcached', 11211))

bot = telebot.AsyncTeleBot(os.environ.get("TELEGRAM_BOT_TOKEN", ""))


cache = dc.Cache('../tmp/cache')
base = motor.AsyncIOMotorClient("mongodb://mongo:27017", serverSelectionTimeoutMS=10000)
tokens = base.renderBase.tradePairs
url = 'https://api-cloud.bitmart.com/spot/quotation/v3/trades?limit=50&symbol='
proxies = [None]

results = {}
detected_mm = {} 
ex_tokens = {
    'OKX': [],
    'Kucoin': [],
    'Gate': [],
    'MEXC Global': [],
    'Bitget': [],
    'Bybit': [],
    'BingX': [],
    'HTX': [],
    'CoinEx': [],
    'Bitmart': []
}


def get_cached_history():
    cached_data = cache.get('history_mexc')
    if cached_data:
        return pickle.loads(gzip.decompress(cached_data))
    return None

async def get_all_ex_tokens():
    try:
        keys = ['OKX', 'Kucoin', 'Gate', 'MEXC Global', 'Bitget', 'Bybit', 'BingX', 'HTX', 'CoinEx','Bitmart']
        all_tokens = await tokens.find().to_list(length=None)
        for ex in all_tokens:
            ex_tokens[ex['id']] = ex['tokens']
    except Exception as e:
        print(e, ex_tokens.keys())


async def mm_math_second_strategy(orders, first_token, token_ex_detected):
    try:
        if not isinstance(orders, list) or not orders:
            return None

        orders = orders[:50] if len(orders) > 50 else orders

        # Проверка времени последнего ордера
        current_time = int(time.time() * 1000)
        try:
            last_order_time = int(orders[-1]['time'])
        except (KeyError, IndexError):
            return None
        if current_time - last_order_time > 7200000:  # 2 часа в миллисекундах
            return None

        sequences = []
        current_sequence = []

        try:
            price = float(orders[0]['price'])
        except (KeyError, ValueError, IndexError):
            return None

        async def check_sequence(index_current_order, index_double_order):
            current_order_val = orders[index_current_order]
            double_order_val = orders[index_double_order]

            procent_volume = float(current_order_val['qty']) * 0.05
            difference = abs(float(current_order_val['qty']) - float(double_order_val['qty']))

            if difference <= procent_volume:
                if current_sequence and abs(float(double_order_val['qty']) - current_sequence[-1]) <= procent_volume:
                    current_sequence.append(float(double_order_val['qty']))
                    for seq in sequences:
                        if len(seq) >= len(current_sequence) and all(current_sequence[i] in seq for i in range(len(current_sequence))):
                            break
                    else:
                        if len(current_sequence) >= 5:
                            sequences.append(current_sequence[:])
                else:
                    if current_sequence:
                        for seq in sequences:
                            if len(seq) >= len(current_sequence) and all(current_sequence[i] in seq for i in range(len(current_sequence))):
                                break
                        else:
                            if len(current_sequence) >= 5:
                                sequences.append(current_sequence[:])
                    current_sequence.clear()
                    current_sequence.append(float(double_order_val['qty']))

        async def for_every_order(index_current_order):
            procent_volume = float(orders[index_current_order]['qty']) * 0.05
            await asyncio.gather(*[check_sequence(index_current_order, index_double_order) 
                                for index_double_order in range(index_current_order + 1, len(orders))])
            
            if current_sequence and len(current_sequence) >= 10:
                await asyncio.gather(*[check_sequence(index_current_order, i)
                                    for i in range(len(orders)) 
                                    if i < len(orders)])  # Добавлена проверка индекса
                sequences.append(current_sequence[:])
            current_sequence.clear()



        await asyncio.gather(*[for_every_order(index) for index in range(len(orders))])

        if current_sequence and len(current_sequence) >= 10:
            await asyncio.gather(*[check_sequence(len(orders)-1, i) # Используем последний валидный индекс
                                for i in range(len(sequences))
                                if len(sequences[i]) >= len(current_sequence) and 
                                all(current_sequence[j] in sequences[i] for j in range(len(current_sequence)))])
            sequences.append(current_sequence[:])


        sequences.sort(key=len, reverse=True)

        unique_sequences = []
        tasks = []
        for seq in sequences:
            is_unique = True
            for us in unique_sequences:
                if all(seq[j] in us for j in range(len(seq))):
                    is_unique = False
                    break
            if is_unique:
                unique_sequences.append(seq)

        if len(unique_sequences) >= 2:
            print(f"{first_token} Количество последовательностей больше 1: {len(unique_sequences)}")
            avg_values = []
            for seq in unique_sequences:
                avg_volume = sum(seq)/len(seq) if seq else 0
                avg_dollar = round(avg_volume * price, 2)
                avg_values.append([avg_dollar, len(seq)])
            detected_mm[first_token] = [len(unique_sequences), token_ex_detected, avg_values]  # Заменили unique_sequences на token_ex_detected
        else:
            print(f"{first_token} МаркетМекер не замечен")

        return None
    except Exception as e:
        print('mm_math_second_strategy Error', e)
        pass



async def collect_trades(token, session):
    try:
        async with session.get(f'{url}{token}') as response:
            if response.status == 200:
                result = await response.json()
                formatted_trades = [
                    {
                        'price': obj[2],
                        'qty': obj[3],
                        'time': str(obj[1])
                    } for obj in result['data']
                ]
                results[token] = formatted_trades 
                print(token, len(formatted_trades))  
    except Exception as e:
        print(f"Ошибка для токена {token}: {e}")
        pass

async def __main__():
    async with aiohttp.ClientSession() as session:  # Создаем одну общую сессию
        semaphore = asyncio.Semaphore(500)
        try:
            await get_all_ex_tokens()
            all_tokens = await tokens.find({'id':'Bitmart'}).to_list(length=None)
            
            tasks = []
            for i in range(0, len(all_tokens[0]['tokens']), semaphore._value):
                chunk = all_tokens[0]['tokens'][i:i + semaphore._value]
                async with semaphore:
                    tasks.extend([collect_trades(token, session) for token in chunk])
                await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []

            mm_tasks = []
            for token in results.keys():
                token_ex_detected = []
                for ex in ex_tokens.keys():
                    symbol_formatted = {
                        'OKX': token.replace('_USDT', '-USDT'),
                        'Kucoin': token.replace('_USDT', '-USDT'), 
                        'Gate': token.replace('_USDT', '_USDT'),
                        'BingX': token.replace('_USDT', '-USDT'),
                        'HTX': token.replace('_USDT', 'USDT').lower(),
                        'Bitmart': token.replace('_USDT', '_USDT'),
                        'MEXC Global': token.replace('_USDT', 'USDT'),
                        'Bitget': token.replace('_USDT', 'USDT'),
                        'Bybit': token.replace('_USDT', 'USDT'),
                        'CoinEx': token.replace('_USDT', 'USDT'),
                    }[ex]

                    if symbol_formatted in ex_tokens[ex]:
                        token_ex_detected.append(ex)

                if len(token_ex_detected) >= 2:
                    mm_tasks.append(mm_math_second_strategy(results[token], token, token_ex_detected))
            
            await asyncio.gather(*mm_tasks, return_exceptions=True)

            if detected_mm:
                message = ""
                current_length = len(message)
                part = 1

                key = f'Bitmart_MM'
                memcache.set(key, detected_mm.keys())
                
                for token, combinations in detected_mm.items():
                    token_link = f"https://www.bitmart.com/trade/ru-RU?symbol={token}"
                    exchanges = " / ".join(combinations[1])
                    
                    max_val_of_comb = 0
                    for value in combinations[2]:
                        if value[0] > max_val_of_comb:
                            max_val_of_comb = value[0]

                    if max_val_of_comb >= 5:
                        lvl_of_comb = '📗' if max_val_of_comb <= 10 else '📒' if (max_val_of_comb > 10 and max_val_of_comb <= 50) else '📕'  
                        cmc_link = f"https://coinmarketcap.com/currencies/{token.replace('_USDT', '').lower()}/"   

                        token_message = f"{lvl_of_comb} Bitmart: [{token}]({token_link}) | [СМС]({cmc_link})\n"
                        
                        if current_length + len(token_message) > 4000:
                            await bot.send_message('0', message, parse_mode='Markdown')
                            message = f"Часть {part}:\n\n"
                            current_length = len(message)
                            part += 1
                            
                        message += token_message
                        current_length += len(token_message)
                
                if message:
                    await bot.send_message('0', message, parse_mode='Markdown')
            else:
                await bot.send_message('0', "МаркетМейкеры не обнаружены")

        except Exception as e:
            print(f'Main func err: {e}')
            pass


if __name__ == '__main__':
    asyncio.run(__main__())

            

