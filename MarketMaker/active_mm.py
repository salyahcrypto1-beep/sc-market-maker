from portable.freshness import book_is_fresh, fresh_books
import ast
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

# Инициализация логирования
os.makedirs('logs', exist_ok=True)
logging.basicConfig(filename='logs/process_main.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

from pymemcache.client import base

base_db = motor.AsyncIOMotorClient("mongodb://mongo:27017", serverSelectionTimeoutMS=10000)
tokens = base_db.renderBase.tradePairs

cache = base.Client(('memcached', 11211))

# Загрузка переменных окружения
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")


class OrderBookAnalyzer:
    def __init__(self):
        self.allExchanges: List[str] = ['Kucoin', 'Gate', 'MEXC', 'Bitget', 'Bybit', 'BingX', 'HTX', 'CoinEx', 'Bitmart']
        self.local_orderbooks: Dict[str, Dict] = {}
        self.passive_mm: Dict[str, Dict] = {}
        self.tokens_exs: Dict[str, Dict] = {}
        self.previous_snapshots: Dict[str, Dict] = {}
        self.pattern_counts: Dict[str, Dict] = defaultdict(lambda: {'token': '', 'exchange': '', 'count': 0})
        self.initial_filtered_orders = {}

        # Новые поля
        self.five_minute_data = {}
        self.active_mm_signals = {}
        self.users = []
        self.mm_users = []
        self.mm_users_ids = []
        self.user_pages = {}
        self.current_signals = defaultdict(list)

        self.active_users = []

        # Изменяем интервал на 60 секунд
        self.scheduler = AsyncIOScheduler(timezone='UTC')
        self.db_renderBase = base_db.renderBase

        now = datetime.now()
        next_run = now.replace(microsecond=0)
        seconds_since_start = (next_run.minute * 60 + next_run.second)
        seconds_to_add = 240 - (seconds_since_start % 240)
        next_run = next_run + timedelta(seconds=seconds_to_add)

        # Планируем задачи
        self.scheduler.add_job(
            self.report_patterns,
            'interval',
            seconds=30
        )

        self.scheduler.add_job(
            self.set_current_users,
            'interval',
            seconds=10
        )

        self.scheduler.start()

    async def get_all_symbols_data(self):
        all_symbols_data = await self.db_renderBase.AllSymbolsData.find().to_list(length=None)

        return {item['symbol']: item['currency'] for item in all_symbols_data}

    async def update_local_orderbooks(self):
        """Обновление локальных ордербуков из кэша"""
        while True:
            print("Обновление локальных ордербуков из кэша")
            try:
                for exchange in self.allExchanges:
                    key = f'orderbook_{exchange}_full_snapshot'
                    value = cache.get(key)
                    if value:
                        data = pickle.loads(gzip.decompress(value))
                        data = fresh_books(data)
                        self.local_orderbooks[exchange] = data
                    else:
                        await asyncio.sleep(2)
                        key = f'orderbook_{exchange}_full_snapshot'
                        value = cache.get(key)
                        if value:
                            data = pickle.loads(gzip.decompress(value))
                            data = fresh_books(data)
                            self.local_orderbooks[exchange] = data

                await asyncio.sleep(5)
            except Exception as e:
                print(f"Ошибка при обновлении локальных ордербуков: {e}")

    async def get_socket_orderbook(self, exchange: str, symbol: str):
        """Получение ордербука для заданной биржи и токена"""
        try:
            orderbook = self.local_orderbooks.get(exchange, {}).get(symbol)
            if orderbook and book_is_fresh(orderbook):
                return {
                    'bids': list(orderbook['bids'].items()) if isinstance(orderbook['bids'], OrderedDict) else orderbook['bids'],
                    'asks': list(orderbook['asks'].items()) if isinstance(orderbook['asks'], OrderedDict) else orderbook['asks'],
                    'lastUpdate': orderbook['lastUpdate']
                }
            return None
        except Exception as e:
            print(f"Ошибка при получении ордербука: {e}")
            return None

    async def analyze_orderbook(self, exchange: str, token: str):
        try:
            orderbook = await self.get_socket_orderbook(exchange, token)
            if not orderbook:
                return

            key = f"{token}|{exchange}"
            current_snapshot = orderbook

            if key not in self.previous_snapshots:
                self.previous_snapshots[key] = current_snapshot
                return

            previous_snapshot = self.previous_snapshots[key]

            # Проверяем наличие дублированных объемов в первых 10 ордерах
            def has_duplicate_volumes(orders):
                volumes = [float(order[1]) for order in orders[:10]]
                return len(volumes) != len(set(volumes))

            # Пропускаем анализ если найдены дубли объемов
            if (has_duplicate_volumes(current_snapshot['asks']) or
                    has_duplicate_volumes(current_snapshot['bids']) or
                    has_duplicate_volumes(previous_snapshot['asks']) or
                    has_duplicate_volumes(previous_snapshot['bids'])):
                return

            prev_asks = previous_snapshot['asks'][:10]
            prev_bids = previous_snapshot['bids'][:10]
            curr_asks = current_snapshot['asks'][:10]
            curr_bids = current_snapshot['bids'][:10]

            if prev_asks == curr_asks and prev_bids == curr_bids:
                return

            if key not in self.initial_filtered_orders:
                await asyncio.gather(
                    self.second_stage_comparison(prev_asks, curr_asks, token, exchange, "asks"),
                    self.second_stage_comparison(prev_bids, curr_bids, token, exchange, "bids")
                )
            else:
                await asyncio.gather(
                    self.third_stage_comparison(self.initial_filtered_orders[key]['asks'], curr_asks, token, exchange),
                    self.third_stage_comparison(self.initial_filtered_orders[key]['bids'], curr_bids, token, exchange)
                )

            self.previous_snapshots[key] = current_snapshot

        except Exception as e:
            print(f"Ошибка в analyze_orderbook: {e}")

    async def second_stage_comparison(self, filtered_prev, filtered_curr, token, exchange, side):
        """Первичное сравнение и сохранение отфильтрованных ордеров"""
        key = f"{token}|{exchange}"
        filtered_orders = []

        for prev_order in filtered_prev:
            x1_prev, y1_prev = float(prev_order[1]), float(prev_order[0])
            for curr_order in filtered_curr:
                x1_curr, y1_curr = float(curr_order[1]), float(curr_order[0])
                if x1_prev == x1_curr and not (y1_prev == y1_curr):
                    filtered_orders.append(prev_order)

        if filtered_orders:
            if key not in self.initial_filtered_orders:
                self.initial_filtered_orders[key] = {
                    'asks': [],
                    'bids': []
                }

            self.initial_filtered_orders[key][side] = filtered_orders

    async def third_stage_comparison(self, initial_filtered, current_orders, token, exchange):
        """Последующие сравнения с изначально отфильтрованными ордерами"""
        key = f"{token}|{exchange}"
        if key not in self.pattern_counts:
            self.pattern_counts[key] = {
                'token': token,
                'exchange': exchange,
                'orders': defaultdict(int)
            }

        for filtered_order in initial_filtered:
            order_key = str(filtered_order)
            x1_filtered = float(filtered_order[1])
            for curr_order in current_orders:
                x1_curr, y1_curr = float(curr_order[1]), float(curr_order[0])
                if x1_filtered == x1_curr and not (float(filtered_order[0]) == y1_curr):
                    self.pattern_counts[key]['orders'][order_key] += 1

    async def report_patterns(self):
        print('Начало report_patterns')
        try:
            token_data = {}
            for key, data in self.pattern_counts.items():
                if data['orders']:  # Проверяем наличие ордеров
                    if key not in token_data:
                        token_data[key] = []
                    token_data[key].append(data['orders'])

            # Сохраняем данные в five_minute_data
            for key, orders in token_data.items():
                if key not in self.five_minute_data:
                    self.five_minute_data[key] = []
                self.five_minute_data[key].append(orders)

            # Очищаем данные для следующего цикла
            self.pattern_counts.clear()
            self.initial_filtered_orders.clear()

        except Exception as e:
            print(f"Ошибка в report_patterns: {e}")

    async def five_minute_analysis(self):
        print('Начало five_minute_analysis')
        asl = await self.get_all_symbols_data()
        users = self.users

        try:
            if not self.five_minute_data:
                return

            message = ""
            current_length = len(message)
            part = 1

            # Копируем текущие данные для анализа
            current_data = self.five_minute_data.copy()

            for key, data_list in current_data.items():
                try:
                    if len(data_list) < 3:
                        continue

                    token, exchange = key.split('|')

                    # Инициализируем словарь для подсчета общего количества движений ордеров
                    order_total_counts = defaultdict(int)
                    max_order = None
                    max_count = 0

                    # Проходим по данным за последние 3 минуты
                    for minute_data in data_list[-3:]:
                        for orders in minute_data:
                            for order, count in orders.items():
                                order_total_counts[order] += count
                                if order_total_counts[order] > max_count:
                                    max_count = order_total_counts[order]
                                    max_order = order

                    if max_count >= 2:

                        # Проверяем присутствие max_order в каждой минуте
                        present_in_all = all(
                            any(max_order in orders for orders in minute_data)
                            for minute_data in data_list[-3:]
                        )

                        if present_in_all:
                            source_orderbook = await self.get_socket_orderbook(exchange, token)
                            if not source_orderbook or not source_orderbook.get('asks') or not source_orderbook.get('bids'):
                                continue

                            is_in_asks = any(str(order) == max_order for order in source_orderbook['asks'][:10])

                            token_formatted = {
                                'OKX': token.replace('-USDT', '/USDT'),
                                'Kucoin': token.replace('-USDT', '/USDT'),
                                'Gate': token.replace('_USDT', '/USDT'),
                                'BingX': token.replace('-USDT', '/USDT'),
                                'HTX': token.replace('USDT', 'USDT').lower(),
                                'Bitmart': token.replace('_USDT', '/USDT'),
                                'MEXC': token.replace('USDT', '/USDT'),
                                'Bitget': token.replace('USDT', '/USDT'),
                                'Bybit': token.replace('USDT', '/USDT'),
                                'CoinEx': token.replace('USDT', '/USDT'),
                            }[exchange]

                            curr_token_traded = []
                            for other_ex in asl[token_formatted].keys():
                                if other_ex == "MEXC Global":
                                    if not ("MEXC" == exchange):
                                        curr_token_traded.append("MEXC")
                                else:
                                    if not (other_ex == exchange):
                                        curr_token_traded.append(other_ex)

                            transfer = {}
                            try:
                                if exchange == "MEXC":
                                    for net in asl[token_formatted]['MEXC Global']['networks'].keys():
                                        transfer[net] = asl[token_formatted]['MEXC Global']['networks'][net]
                                else:
                                    for net in asl[token_formatted][exchange]['networks'].keys():
                                        transfer[net] = asl[token_formatted][exchange]['networks'][net]

                            except Exception as e:
                                print(f"Нет данных в asl", e)
                                pass

                            link_formatted = {
                                'Kucoin': f"https://www.kucoin.com/ru/trade/{token}",
                                'Gate': f"https://www.gate.io/ru/trade/{token}",
                                'BingX': f"https://bingx.com/ru-ru/spot/{token}/",
                                'HTX': f"https://www.htx.com/ru-ru/trade/{token.replace('usdt', '_usdt')}?type=spot",
                                'Bitmart': f"https://www.bitmart.com/trade/ru-RU?symbol={token}",
                                'MEXC': f"https://www.mexc.com/ru-RU/exchange/{token.replace('USDT', '_USDT')}",
                                'Bitget': f"https://www.bitget.com/ru/spot/{token}",
                                'Bybit': f"https://www.bybit.com/en/trade/spot/{token.replace('USDT', '')}/USDT",
                                'CoinEx': f"https://www.coinex.com/ru/exchange/{token.replace('USDT', '-USDT')}",
                            }

                            max_order_list = ast.literal_eval(max_order)
                            max_order_volume = float(max_order_list[1])
                            max_order_price = float(max_order_list[0])
                            dollar_value = max_order_volume * max_order_price
                            action = "📗" if not (is_in_asks) else "📕"

                            # Подсчитываем общее количество движений всех ордеров MM (исключая ведущий ордер)
                            total_mm_orders = sum(count for order, count in order_total_counts.items())

                            last_time = datetime.now()
                            time_str = last_time.strftime('%H:%M:%S')

                            token_message = (
                                f"Токен: {token_formatted}\n\n"
                                f"Замечен на бирже: [{exchange}]({link_formatted[exchange]})\n"
                                f"Всего ордеров MM: {total_mm_orders}шт\n\n"
                                f"Ведущий ордер {action}:\n"
                                f"Движений: {max_count} движения\n"
                                f"Объем: {max_order_volume:.2f} токенов | {dollar_value:.2f}$\n\n"
                                f"Биржи: "
                            )

                            if len(curr_token_traded) > 1:
                                exchanges_links = []
                                for ex_trade in curr_token_traded:
                                    exchanges_links.append(f"[{ex_trade}]({link_formatted.get(ex_trade, '')})")
                                token_message += " / ".join(exchanges_links) + "\n\n"
                            else:
                                token_message += f"[{exchange}]({link_formatted[exchange]})\n\n"

                            token_message += f"Время обнаружения: {time_str} МСК\n\n"

                            for net in transfer.keys():
                                withdraw = '🟩' if transfer[net]['withdraw'] else '🟥'
                                deposit = '🟩' if transfer[net]['deposit'] else '🟥'
                                token_message += f"{net}: Ввод: {deposit} / Вывод: {withdraw}\n"

                            # Добавляем дополнительные поля в active_mm_signals
                            self.active_mm_signals[key] = {
                                'message': token_message,
                                'time': last_time,
                                'action': action,
                                'dollar_value': dollar_value,
                                'token_formatted': token_formatted,
                                'exchange': exchange
                            }
                            try:
                                if exchange not in self.current_signals[token_formatted]:
                                    self.current_signals[token_formatted].append(exchange)
                            except Exception as e:
                                print(f"Ошибка в self.current_signals set: {e}")
                                pass

                            """ async def send_all_users(message_text, user_id, key):
                                # Get the user's saveList from mm_user collection
                                user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
                                save_list = user_model.get('saveList', [])
                                value = self.active_mm_signals[key]
                                token_formatted = value['token_formatted']
                                exchange = value['exchange']
                                token_key = f'{token_formatted}|{exchange}'

                                if token_key in save_list:
                                    button_text = 'Удалить ❌'
                                    callback_data = f'delete_{token_key}'
                                else:
                                    button_text = 'Сохранить ✅'
                                    callback_data = f'save_{token_key}'

                                markup = telebot.types.InlineKeyboardMarkup()
                                button = telebot.types.InlineKeyboardButton(button_text, callback_data=callback_data)
                                markup.add(button)

                                await self.bot.send_message(user_id, message_text, parse_mode='Markdown', disable_web_page_preview=True, reply_markup=markup)

                            for mm_user in self.mm_users:
                                if mm_user.get('monitoring', True):
                                    user_id = mm_user['id']
                                    await send_all_users(token_message, user_id, key) """

                except Exception as e:
                    print(f"Ошибка в five_minute_analysis local: {e}")
                    pass

            # Полный сброс состояния после анализа
            self.initial_filtered_orders.clear()
            self.previous_snapshots.clear()
            self.local_orderbooks.clear()
            self.passive_mm.clear()
            self.tokens_exs.clear()

            logging.info("Состояние анализатора обновлено")
            print("Состояние анализатора обновлено")

        except Exception as e:
            print(f"Ошибка в five_minute_analysis global: {e}")
            pass

    """ async def save_delete_callback(self, call):
        try:
            user_id = call.message.chat.id
            data = call.data
            if data.startswith('save_'):
                token_key = data[5:]
                # Add token_key to user's saveList
                user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
                save_list = user_model.get('saveList', [])
                if token_key not in save_list:
                    save_list.append(token_key)
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
                    save_list.remove(token_key)
                    await base_db.userdata.mm_user.update_one({'id': int(user_id)}, {'$set': {'saveList': save_list}})
                    await base_db.userdata.user.update_one({'id': int(user_id)}, {'$set': {'save_mm_signals': save_list}})
                    # Update the button to 'Сохранить'
                    markup = telebot.types.InlineKeyboardMarkup()
                    button = telebot.types.InlineKeyboardButton('Сохранить ✅', callback_data=f'save_{token_key}')
                    markup.add(button)
                    await self.bot.edit_message_reply_markup(user_id, call.message.message_id, reply_markup=markup)
                await self.bot.answer_callback_query(call.id, text='Токен удален.')
        except Exception as e:
            print(f"Ошибка в save_delete_callback: {e}")
            await self.bot.answer_callback_query(call.id, text='Произошла ошибка.', show_alert=True) """

    """ async def show_full_message(self, call):
        try:
            key = call.data[5:]  # Remove 'show_'
            if key in self.active_mm_signals:
                user_id = call.message.chat.id
                user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
                save_list = user_model.get('saveList', [])
                value = self.active_mm_signals[key]
                message_text = value['message']
                token_formatted = value['token_formatted']
                exchange = value['exchange']
                token_key = f'{token_formatted}|{exchange}'

                if token_key in save_list:
                    button_text = 'Удалить ❌'
                    callback_data = f'delete_{token_key}'
                else:
                    button_text = 'Сохранить ✅'
                    callback_data = f'save_{token_key}'

                markup = telebot.types.InlineKeyboardMarkup()
                button = telebot.types.InlineKeyboardButton(button_text, callback_data=callback_data)
                markup.add(button)

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
            ) """

    """ async def navigate_pages(self, call):
        try:
            if str(call.message.chat.id) in self.users:
                user_id = call.message.chat.id
                data = call.data.split('_')
                if len(data) == 2:
                    action = data[1]
                    current_page = self.user_pages.get(user_id, 1)
                    total_items = len(self.active_mm_signals)
                    items_per_page = 10
                    total_pages = math.ceil(total_items / items_per_page)
                    if action == 'next':
                        if current_page < total_pages:
                            current_page += 1
                    elif action == 'prev':
                        if current_page > 1:
                            current_page -= 1
                    self.user_pages[user_id] = current_page
                    await self.generate_active_mm_list(user_id, page=current_page, call=call)
            else:
                await self.bot.answer_callback_query(
                    call.id,
                    text='У вас нет доступа',
                    show_alert=True
                )
        except Exception as e:
            print(f"Ошибка в navigate_pages: {e}")
            await self.bot.answer_callback_query(
                call.id,
                text='Произошла ошибка при навигации по страницам.',
                show_alert=True
            ) """

    """ async def navigate_favorites(self, call):
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
            ) """

    """ async def delete_favorite(self, call):
        try:
            user_id = call.message.chat.id
            token_key = call.data[10:]  # Remove 'deletefav_'
            # Remove token_key from user's saveList
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            save_list = user_model.get('saveList', [])
            if token_key in save_list:
                save_list.remove(token_key)
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
            ) """

    """ async def active_mm_list(self, message):
        if str(message.chat.id) in self.users:
            try:
                self.user_pages[message.chat.id] = 1  # Initialize page number
                await self.generate_active_mm_list(message.chat.id, page=1, message=message)
            except Exception as e:
                print(f"Ошибка в active_mm_list: {e}")
                await self.bot.send_message(
                    message.chat.id,
                    'Произошла ошибка при получении списка активных ММ.'
                )
        else:
            await self.bot.send_message(
                message.chat.id,
                'У вас нет доступа'
            ) """

    """ async def generate_active_mm_list(self, chat_id, page=1, message=None, call=None):
        try:
            # Get the user's saveList
            user_model = await base_db.userdata.mm_user.find_one({'id': int(chat_id)})
            save_list = user_model.get('saveList', [])

            # Создаем разметку для инлайн-клавиатуры
            markup = telebot.types.InlineKeyboardMarkup(row_width=1)
            now = datetime.now()
            to_remove = []

            # Собираем активные сигналы в список и удаляем устаревшие
            active_signals = []
            for key, value in self.active_mm_signals.items():
                last_time = value['time']
                # Проверяем время жизни сигнала (15 минут)
                if (now - last_time).total_seconds() > 900:
                    to_remove.append(key)
                    continue
                active_signals.append((key, value))

            # Удаляем устаревшие сигналы
            for key in to_remove:
                del self.active_mm_signals[key]

            total_items = len(active_signals)
            if total_items == 0:
                await self.bot.send_message(chat_id, 'Нет активных ММ.')
                return

            # Сортируем активные сигналы по dollar_value в порядке убывания
            active_signals.sort(key=lambda x: x[1].get('dollar_value', 0), reverse=True)

            items_per_page = 10
            total_pages = math.ceil(total_items / items_per_page)

            if page > total_pages:
                page = total_pages
            elif page < 1:
                page = 1

            start_index = (page - 1) * items_per_page
            end_index = start_index + items_per_page
            page_signals = active_signals[start_index:end_index]

            # Добавляем кнопки элементов
            for key, value in page_signals:
                token, exchange = key.split('|')
                token_formatted = {
                    'OKX': token.replace('-USDT', '/USDT'),
                    'Kucoin': token.replace('-USDT', '/USDT'),
                    'Gate': token.replace('_USDT', '/USDT'),
                    'BingX': token.replace('-USDT', '/USDT'),
                    'HTX': token.replace('USDT', 'USDT').lower(),
                    'Bitmart': token.replace('_USDT', '/USDT'),
                    'MEXC': token.replace('USDT', '/USDT'),
                    'Bitget': token.replace('USDT', '/USDT'),
                    'Bybit': token.replace('USDT', '/USDT'),
                    'CoinEx': token.replace('USDT', '/USDT'),
                }[exchange]
                action = value.get('action', '')
                last_time = value['time']
                dollar_value = value.get('dollar_value', 0)
                button_text = f"{token_formatted}: {exchange} {action} | {dollar_value:.2f}$"

                token_key = f'{token_formatted}|{exchange}'
                if token_key in save_list:
                    button_text += ' ⭐'

                button = telebot.types.InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"show_{key}"
                )
                markup.add(button)

            # Кнопки навигации
            navigation_buttons = []
            if page > 1:
                navigation_buttons.append(telebot.types.InlineKeyboardButton('⬅️', callback_data='page_prev'))
            if page < total_pages:
                navigation_buttons.append(telebot.types.InlineKeyboardButton('➡️', callback_data='page_next'))

            if navigation_buttons:
                markup.row(*navigation_buttons)

            # Сообщение с номером страницы
            message_text = f'Активные ММ (Стр {page}/{total_pages}):'

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
                ) """

    """ async def show_favorites(self, message):
        user_id = message.chat.id
        user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
        save_list = user_model.get('saveList', [])

        if not save_list:
            await self.bot.send_message(user_id, 'У вас нет сохраненных токенов.')
            return

        self.user_pages[user_id] = 1  # Initialize page number
        await self.generate_favorites_list(user_id, page=1) """

    """ async def generate_favorites_list(self, user_id, page=1, call=None):
        try:
            user_model = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            save_list = user_model.get('saveList', [])

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

            markup = telebot.types.InlineKeyboardMarkup(row_width=2)

            for token_key in page_tokens:
                token_formatted, exchange = token_key.split('|')
                button_text = f'{token_formatted} ({exchange})'
                delete_button = telebot.types.InlineKeyboardButton('❌', callback_data=f'deletefav_{token_key}')
                token_button = telebot.types.InlineKeyboardButton(button_text, callback_data='noop')
                markup.add(token_button, delete_button)

            # Pagination buttons
            navigation_buttons = []
            if page > 1:
                navigation_buttons.append(telebot.types.InlineKeyboardButton('⬅️', callback_data='fav_prev'))
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
                await self.bot.send_message(user_id, message_text, reply_markup=markup)
        except Exception as e:
            print(f"Ошибка в generate_favorites_list: {e}")
            if call:
                await self.bot.answer_callback_query(
                    call.id,
                    text='Произошла ошибка при обновлении списка.',
                    show_alert=True
                ) """

    """ async def start(self, message):
        user_id = str(message.chat.id)

        if user_id in self.users:
            # Check if user exists in mm_user collection
            user_in_mm_user = await base_db.userdata.mm_user.find_one({'id': int(user_id)})
            if not user_in_mm_user:
                # Create user model
                user_model = {
                    'id': int(user_id),
                    'monitoring': True,
                    'saveList': []
                }
                await base_db.userdata.mm_user.insert_one(user_model)
                self.mm_users_ids.append(user_id)
                self.mm_users.append(user_model)

            # Build the reply keyboard
            markup = telebot.types.ReplyKeyboardMarkup(row_width=2)
            item1 = telebot.types.KeyboardButton('Избранное ⭐')
            item2 = telebot.types.KeyboardButton('Мониторинг 💻')
            item3 = telebot.types.KeyboardButton('Активные Ⓜ️Ⓜ️')
            markup.add(item1, item2, item3)
            await self.bot.send_message(message.chat.id, 'Выберите опцию:', reply_markup=markup)
            self.active_users.append(message.chat.id)
        else:
            await self.bot.send_message(message.chat.id, 'У вас нет доступа') """

    """ async def monitoring_menu(self, message):
        user_id = message.chat.id
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
        await self.bot.send_message(user_id, 'Настройки мониторинга:', reply_markup=markup) """

    """ async def monitoring_callback(self, call):
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
            await self.bot.answer_callback_query(call.id, text='Мониторинг выключен.') """

    async def run_analysis(self):
        """Запуск анализа для всех токенов и бирж"""
        while True:
            
            print("Запуск анализа для всех токенов и бирж")
            tasks = []
            for exchange in self.allExchanges:

                symbols = self.local_orderbooks.get(exchange, {}).keys()

                for symbol in symbols:
                    # Преобразуем обратно формат символа
                    tasks.append(self.analyze_orderbook(exchange, symbol))
            await asyncio.gather(*tasks)
            await asyncio.sleep(1)

    async def start_cycle_result(self):
        while True:
            try:
                await asyncio.sleep(240)
                await self.five_minute_analysis()
                if len(self.current_signals) > 0:
                    data_snep_signals = gzip.compress(pickle.dumps(self.current_signals))
                    cache.set('mm_symbols_active', data_snep_signals, 1000)
            except Exception as e:
                print(f"Ошибка в start_cycle_result: {e}")
                pass

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


async def main():
    analyzer = OrderBookAnalyzer()
    tasks = [
        asyncio.create_task(analyzer.update_local_orderbooks()),
        asyncio.create_task(analyzer.run_analysis()),
        asyncio.create_task(analyzer.start_cycle_result()),
    ]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
