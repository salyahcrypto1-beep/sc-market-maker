# SC Market Maker — подробная архитектура двух стратегий

Бот строит **две независимые гипотезы по публичным данным**: перемещение одинакового объёма между ценами и сохранение книги вокруг сделки. Затем объединяет кандидатов, добавляет межбиржевой контекст и отдаёт персональный список/мониторинг. Публичный L2-стакан агрегирует заявки по цене, поэтому обнаружение паттерна не идентифицирует конкретного участника.

## 1. Карта процессов и потоков данных

```mermaid
flowchart TB
    WS[Native exchange WS processes] --> C[(Full order-book snapshots)]
    C --> A[active_mm.OrderBookAnalyzer]
    C --> H[marketM: bounded book history]
    REST[Native public trade endpoints] --> T[marketM: normalize trades]
    H --> P[Match book before and after each trade]
    T --> P
    A --> AC[(mm_symbols_active)]
    P --> PC[(mm_symbols)]
    AC --> MERGE[MM_signals.update_mm_list]
    PC --> MERGE
    DB[(Persisted mm_data)] --> MERGE
    MERGE --> ROUTE[Inter-exchange context and quote difference]
    META[(Public network metadata)] --> ROUTE
    C --> ROUTE
    ROUTE --> FINAL[final_signals]
    FINAL --> LIST[Search, rank, paginate]
    FINAL --> MON[Favorites monitoring]
    USER[(Local user preferences)] --> LIST
    USER --> MON
    LIST --> TG[Personal Telegram bot]
    MON --> TG
```

`marketM.py` и `active_mm.py` не вызывают друг друга. Кэш — граница процессов, а не общий Python-объект. `MM_signals.py` объединяет результаты позднее; пропажа одного источника не превращает другой в подтверждение первой стратегии.

## 2. Активный MM: состояние на пару token|exchange

Источник: [`active_mm.py`](../MarketMaker/active_mm.py): `analyze_orderbook`, `second_stage_comparison`, `third_stage_comparison`.

```mermaid
stateDiagram-v2
    [*] --> NoBaseline
    NoBaseline --> Baseline: First fresh book
    Baseline --> Baseline: Duplicate sizes or unchanged book
    Baseline --> Candidate: Same size at different price
    Candidate --> Tracking: Save asks and bids separately
    Tracking --> Tracking: Same reference size moves again
    Tracking --> WindowReport: Periodic aggregation
    WindowReport --> Baseline: Clear candidate and counter maps
    WindowReport --> ThreeWindows: Append nonempty observations
    ThreeWindows --> Confirmed: Leader exists in all three windows and count at least two
    ThreeWindows --> Baseline: Evidence insufficient
```

Первые 10 уровней каждой стороны ограничивают объём сравнений. Ключ `token|exchange` отделяет наблюдения разных рынков; `previous_snapshots` хранит базу сравнения, `initial_filtered_orders` — кандидатов по сторонам, `pattern_counts` — повторения конкретного исходного `[price,quantity]`.

## 3. Устранение неоднозначности до сопоставления

```mermaid
flowchart TD
    B[Previous and current fresh books] --> TOP[Take first 10 asks and bids]
    TOP --> DUP{Duplicate quantities inside any of four sides?}
    DUP -- yes --> SKIP[Skip comparison]
    DUP -- no --> EQ{Both sides unchanged?}
    EQ -- yes --> SKIP
    EQ -- no --> HAS{Candidate set already exists?}
    HAS -- no --> ST2[Second stage: pair previous and current orders]
    HAS -- yes --> ST3[Third stage: compare stored references with current orders]
    ST2 --> SAVE[Separate candidate arrays for asks and bids]
    ST3 --> COUNT[Increment per-reference movement counters]
    SAVE --> PREV[Update previous snapshot]
    COUNT --> PREV
```

Если один размер встречается дважды, нельзя однозначно связать старый уровень с новым по количеству. Поэтому проверка дублей стоит **до** поиска совпадений. Сравнение каждой стороны ограничено 10×10 парами; это небольшая фиксированная матрица, а не сравнение всей глубины биржи.

На первом этапе совпадение означает `quantity_prev == quantity_current` и `price_prev != price_current`. На последующих этапах сравнение идёт с сохранённым первоначальным кандидатом. Это не проверка ID биржевого ордера: публичный поток его не предоставляет.

## 4. Подтверждение на нескольких окнах

Источник: `report_patterns`, `five_minute_analysis`, `start_cycle_result`.

```mermaid
flowchart TD
    C[Pattern counters] --> R[report_patterns every 30 seconds]
    R --> N{Nonempty observations?}
    N -- yes --> APP[Append to five_minute_data for this token and exchange]
    N -- no --> CLEAR[Clear current counters and candidates]
    APP --> CLEAR
    APP --> THREE{At least three stored windows?}
    THREE -- no --> WAIT[Wait]
    THREE -- yes --> SUM[Sum counts by reference order in last three windows]
    SUM --> MAX[Choose order with maximum aggregate count]
    MAX --> TH{Maximum count at least 2?}
    TH -- no --> WAIT
    TH -- yes --> ALL{Same reference occurs in every window?}
    ALL -- no --> WAIT
    ALL -- yes --> CUR[Read fresh source book and other exchange context]
    CUR --> OUT[Persist active-MM candidate]
```

Не путать имя `five_minute_analysis` с длительностью доказательства: report настроен на 30 секунд, `start_cycle_result` запускает анализ раз в 240 секунд, а анализ берёт три **сохранённых непустых** окна. Это не гарантия трёх непрерывных минут или пяти минут. Пропущенные пустые окна в исходной структуре не создают явных нулевых наблюдений. Присутствие в каждом окне отсеивает единичные всплески, но временную плотность нужно оценивать отдельно.

## 5. Вторая стратегия: публичная сделка и книга вокруг неё

Источник: [`marketM.py`](../MarketMaker/marketM.py): `update_local_orderbooks`, `collect_trades_all_exs`, `process_trade`.

```mermaid
sequenceDiagram
    participant W as Native WS cache
    participant H as Book history
    participant E as Public trades API
    participant A as Trade analyzer
    W->>H: Current asks / bids, sampled every 5 seconds
    H->>H: Keep last 18 snapshots per side
    E->>A: price, qty, time, affected side
    A->>A: Require price times qty above 50
    A->>H: Find nearest snapshot strictly before trade
    A->>H: Find nearest snapshot strictly after trade
    H->>A: Before / after or missing
    A->>A: Filter ordinary partial consumption of first level
    A->>A: Compare all stored levels exactly
    A->>A: Require current book spread above 1 percent
    A->>W: mm_symbols candidate
```

История ограничена 18 элементами на сторону: это ограничивает память и линейный поиск соседних снимков. При шаге 5 секунд ожидаемый горизонт порядка 90 секунд, но это не точная длительность при задержках. Timestamp истории ставится при считывании кэша, исходный `lastUpdate` хранится отдельно. Значит сопоставление идёт по sampled-наблюдениям, а не атомарному порядку всех событий биржи.

## 6. Все развилки process_trade

```mermaid
flowchart TD
    T[Trade price, qty, millisecond timestamp] --> USD{Notional above 50 USDT?}
    USD -- no --> DROP[Reject]
    USD -- yes --> HIST{At least two snapshots for affected side?}
    HIST -- no --> DROP
    HIST -- yes --> B[Find minimum time distance before trade]
    B --> A[Find minimum time distance after trade]
    A --> BOTH{Both snapshots found?}
    BOTH -- no --> DROP
    BOTH -- yes --> PART{Trade at new top price and old top size exceeds trade size?}
    PART -- yes --> DROP
    PART -- no --> LEN{Same number of levels?}
    LEN -- no --> DROP
    LEN -- yes --> EQ{Every price and quantity unchanged?}
    EQ -- no --> DROP
    EQ -- yes --> SP{Current ask divided by bid minus one above 1 percent?}
    SP -- no --> DROP
    SP -- yes --> ADD[Add exchange to symbol candidate set without duplication]
```

Порог действительно **50**, хотя старые комментарии говорили о 100. Условие первого уровня исключает часть обычных частичных исполнений. Остальная неизменность может указывать на восстановление ликвидности, но также зависит от дискретизации и качества ленты. Нельзя объявлять это доказанным «скрытым ордером» или гарантированным маркет-мейкером.

## 7. Объединение доказательств и накопленная память

Источник: `MM_signals.update_mm_list`, `save_mm_list_to_mongodb`.

```mermaid
flowchart TD
    A[(mm_symbols_active)] --> M[Merge symbol to exchange lists]
    P[(mm_symbols)] --> M
    D[(Mongo mm_data / mm_list)] --> M
    M --> UNIQUE[Append only absent exchange entries]
    UNIQUE --> RAM[In-memory mm_list]
    RAM --> T{300 seconds since last persistence?}
    T -- yes --> DB[Save Mongo snapshot]
    T -- no --> LOOP[Next read cycle in 60 seconds]
    DB --> LOOP
    RAM --> ENRICH[Candidate enrichment and live quotes]
```

Это **накопленный список наблюдавшихся кандидатов**, а не автоматическое доказательство, что каждый участник активен прямо сейчас. Свежие стаканы, текущая разница цен и фильтры проверяются на следующем этапе. Исторический кандидат и исполнимый межбиржевой маршрут — разные сущности.

## 8. Межбиржевой контекст

Источник: `process_symbol`, `process_exchanges`, `detect_mm_signals`.

```mermaid
flowchart TD
    MM[Detected symbol and exchanges] --> DIR[Candidate buy / sell directions]
    DIR --> EX{Historical direction exclusions pass?}
    EX -- no --> X[Skip direction]
    EX -- yes --> NET[Find common network]
    NET --> PERM{Withdraw, deposit and known fee?}
    PERM -- no --> X
    PERM -- yes --> TICK[Read positive ask / bid]
    TICK --> BOOK[Read fresh books for both sides]
    BOOK --> METRIC[Price difference, fee, depth and liquidity indicators]
    METRIC --> FINAL[final_signals keyed by route]
    FINAL --> UI[List and detailed card]
```

Не следует смешивать цену best bid/ask, спред внутри одной биржи и чистую прибыль исполнения по глубине. Это разные показатели разных этапов. MM-сигнал о паттерне не автоматически означает прибыльную арбитражную возможность. В текущем исходном `process_exchanges` выбирается последняя подходящая сеть в переборе, а не доказанно минимальная комиссия среди сетей; схема этого не скрывает.

## 9. Пользовательский список: фильтр до пагинации

Источник: `generate_active_mm_list`.

```mermaid
flowchart TD
    F[final_signals] --> U[Load local saveList and blackList]
    U --> SEARCH[Optional uppercase token substring search]
    SEARCH --> BLACK[Exclude full symbol and base token blacklist entries]
    BLACK --> POS[Keep positive price difference]
    POS --> SORT{Selected ranking mode}
    SORT --> SP[Difference descending]
    SORT --> LIQ[Liquidity indicator ordering]
    SORT --> VOL[Available volume descending]
    SORT --> MIX[Combined rank]
    SP --> PAGE[Slice page after filtering and ordering]
    LIQ --> PAGE
    VOL --> PAGE
    MIX --> PAGE
    PAGE --> CARD[Buttons for details, favorites and refresh]
```

Личная сортировка не перезапускает сбор бирж и стратегии обнаружения. Пользователь меняет представление общего результата. Избранное и чёрный список хранятся в локальной MongoDB и переживают перезапуск контейнеров.

## 10. Мониторинг и обратная связь

```mermaid
sequenceDiagram
    participant U as Local user
    participant T as Telegram handlers
    participant D as Mongo preferences
    participant M as Monitoring loop
    participant S as Current signals
    U->>T: Save route / adjust threshold
    T->>D: Persist favorite and spread limit
    U->>T: Enable monitoring
    T->>D: monitoring = true
    M->>D: Load own preferences
    M->>S: Match favorite against current result
    M->>M: Apply threshold and existing notification state
    M->>T: Format eligible update
    T->>U: Notification with route details
    U->>T: Refresh card / remove favorite
    T->>D: Update only this installation's preferences
```

## 11. Почему архитектура разделена именно так

| Решение | Практический эффект | Чего оно не гарантирует |
|---|---|---|
| Два независимых детектора | Разные виды наблюдаемого поведения | Их сигналы не являются независимыми статистическими доказательствами |
| Top-10 и отказ от дублей размеров | Ограниченная цена сопоставления, меньше неоднозначности | Сохранение всех редких паттернов глубже стакана |
| Несколько окон | Отсечение одиночного движения | Непрерывность наблюдения между непустыми окнами |
| История 18 снимков | Ограниченная память и поиск соседей | Точный порядок событий между двумя snapshot |
| Отдельный UI/monitoring процесс | Интерфейс не вызывает детектор заново | Отсутствие любых блокирующих вызовов в старом коде |
| Нативные биржевые декодеры | Сохранение протокольных особенностей и оптимизаций | Одинаковое качество данных на всех площадках |


## Реальные границы реализации

Исходный сбор стаканов — **собственные адаптеры в `newbot/webSockets/`**, а не CCXT/CCXT Pro. Первая переносимая редакция ошибочно подменила их универсальной библиотекой; текущая редакция эту подмену устраняет. `portable/native_ws.py` запускает оригинальные классы в отдельных процессах. `portable/native_rest.py` — небольшой прямой HTTP-слой для публичных тикеров и выбора пар; это добавленная инфраструктура переносимости, не авторское расчётное ядро.

Серверные процессы проверены отдельно от наличия файлов: присутствие `client_auto.py` или альтернативного `*2.py` в архиве не означает, что файл участвует в работающем боте. Старый отдельный эксперимент `client_auto.py` с CCXT исключён из поставки. В репозитории остаются исходные специализированные адаптеры для изучения, но активный путь перечислен ниже.

### Транспорт по биржам

| Биржа | Реальный модуль | Особенность обработки |
|---|---|---|
| KuCoin | `newbot/webSockets/kucoin.py` | Публичный `bullet-public`; `level2Depth50`; последний raw-снимок и отложенный разбор |
| Bitget | `newbot/webSockets/bitget.py` | `books15`; выделение instId; замена pending-снимка; сортировка asks/bids |
| BingX | `newbot/webSockets/bingx.py` | Сжатые сообщения; heartbeat; последний raw-снимок перед разбором |
| HTX | `newbot/webSockets/htx.py` | Сжатие gzip, ping/pong, отложенная обработка полных depth-снимков |
| MEXC | `newbot/webSockets/mexc.py` | Бинарный Protobuf `PushDataV3ApiWrapper`; limited depth; backoff переподключения |
| Bybit | `newbot/webSockets/bybit.py` | Отдельная обработка snapshot/delta, обновление и удаление ценовых уровней |
| CoinEx | `newbot/webSockets/coinex.py` | Собственный декодер; локальная редакция обновлена на v2/spot, полные снимки; исходный поток на символ |
| Gate | `newbot/webSockets/gate.py` | Собственный WebSocket-клиент, `spot.order_book`, ограниченные полные снимки |
| BitMart | `newbot/webSockets/bitmart.py` | Менеджер подписок, собственный декодер и общий снимок |
| OKX | `newbot/webSockets/okx.py` | В локальной редакции `books5`: полные снимки; исправлен разбор дополнительных колонок |

KuCoin получает **публичный временный токен транспорта**, автоматически: это не персональный API-ключ биржи. Везде используются публичные потоки; кошелёк и торговые полномочия не запрашиваются.

### Почему отложенный JSON-разбор экономит работу

```mermaid
sequenceDiagram
    participant E as Exchange full-snapshot stream
    participant W as Native WS receiver
    participant P as Pending map by symbol
    participant D as Periodic decoder
    participant C as Memcached
    E->>W: snapshot A, symbol X
    W->>P: X = raw A + received_at
    E->>W: snapshot B, symbol X
    W->>P: X = raw B + received_at
    E->>W: snapshot C, symbol Y
    W->>P: Y = raw C + received_at
    D->>P: Swap pending map with empty map
    Note over W,P: New frames go to the new map
    D->>D: Decode latest X and latest Y once
    D->>D: Normalize and sort book levels
    D->>C: gzip + pickle, complete exchange snapshot
```

Это **coalescing полных снимков**: если пришло несколько самодостаточных книг одного символа, промежуточные можно заменить последней до дорогого JSON-разбора. Если за интервал пришло M кадров по S символам, количество разборов ограничено числом обновившихся символов, а не M. Эта логика применима только к полным снимкам. Для delta-потоков пропуск промежуточного обновления повреждает состояние: поэтому нельзя механически переносить оптимизацию KuCoin/Bitget в обработчик Bybit/CoinEx.

### От независимых потоков к единому формату

```mermaid
flowchart TB
    A[Public REST tickers] --> B[Select USDT symbol universe]
    B --> DB[(MongoDB tradePairs)]
    DB --> K[Native KuCoin process]
    DB --> BG[Native Bitget process]
    DB --> MX[Native MEXC process]
    DB --> O[Other native exchange processes]
    K --> N[Normalized asks / bids / lastUpdate]
    BG --> N
    MX --> N
    O --> N
    N --> C[(Memcached exchange full snapshots)]
    C --> R[Independent calculation readers]
    M[Public network endpoints] --> D[(AllSymbolsData currency.networks)]
    D --> R
```

Формат — словарь `native_symbol → {asks, bids, lastUpdate}`. Имена бирж исторически различаются: MM использует `MEXC`, арбитраж — `MEXC Global`; адаптер совместимости зеркалирует ключ. OKX исходно писал per-symbol ключи; локальный runner объединяет их в полный снимок. Основные нативные сборщики публикуют примерно раз в 2 секунды с TTL 10 секунд; конкретный цикл смотрите в соответствующем `update_database`/`update_cache`, а не в универсальном обещании одинаковой частоты.

### Асинхронность и восстановление: что действительно параллельно

```mermaid
flowchart LR
    S[Foreground supervisor] --> P1[Metadata process]
    S --> P2[One WS process per exchange]
    S --> P3[Calculation processes]
    S --> P4[Telegram process]
    P2 --> EV[asyncio sockets and heartbeat tasks]
    P2 --> CO[CoinEx: original per-symbol threads]
    P3 --> IO[Async cache and database cycles]
    P3 --> CPU[CPU calculation where implemented]
    P1 -. unexpected exit .-> F[Supervisor stops child groups]
    P2 -. unexpected exit .-> F
    P3 -. unexpected exit .-> F
    P4 -. unexpected exit .-> F
    F --> D[Docker restart policy]
    D --> S
```

Процессы изолируют тяжёлые циклы друг от друга; `asyncio.gather` внутри процесса перекрывает ожидание сети, но не делает Python-математику автоматически многопроцессной. Синхронные обращения к кэшу в исходных модулях ещё есть. У бирж разные схемы reconnect; экспоненциальный backoff подтверждён, например, в MEXC, но не приписывается всем сборщикам.

TTL снимка и возраст конкретной книги — разные проверки: процесс может заново записать старую книгу и продлить TTL. Поэтому расчёты дополнительно проверяют `lastUpdate` (30 секунд), а пользовательский арбитражный расчёт — ещё и рассинхронизацию двух книг (10 секунд).

### Персональная локальная установка

```mermaid
stateDiagram-v2
    [*] --> Unpaired
    Unpaired --> Unpaired: Wrong pairing code
    Unpaired --> Paired: Valid local setup code, atomic owner record
    Paired --> Paired: Owner commands and callbacks
    Paired --> Rejected: Different user or group chat
    Rejected --> Paired: No state mutation
```

`portable/access.py` хранит одного локально привязанного пользователя. Middleware проверяет сообщения и callback до бизнес-обработчиков. Здесь нет специальной роли автора проекта, его ID, кнопок продажи доступа, добавления/удаления подписчиков, служебного управления общими списками или адреса поддержки владельца. Пользовательские настройки, избранное и личные чёрные списки сохранены.

### Проверяемость вместо декоративных обещаний

Схемы показывают вызовы и структуры данных, подтверждённые исходниками. Они не утверждают измеренную пропускную способность, точность определения маркет-мейкера или гарантированную прибыль. `VALIDATION.md` отделяет unit/import проверки, Docker-интеграцию и реальные сетевые пробы. Доступность API зависит от сети; данные о переводах описаны в [DATA_COVERAGE.md](DATA_COVERAGE.md).
