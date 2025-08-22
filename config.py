#!/usr/bin/env python3
"""
РЕВОЛЮЦИОННАЯ КОНФИГУРАЦИЯ АРБИТРАЖНОГО БОТА

🚀 РЕВОЛЮЦИОННЫЙ ПОДХОД: ТОЛЬКО ПРИБЫЛЬ РЕШАЕТ! 🚀

Принципы:
1. НЕТ искусственных ограничений - только защита от неадекватности
2. Price Impact 80%? Если это дает прибыль - ДЕЛАЕМ!
3. Маленькая ликвидность? Если прибыль есть - ТОРГУЕМ!
4. Главное правило: ПРИБЫЛЬ >= МИНИМАЛЬНОЙ = ИСПОЛНЯЕМ

Все параметры настроены для МАКСИМАЛЬНОГО охвата возможностей.
Фильтры только для защиты от технических проблем, не для ограничения прибыли!

Автор: Flash Loan Arbitrage Team
Дата: 2025
"""

import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# ===== КРИТИЧЕСКИЕ НАСТРОЙКИ (только из .env) =====
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
# ===== ИСТОЧНИК РЕЗЕРВОВ =====
RESERVES_SOURCE = "onchain"  # Используем онchain через ANKR
ANKR_API_KEY = os.getenv("ANKR_API_KEY")  # Получаем из .env файла

# Проверяем что загружены
if not CONTRACT_ADDRESS or not PRIVATE_KEY:
    print("⚠️  ОШИБКА: CONTRACT_ADDRESS и PRIVATE_KEY должны быть в .env файле!")
    print("⚠️  Без них бот не может работать!")

# ===== ОСНОВНЫЕ НАСТРОЙКИ =====
CHAIN = "polygon"  # polygon, ethereum, bsc, arbitrum, optimism

# RPC endpoints для разных сетей
RPC_URLS = {
    "polygon": os.getenv("POLYGON_RPC", "https://polygon-rpc.com"),
    "ethereum": os.getenv("ETH_RPC", "https://eth.llamarpc.com"),
    "bsc": os.getenv("BSC_RPC", "https://bsc-dataseed.binance.org/"),
    "arbitrum": os.getenv("ARB_RPC", "https://arb1.arbitrum.io/rpc"),
    "optimism": os.getenv("OP_RPC", "https://mainnet.optimism.io")
}

# ===== 🚀 УЛЬТРА-РЕВОЛЮЦИОННЫЕ ТОРГОВЫЕ ПАРАМЕТРЫ 🚀 =====

# Минимальная прибыль для исполнения сделки (USD)
# ЦЕЛЬ: Найти ЛЮБУЮ прибыль!
MIN_PROFIT_USD = -1  # Отрицательное = показывать ВСЁ для анализа

# Минимальная ликвидность в паре (USD)
# Снижаем до минимума для тестирования
MIN_LIQUIDITY_USD = 500  # $500 - абсолютный минимум

# Минимальный спред для анализа (%)
# КРИТИЧНО: Снижаем ниже комиссий для поиска ЛЮБЫХ возможностей
MIN_SPREAD_PERCENT = 0.75  # 0.3% - даже если не покрывает комиссии, проверим!

# Максимальный допустимый price impact (%)
# РЕВОЛЮЦИОННЫЙ ПОДХОД: Если прибыль есть - impact не важен!
MAX_PRICE_IMPACT = 90.0  # 90% - почти любой impact допустим!

# ===== КОМИССИИ (фиксированные) =====

# Комиссии DEX (%)
DEX_FEES = {
    "uniswap": 0.3,
    "uniswap_v3": 0.3,
    "sushiswap": 0.3,
    "quickswap": 0.3,
    "pancakeswap": 0.25,
    "default": 0.3
}

# Комиссия за Flash Loan (%)
FLASH_LOAN_FEES = {
    "aave": 0.05,      # Aave v3 - 0.05%
    "balancer": 0.0,   # Balancer - 0% (РЕВОЛЮЦИОННО!)
    "default": 0.05
}

# ===== БЕЛЫЙ СПИСОК DEX =====
# Только эти DEX будут использоваться для арбитража
ENABLED_DEXES = [
    'quickswap',
    'sushiswap', 
    'apeswap',
    'jetswap',
    'dfyn',
    # 'uniswap', # ЗАКОММЕНТИРОВАТЬ! На Polygon это фейк
    'polycat',
    'waultswap',
    'gravityfinance',
    'dystopia',
    # 'balancer'     # пока не добавляйте - требует особой логики
]

# ===== КОНФИГУРАЦИЯ DEX =====
# Все адреса и типы известны заранее
DEX_CONFIG = {
    "quickswap": {
        "router": "0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff",
        "factory": "0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32",
        "init_code_pair_hash": "0x96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f",
        "type": 'V2',
        "fee": 0.3
    },
    "sushiswap": {
        "router": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
        "factory": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
        "init_code_pair_hash": "0xe18a34eb0e04b04f7a0ac29a6e80748dca96319b42c54d679cb821dca90c6303",
        "type": 'V2',
        "fee": 0.3
    },
    'apeswap': {
        'router': '0xC0788A3aD43d79aa53B09c2EaCc313A787d1d607',
        'factory': '0x0841BD0B734E4F5853f0dD8d7Ea041c241fb0Da6',
        "init_code_pair_hash": "0xf4b8a02d292b184779b6e4125cd0695b98935efa81e7dd7d0a9c839de1e5b3bd",
        "type": 'V2',
        "fee": 0.3
    },
    'jetswap': {
        'router': '0x5C6EC38fb0e2609672BDf628B1fD605A523E5923',
        'factory': '0x668ad0ed2622C62E24f0d5ab6B6Ac1b9D2cD4AC7',
        'init_code_pair_hash': '0x505c843b83f01afef714149e8b174427d552e1aca4834b4f9b4b525f426ff3c6',
        'type': 'V2',
        'fee': 0.3
    },
    "dfyn": {
        "router": "0xA102072A4C07F06EC3B4900FDC4C7B80b6c57429",
        "factory": "0xE7Fb3e833eFE5F9c441105EB65Ef8b261266423B",
        "init_code_pair_hash": "0xf187ed688403aa4f7acfada758d8d53698753b998a3071b06f1b777f4330eaf3",
        "type": 'V2',
        "fee": 0.3
    },
    'uniswap': {  # На Polygon это фактически QuickSwap
        'router': '0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff',
        'factory': '0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32',
        'init_code_pair_hash': '0x96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f',
        'type': 'V2',
        'fee': 0.3
    },
    'polycat': {
        'router': '0x94930a328162957FF1dd48900aF67B5439336cBD',
        'factory': '0x477Ce834Ae6b7aB003cCe4BC4d8697763FF456FA',
        'init_code_pair_hash': '0x3cad6f9e70e13835b4f07e5dd475f25a109450b22811d0437da51e66c161255a',
        'type': 'V2',
        'fee': 0.3
    },
    'waultswap': {
        'router': '0x3a1D87f206D12415f5b0A33E80c3f8B7b0b6d4e8',
        'factory': '0xa98ea6356A316b44Bf710D5f9b6b4eA0081409Ef',
        'init_code_pair_hash': '0x1cdc2246d318ab84d8bc7ae2a3d81c235f3db4e113f4c6fdc1e2211a9291be47',
        'type': 'V2',
        'fee': 0.3
    },
    'gravityfinance': {
        'router': '0x57dE98135e8287F163c59cA4fF45f1341b680248',
        'factory': '0x3EdAB7c8E32DEEa3Bd0172994D9aBD8524147d97',
        'init_code_pair_hash': '0x83c95f826db1583ef6603bb6e619ebaa4d3602086f6b929dd37f37a1ad730db5',
        'type': 'V2',
        'fee': 0.3
    },
    'dystopia': {
        'router': '0xbE75Dd16D029c6B32B7aD57A0FD9C1c20Dd2862e',
        'factory': '0x1d21Db6cde1b18c7E47B0F7F42f4b3F68b9c2176',
        'init_code_pair_hash': '0x009bce6e57e441e02b7e9cb40e4e40c1410a57ac793c7c256c0c1e62a43e6ca2',
        'type': 'V2',
        'fee': 0.3
    },
    'balancer': {
        'router': '0xBA12222222228d8Ba445958a75a0704d566BF2C8',  # Vault address
        'factory': '0xBA12222222228d8Ba445958a75a0704d566BF2C8',
        'type': 'BALANCER',
        'fee': 0.3
        # Balancer не использует CREATE2 с init_code_pair_hash
    }
}

# Примерная стоимость газа (USD)
GAS_COSTS = {
    "polygon": 0.05,    # Polygon - дешевый газ
    "bsc": 0.2,
    "arbitrum": 0.5,
    "optimism": 0.5,
    "ethereum": 10.0   # Ethereum - дорогой газ
}

# ===== ТЕХНИЧЕСКИЕ ПАРАМЕТРЫ =====

# Интервал между сканированиями (секунды)
SCAN_INTERVAL = 15  # Каждые 30 секунд - оптимально

# Максимум параллельных запросов к API
MAX_CONCURRENT_REQUESTS = 10

# Таймаут для API запросов (секунды)
API_TIMEOUT = 10

# ===== QUOTE ТОКЕНЫ ДЛЯ РАЗНЫХ СЕТЕЙ =====
# РАСШИРЯЕМ список для большего охвата!

QUOTE_TOKENS = {
    "polygon": [
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # USDC.e
        "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",  # USDC native
        "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",  # USDT
        "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",  # WMATIC
        "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",  # DAI
        "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",  # WETH
        "0x1bfd67037b42cf73acF2047067bd4F2C47D9BfD6",  # WBTC
        "0xD6DF932A45C0f255f85145f286eA0b292B21C90B",  # AAVE
    ],
    "ethereum": [
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
        "0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
    ],
    "bsc": [
        "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",  # USDC
        "0x55d398326f99059fF775485246999027B3197955",  # USDT
        "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",  # BUSD
        "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
    ]
}

# Token decimals для популярных токенов
TOKEN_DECIMALS = {
    # Stablecoins на Polygon
    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174": 6,  # USDC.e
    "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359": 6,  # USDC native
    "0xc2132D05D31c914a87C6611C10748AEb04B58e8F": 6,  # USDT
    "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063": 18, # DAI
    # Основные токены
    "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270": 18, # WMATIC
    "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619": 18, # WETH
    "0x1bfd67037b42cf73acF2047067bd4F2C47D9BfD6": 8,  # WBTC
}

# ===== РЕЖИМЫ РАБОТЫ =====

# Режим симуляции (не исполнять реальные сделки)
# ВАЖНО: Сейчас FALSE - боевая торговля!
SIMULATION_MODE = False

# Режим отладки (больше логов)
# ВАЖНО: Включен для диагностики
DEBUG_MODE = True

# ===== ДОПОЛНИТЕЛЬНЫЕ НАСТРОЙКИ =====

# Исключить токены (пока не используется в революционном подходе)
EXCLUDE_TOKENS = set()  # Пустой - не исключаем ничего!

# Минимальный объем торгов за 24ч (USD)
MIN_VOLUME_24H = 0  # Не ограничиваем - если есть прибыль, торгуем!

# Максимальное количество DEX для пары
MAX_DEX_COUNT = 100  # Не ограничиваем

# ===== ЦЕНЫ БАЗОВЫХ ТОКЕНОВ (для газа и отображения) =====
BASE_TOKEN_USD_PRICES = {
    '0x2791bca1f2de4661ed88a30c99a7a9449aa84174': 1.0,    # USDC.e
    '0x3c499c542cef5e3811e1192ce70d8cc03d5c3359': 1.0,    # USDC
    '0xc2132d05d31c914a87c6611c10748aeb04b58e8f': 1.0,    # USDT
    '0x8f3cf7ad23cd3cadbd9735aff958023239c6a063': 1.0,    # DAI
    '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270': 0.5,    # WMATIC (примерно)
    '0x7ceb23fd6bc0add59e62ac25578270cff1b9f619': 2300,   # WETH (примерно)
    '0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6': 58000,  # WBTC (примерно)
    '0xd6df932a45c0f255f85145f286ea0b292b21c90b': 85,     # AAVE (примерно)
}

# ===== HELPER ФУНКЦИИ =====

def get_dex_fee(dex_name: str) -> float:
    """Получить комиссию DEX"""
    return DEX_FEES.get(dex_name.lower(), DEX_FEES["default"])

def get_flash_loan_fee(provider: str = "aave") -> float:
    """Получить комиссию Flash Loan"""
    return FLASH_LOAN_FEES.get(provider.lower(), FLASH_LOAN_FEES["default"])

def get_gas_cost(chain: str) -> float:
    """Получить стоимость газа для сети"""
    return GAS_COSTS.get(chain.lower(), 1.0)

def get_total_fees(chain: str, dex1: str, dex2: str) -> float:
    """Рассчитать общие комиссии в процентах"""
    dex1_fee = get_dex_fee(dex1)
    dex2_fee = get_dex_fee(dex2)
    flash_fee = get_flash_loan_fee()
    
    return dex1_fee + dex2_fee + flash_fee

# Вывод конфигурации при импорте
if __name__ == "__main__" or DEBUG_MODE:
    print("\n" + "="*60)
    print("🚀 УЛЬТРА-РЕВОЛЮЦИОННАЯ КОНФИГУРАЦИЯ АРБИТРАЖА 🚀")
    print("="*60)
    print("ПРИНЦИП: НАЙТИ ЛЮБУЮ ПРИБЫЛЬ!")
    print("="*60)
    print(f"Сеть: {CHAIN}")
    print(f"Режим: {'СИМУЛЯЦИЯ' if SIMULATION_MODE else '⚡ БОЕВАЯ ТОРГОВЛЯ ⚡'}")
    print("\nПАРАМЕТРЫ:")
    print(f"  💰 Мин. прибыль: ${MIN_PROFIT_USD} (даже $0.10 - это успех!)")
    print(f"  💧 Мин. ликвидность: ${MIN_LIQUIDITY_USD:,}")
    print(f"  📊 Мин. спред: {MIN_SPREAD_PERCENT}% (проверяем ВСЁ!)")
    print(f"  🚀 Макс. price impact: {MAX_PRICE_IMPACT}% (почти без ограничений!)")
    print(f"  ⏱️  Интервал сканирования: {SCAN_INTERVAL} сек")
    print(f"  🔍 Quote токенов для поиска: {len(QUOTE_TOKENS.get(CHAIN, []))}")
    print("\nКОМИССИИ:")
    print(f"  DEX: {get_dex_fee('default')}% x2 = {get_dex_fee('default')*2}%")
    print(f"  Flash loan: {get_flash_loan_fee()}%")
    print(f"  Gas: ${get_gas_cost(CHAIN)}")
    print(f"  ИТОГО: {get_total_fees(CHAIN, 'default', 'default')}% + ${get_gas_cost(CHAIN)} gas")
    print("="*60)
    print("СТАТУС: Готов найти ЛЮБУЮ возможность!")
    print("="*60 + "\n")