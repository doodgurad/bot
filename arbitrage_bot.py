#!/usr/bin/env python3

import sys
import traceback
import asyncio
import aiohttp
import json
import os
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware  # ВАЖНО: POA middleware для Polygon
from eth_account import Account
from eth_abi import encode
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import logging
import math
from decimal import Decimal, ROUND_DOWN

# Импорт LUT модуля
from lut_runtime_v2 import load_lut, size_from_lut

try:
    # Импорты из конфига
    from config import (
        CHAIN, RPC_URLS, CONTRACT_ADDRESS, PRIVATE_KEY,
        MIN_PROFIT_USD, MIN_LIQUIDITY_USD, MIN_SPREAD_PERCENT, 
        MAX_PRICE_IMPACT,
        SCAN_INTERVAL, SIMULATION_MODE,
        get_dex_fee, get_flash_loan_fee, get_gas_cost,
        QUOTE_TOKENS,  # Используем вместо BASE_TOKENS
        TOKEN_DECIMALS,
        MIN_VOLUME_24H,  # ДОБАВИТЬ ЭТУ СТРОКУ
        ENABLED_DEXES,  # НОВОЕ
        DEX_CONFIG,  # НОВОЕ
        BASE_TOKEN_USD_PRICES  # НОВОЕ - цены токенов
    )
except ImportError as e:
    print(f"\n❌ ОШИБКА ИМПОРТА: {e}")
    print("\nПроверьте, что все необходимые модули установлены и config.py содержит все нужные переменные.")
    traceback.print_exc()
    sys.exit(1)

# ===== КОНФИГУРАЦИЯ =====

# API endpoints
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex"

# Получаем RPC URL для выбранной сети
RPC_URL = RPC_URLS.get(CHAIN)

# ===== RPC РОТАТОР ДЛЯ ОБХОДА ЛИМИТОВ =====
class RPCRotator:
    """Ротация между несколькими RPC для обхода rate limits"""
    def __init__(self, chain="polygon"):
        if chain == "polygon":
            self.rpcs = [
                "https://polygon-rpc.com",
                "https://rpc-mainnet.matic.network", 
                "https://matic-mainnet.chainstacklabs.com",
                # "https://rpc-mainnet.maticvigil.com",  # ОТКЛЮЧЕН - возвращает 403
                "https://polygon-bor.publicnode.com",
                "https://polygon.llamarpc.com",
                "https://polygon.rpc.blxrbdn.com",
                "https://polygon-mainnet.public.blastapi.io"
            ]
        else:
            self.rpcs = [RPC_URLS.get(chain)]
            
        self.current = 0
        self.request_count = 0
        self.switch_after = 20
        
    def get_current(self):
        return self.rpcs[self.current]
    
    def get_next(self):
        self.request_count += 1
        if self.request_count >= self.switch_after:
            self.current = (self.current + 1) % len(self.rpcs)
            self.request_count = 0
            print(f"🔄 Переключение на RPC #{self.current + 1}: {self.rpcs[self.current][:30]}...")
        return self.rpcs[self.current]
    
    def force_switch(self):
        self.current = (self.current + 1) % len(self.rpcs)
        self.request_count = 0
        print(f"⚠️ Принудительное переключение на RPC #{self.current + 1}: {self.rpcs[self.current][:30]}...")
        return self.rpcs[self.current]

# Создаем глобальный экземпляр ротатора
rpc_rotator = RPCRotator(CHAIN)
RPC_URL = rpc_rotator.get_current()


# v6.3: Используем токены из config.py с добавлением имен
BASE_TOKENS = {}
for chain, tokens in {"polygon": QUOTE_TOKENS.get("polygon", [])}.items():
    BASE_TOKENS[chain] = {
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174": "USDC.e",
        "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359": "USDC",
        "0xc2132D05D31c914a87C6611C10748AEb04B58e8F": "USDT",
        "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063": "DAI",
        "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270": "WMATIC",
        "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619": "WETH",
        "0x1bfd67037b42cf73acF2047067bd4F2C47D9BfD6": "WBTC",
        "0xD6DF932A45C0f255f85145f286eA0b292B21C90B": "AAVE",
    }

# ABI для универсального контракта
UNIVERSAL_CONTRACT_ABI = json.loads('''[
    {
        "inputs": [
            {"name": "asset", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "params", "type": "bytes"}
        ],
        "name": "executeArbitrage",
        "outputs": [],
        "type": "function"
    },
    {
        "inputs": [],
        "name": "owner",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function"
    },
    {
        "inputs": [{"name": "token", "type": "address"}],
        "name": "emergencyWithdraw",
        "outputs": [],
        "type": "function"
    }
]''')

# Минимальный ABI для пары (для получения factory)
PAIR_ABI = json.loads('''[
    {
        "constant": true,
        "inputs": [],
        "name": "factory",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "token0",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "token1",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function"
    }
]''')


@dataclass
class ArbitrageOpportunity:
    """Структура арбитражной возможности"""
    token_symbol: str
    token_address: str
    quote_token_address: str  # Адрес quote токена (базовый токен)
    quote_token_symbol: str  # v6.3: Символ базового токена
    buy_dex: str
    sell_dex: str
    buy_price: float
    sell_price: float
    spread_percent: float
    optimal_trade_size: float
    expected_profit: float
    buy_liquidity: float
    sell_liquidity: float
    price_impact_percent: float
    buy_dex_pair: str = ""
    sell_dex_pair: str = ""
    buy_router: str = ""  # Адрес роутера для покупки
    sell_router: str = ""  # Адрес роутера для продажи


class CalldataBuilder:
    """Строитель calldata для различных типов DEX"""
    
    @staticmethod
    def build_v2_swap(
        router: str,
        amount_in: int,
        amount_out_min: int,
        path: List[str],
        to: str,
        deadline: int
    ) -> bytes:
        """Строит calldata для V2 swap (swapExactTokensForTokens)"""
        # Function selector для swapExactTokensForTokens
        # function swapExactTokensForTokens(uint,uint,address[],address,uint)
        function_selector = Web3.keccak(
            text="swapExactTokensForTokens(uint256,uint256,address[],address,uint256)"
        )[:4]
        
        # Кодируем параметры
        encoded_params = encode(
            ['uint256', 'uint256', 'address[]', 'address', 'uint256'],
            [amount_in, amount_out_min, path, to, deadline]
        )
        
        return function_selector + encoded_params
    
    @staticmethod
    def build_v3_swap(
        router: str,
        token_in: str,
        token_out: str,
        fee: int,
        recipient: str,
        deadline: int,
        amount_in: int,
        amount_out_min: int,
        sqrt_price_limit: int = 0
    ) -> bytes:
        """Строит calldata для V3 swap (exactInputSingle)"""
        # Function selector для exactInputSingle
        function_selector = Web3.keccak(
            text="exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))"
        )[:4]
        
        # Структура ExactInputSingleParams
        params = (
            token_in,
            token_out,
            fee,  # 3000 = 0.3%, 500 = 0.05%, 10000 = 1%
            recipient,
            deadline,
            amount_in,
            amount_out_min,
            sqrt_price_limit
        )
        
        # Кодируем параметры
        encoded_params = encode(
            ['(address,address,uint24,address,uint256,uint256,uint256,uint160)'],
            [params]
        )
        
        return function_selector + encoded_params
    
    @staticmethod
    def build_algebra_swap(
        router: str,
        token_in: str,
        token_out: str,
        recipient: str,
        deadline: int,
        amount_in: int,
        amount_out_min: int,
        sqrt_price_limit: int = 0
    ) -> bytes:
        """Строит calldata для Algebra swap (похож на V3 но без fee)"""
        # Algebra использует похожий интерфейс но без явного fee
        # Function selector для exactInputSingle в Algebra
        function_selector = Web3.keccak(
            text="exactInputSingle((address,address,address,uint256,uint256,uint256,uint160))"
        )[:4]
        
        params = (
            token_in,
            token_out,
            recipient,
            deadline,
            amount_in,
            amount_out_min,
            sqrt_price_limit
        )
        
        encoded_params = encode(
            ['(address,address,address,uint256,uint256,uint256,uint160)'],
            [params]
        )
        
        return function_selector + encoded_params

@dataclass
class SwapRoute:
    """Описание одного свопа в маршруте"""
    token_in: str
    token_out: str
    dex: str
    router: str
    amount_in: int = 0  # Будет рассчитан позже


class FlashLoanStrategy:
    """v7.0: Универсальная стратегия выбора токена для Flash Loan"""
    
    # Токены с хорошей ликвидностью в Aave на Polygon
    AAVE_TOKENS = {
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174": "USDC.e",
        "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359": "USDC",
        "0xc2132D05D31c914a87C6611C10748AEb04B58e8F": "USDT",
        "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063": "DAI",
        "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619": "WETH",
        "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270": "WMATIC",
        "0x1bfd67037b42cf73acF2047067bd4F2C47D9BfD6": "WBTC",
    }
    
    @staticmethod
    async def get_aave_liquidity(token_address: str, w3: Web3) -> float:
        """Получает доступную ликвидность для токена в Aave"""
        try:
            aave_data_provider = "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"
            
            abi = json.loads('''[{
                "inputs": [{"name": "asset", "type": "address"}],
                "name": "getReserveData",
                "outputs": [
                    {"name": "unbacked", "type": "uint256"},
                    {"name": "accruedToTreasuryScaled", "type": "uint256"},
                    {"name": "totalAToken", "type": "uint256"},
                    {"name": "totalStableDebt", "type": "uint256"},
                    {"name": "totalVariableDebt", "type": "uint256"},
                    {"name": "liquidityRate", "type": "uint256"},
                    {"name": "variableBorrowRate", "type": "uint256"},
                    {"name": "stableBorrowRate", "type": "uint256"},
                    {"name": "averageStableBorrowRate", "type": "uint256"},
                    {"name": "liquidityIndex", "type": "uint256"},
                    {"name": "variableBorrowIndex", "type": "uint256"},
                    {"name": "lastUpdateTimestamp", "type": "uint40"}
                ],
                "type": "function"
            }]''')
            
            contract = w3.eth.contract(address=Web3.to_checksum_address(aave_data_provider), abi=abi)
            data = contract.functions.getReserveData(token_address).call()
            
            total_liquidity = data[2]
            total_debt = data[3] + data[4]
            available = total_liquidity - total_debt
            
            # Возвращаем в USD (упрощенно, можно улучшить)
            decimals = 18 if token_address == "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270" else 6
            return available / (10 ** decimals)
            
        except:
            return 0
    
    @classmethod
    async def choose_optimal_flash_loan_token(
        cls,
        opportunity: ArbitrageOpportunity,
        w3: Web3,
        logger
    ) -> Tuple[str, str, List[SwapRoute]]:
        """
        Выбирает оптимальный токен для Flash Loan
        Returns: (flash_loan_token, token_symbol, swap_routes)
        """
        
        options = []
        
        # Вариант 1: Flash Loan в базовом токене (как сейчас)
        if opportunity.quote_token_address in cls.AAVE_TOKENS:
            route = [
                SwapRoute(
                    token_in=opportunity.quote_token_address,
                    token_out=opportunity.token_address,
                    dex=opportunity.buy_dex,
                    router=opportunity.buy_router
                ),
                SwapRoute(
                    token_in=opportunity.token_address,
                    token_out=opportunity.quote_token_address,
                    dex=opportunity.sell_dex,
                    router=opportunity.sell_router
                )
            ]
            liquidity = await cls.get_aave_liquidity(opportunity.quote_token_address, w3)
            options.append({
                'token': opportunity.quote_token_address,
                'symbol': opportunity.quote_token_symbol,
                'route': route,
                'liquidity': liquidity,
                'strategy': 'base_token'
            })
        
        # Вариант 2: Flash Loan в торгуемом токене (инверсия)
        if opportunity.token_address in cls.AAVE_TOKENS:
            route = [
                SwapRoute(
                    token_in=opportunity.token_address,
                    token_out=opportunity.quote_token_address,
                    dex=opportunity.sell_dex,  # Обратный порядок!
                    router=opportunity.sell_router
                ),
                SwapRoute(
                    token_in=opportunity.quote_token_address,
                    token_out=opportunity.token_address,
                    dex=opportunity.buy_dex,
                    router=opportunity.buy_router
                )
            ]
            liquidity = await cls.get_aave_liquidity(opportunity.token_address, w3)
            options.append({
                'token': opportunity.token_address,
                'symbol': opportunity.token_symbol,
                'route': route,
                'liquidity': liquidity,
                'strategy': 'trade_token'
            })
        
        # Вариант 3: Всегда можем использовать USDC.e (самый ликвидный)
        usdc_address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        if opportunity.token_address != usdc_address and opportunity.quote_token_address != usdc_address:
            # Нужны дополнительные свопы USDC->base и base->USDC
            # Пока пропускаем для простоты
            pass
        
        # Выбираем лучший вариант - предпочитаем базовый токен (USDC)
        # так как в нем больше ликвидности и стабильный курс
        best = None
        for opt in options:
            if opt['strategy'] == 'base_token':
                best = opt
                break
                
        if not best and options:
            best = options[0]
        
        logger.info(f"🎯 Выбрана стратегия Flash Loan: {best['strategy']}")
        logger.info(f"   Токен: {best['symbol']} ({best['token'][:10]}...)")
        logger.info(f"   Ликвидность: ${best['liquidity']:,.0f}")
        
        return best['token'], best['symbol'], best['route']


class ArbitrageBot:
    """Основной класс арбитражного бота для универсального контракта"""
    
    def __init__(self):
        """Инициализация бота"""
        # Сначала настраиваем логирование
        self.setup_logging()
        
        # Используем глобальный ротатор RPC
        global rpc_rotator
        self.rpc_rotator = rpc_rotator
        
        # Web3 подключение с POA middleware для Polygon
        self.w3 = Web3(Web3.HTTPProvider(self.rpc_rotator.get_current()))
        self.logger.info(f"🌐 Подключен к RPC: {self.rpc_rotator.get_current()[:30]}...")
        
        # ВАЖНО: Добавляем POA middleware для Polygon
        if CHAIN in ['polygon', 'bsc']:  # Эти сети используют POA
            self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            self.logger.info("✅ POA middleware добавлен для " + CHAIN)
        
        self.account = Account.from_key(PRIVATE_KEY)
        
        # Контракт (универсальный)
        self.contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDRESS),
            abi=UNIVERSAL_CONTRACT_ABI
        )
        
        # HTTP сессия для API запросов
        self.session = None
        
        # Включаем DEBUG если нужно
        try:
            from config import DEBUG_MODE
            if DEBUG_MODE:
                self.logger.setLevel(logging.DEBUG)
                self.logger.info("📝 DEBUG режим включен")
        except ImportError:
            # Если нет в config, проверяем переменную окружения
            if os.getenv("DEBUG_MODE", "False").lower() == "true":
                self.logger.setLevel(logging.DEBUG)
                self.logger.info("📝 DEBUG режим включен (из env)")
        
        # Статистика
        self.total_scans = 0
        self.opportunities_found = 0
        self.trades_executed = 0
        self.total_profit = 0.0
        
        # Используем только известные DEX из конфига
        self.enabled_dexes = ENABLED_DEXES
        self.dex_config = DEX_CONFIG
        self.logger.info(f"✅ Включено {len(self.enabled_dexes)} DEX: {', '.join(self.enabled_dexes)}")
        
        # v6.3: Получаем базовые токены для текущей сети
        self.base_tokens = BASE_TOKENS.get(CHAIN, {})
        self.logger.info(f"📊 Базовые токены для арбитража ({len(self.base_tokens)}): {', '.join(self.base_tokens.values())}")
        
        # Загружаем LUT таблицу
        try:
            self.lut = load_lut("lut_v2.json")
            self.logger.info("✅ LUT таблица загружена")
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки LUT: {e}")
            self.lut = None
        
        # Кэш для резолва пар: (dex, token0, token1) -> pair_address
        self.pair_address_cache = {}
        self.logger.info("📦 Инициализирован кэш адресов пар")
        
        # НОВОЕ v6.5: Известные рабочие пары DEX с разными роутерами
        self.verified_dex_pairs = {
            'polygon': [
                ('quickswap', 'sushiswap'),  # Точно разные роутеры
                ('quickswap', 'apeswap'),    # Точно разные роутеры
                ('sushiswap', 'apeswap'),    # Точно разные роутеры
                ('quickswap', 'dfyn'),        # Точно разные роутеры
                ('quickswap', 'jetswap'),     # Точно разные роутеры
                # НЕ добавляем ('quickswap', 'uniswap') - одинаковые роутеры!
            ]
        }.get(CHAIN, [])
        
        # Используем цены из конфига
        self.BASE_TOKEN_USD_PRICES = BASE_TOKEN_USD_PRICES
        
    async def choose_flash_loan_asset(self, opportunity: ArbitrageOpportunity) -> Tuple[str, str, List[SwapRoute]]:
        """v7.0: Умный выбор токена для Flash Loan с оптимальным маршрутом"""
        return await FlashLoanStrategy.choose_optimal_flash_loan_token(
            opportunity,
            self.w3,
            self.logger
        )
    
    def safe_web3_call(self, func, *args, max_retries=3, **kwargs):
        """Безопасный вызов Web3 с авто-переключением RPC при ошибке"""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)  # всегда передаём аргументы
            except Exception as e:
                error_str = str(e).lower()
                if any(err in error_str for err in ['rate limit', 'too many requests', '429']):
                    self.logger.warning("⚠️ Rate limit, переключаю RPC...")
                    new_rpc = self.rpc_rotator.force_switch()
                    self.w3 = Web3(Web3.HTTPProvider(new_rpc))
                    if CHAIN in ['polygon', 'bsc']:
                        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                    if attempt < max_retries - 1:
                        import time; time.sleep(0.5)
                        continue
                if attempt == max_retries - 1:
                    raise
            except Exception as e:
                error_str = str(e).lower()
                if any(err in error_str for err in ['rate limit', 'too many requests', '429']):
                    self.logger.warning(f"⚠️ Rate limit, переключаю RPC...")
                    new_rpc = self.rpc_rotator.force_switch()
                    self.w3 = Web3(Web3.HTTPProvider(new_rpc))
                    if CHAIN in ['polygon', 'bsc']:
                        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(0.5)
                        continue
                if attempt == max_retries - 1:
                    raise e
        return None
    
    def setup_logging(self):
        """Настройка логирования"""
        import sys
        import codecs
        
        if sys.platform == 'win32':
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
        
        log_format = '%(asctime)s | %(levelname)s | %(message)s'
            
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('arbitrage_bot.log', encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger('ArbitrageBot')
    
        
    def get_token_decimals(self, token_address: str) -> int:
        """Получает количество decimals для токена"""
        checksum_address = Web3.to_checksum_address(token_address)
        
        # Проверяем в кэше
        if checksum_address in TOKEN_DECIMALS:
            return TOKEN_DECIMALS[checksum_address]
        
        # Если не нашли, пробуем получить из блокчейна
        try:
            # Минимальный ERC20 ABI для decimals
            erc20_abi = json.loads('[{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"}]')
            token_contract = self.w3.eth.contract(address=checksum_address, abi=erc20_abi)
            decimals = self.safe_web3_call(token_contract.functions.decimals().call)
            
            # Кэшируем результат
            TOKEN_DECIMALS[checksum_address] = decimals
            self.logger.debug(f"Получены decimals для {checksum_address}: {decimals}")
            return decimals
        except Exception as e:
            self.logger.warning(f"Не удалось получить decimals для {checksum_address}, используем 18: {e}")
            # ВАЖНО: Кэшируем даже неудачные попытки чтобы не повторять их
            TOKEN_DECIMALS[checksum_address] = 18
            return 18  # По умолчанию
    
    def is_base_token(self, token_address: str) -> bool:
        """v6.3: Проверяет, является ли токен базовым (stablecoin/WMATIC/WETH)"""
        if not token_address:
            return False
        try:
            return token_address.lower() in [addr.lower() for addr in self.base_tokens.keys()]
        except AttributeError:
            self.logger.debug(f"is_base_token: некорректный адрес {token_address}")
            return False
    
    def get_base_token_symbol(self, token_address: str) -> str:
        """v6.3: Получает символ базового токена"""
        for addr, symbol in self.base_tokens.items():
            if addr.lower() == token_address.lower():
                return symbol
        return "UNKNOWN"
    
    
    async def is_v2_pair(self, pair_address: str) -> bool:
        """Проверяет, является ли адрес V2-парой"""
        try:
            # 1. Проверяем что есть байткод
            bytecode = self.w3.eth.get_code(Web3.to_checksum_address(pair_address))
            if len(bytecode) <= 2:
                return False
            
            # 2. Проверяем что есть функции token0() и token1()
            pair_abi = json.loads('[{"constant":true,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"}]')
            pair_contract = self.w3.eth.contract(address=Web3.to_checksum_address(pair_address), abi=pair_abi)
            
            # Пробуем вызвать token0() и token1()
            token0 = pair_contract.functions.token0().call()
            token1 = pair_contract.functions.token1().call()
            
            # Если оба вызова прошли успешно - это V2-пара
            return token0 != '0x0000000000000000000000000000000000000000' and token1 != '0x0000000000000000000000000000000000000000'
        except:
            return False

    def compute_pair_address_create2(self, factory: str, token0: str, token1: str, init_code_hash: str) -> str:
        """Вычисляет адрес пары через CREATE2"""
        from eth_abi import encode
        from eth_utils import keccak
        
        # Сортируем токены
        if token0.lower() > token1.lower():
            token0, token1 = token1, token0
            
        # Убираем 0x из адресов
        factory_clean = factory[2:] if factory.startswith('0x') else factory
        token0_clean = token0[2:] if token0.startswith('0x') else token0
        token1_clean = token1[2:] if token1.startswith('0x') else token1
        init_code_clean = init_code_hash[2:] if init_code_hash.startswith('0x') else init_code_hash
        
        # Кодируем токены
        encoded = encode(['address', 'address'], [Web3.to_checksum_address(token0), Web3.to_checksum_address(token1)])
        
        # Вычисляем salt = keccak256(encoded)
        salt = keccak(encoded).hex()
        
        # CREATE2: 0xff ++ factory ++ salt ++ init_code_hash
        data = 'ff' + factory_clean + salt + init_code_clean
        
        # Вычисляем адрес
        raw_address = keccak(bytes.fromhex(data))[12:].hex()
        
        return Web3.to_checksum_address('0x' + raw_address)
    
    def get_cached_pair_address(self, dex: str, token0: str, token1: str) -> Optional[str]:
        """Получает адрес пары из кэша"""
        # Сортируем токены для консистентности
        sorted_tokens = tuple(sorted([token0.lower(), token1.lower()]))
        cache_key = (dex.lower(), sorted_tokens[0], sorted_tokens[1])
        return self.pair_address_cache.get(cache_key)

    def cache_pair_address(self, dex: str, token0: str, token1: str, pair_address: str):
        """Сохраняет адрес пары в кэш"""
        sorted_tokens = tuple(sorted([token0.lower(), token1.lower()]))
        cache_key = (dex.lower(), sorted_tokens[0], sorted_tokens[1])
        self.pair_address_cache[cache_key] = pair_address.lower()
    
    async def get_v2_reserves_by_router(self, router_addr: str, tokenA: str, tokenB: str, dex_name: str = None):
        """
        Находит пару через router->factory.getPair(tokenA, tokenB) и возвращает резервы/адреса токенов.
        Подходит для V2-совместимых DEX.
        Возврат: dict {pair, token0, token1, reserve0, reserve1} или None.
        """
        from web3 import Web3
        
        # Проверяем кэш, если передано имя DEX
        if dex_name:
            cached_pair = self.get_cached_pair_address(dex_name, tokenA, tokenB)
            if cached_pair:
                self.logger.debug(f"   📦 Использую кэшированный адрес пары для {dex_name}: {cached_pair}")
                # Получаем резервы напрямую из кэшированной пары
                pair_reserves = await self.get_pair_reserves(cached_pair)
                if pair_reserves:
                    return {
                        'pair': cached_pair,
                        'token0': pair_reserves.get('token0'),
                        'token1': pair_reserves.get('token1'),
                        'reserve0': pair_reserves.get('reserve0'),
                        'reserve1': pair_reserves.get('reserve1')
                    }
                # Если не удалось получить резервы из кэша, продолжаем обычным путем
                self.logger.debug(f"   ⚠️ Не удалось получить резервы из кэшированной пары, пробую через factory")

        ROUTER_ABI = [
            {"inputs": [], "name": "factory", "outputs": [{"internalType": "address", "name": "", "type": "address"}],
             "stateMutability": "view", "type": "function"}
        ]
        FACTORY_ABI = [
            {"inputs": [{"internalType": "address", "name": "tokenA", "type": "address"},
                        {"internalType": "address", "name": "tokenB", "type": "address"}],
             "name": "getPair", "outputs": [{"internalType": "address", "name": "pair", "type": "address"}],
             "stateMutability": "view", "type": "function"}
        ]
        PAIR_ABI = [
            {"inputs": [], "name": "token0", "outputs": [{"internalType": "address", "name": "", "type": "address"}],
             "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "token1", "outputs": [{"internalType": "address", "name": "", "type": "address"}],
             "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "getReserves", "outputs": [
                {"internalType": "uint112", "name": "reserve0", "type": "uint112"},
                {"internalType": "uint112", "name": "reserve1", "type": "uint112"},
                {"internalType": "uint32", "name": "blockTimestampLast", "type": "uint32"}],
             "stateMutability": "view", "type": "function"}
        ]
        try:
            router = self.w3.eth.contract(address=Web3.to_checksum_address(router_addr), abi=ROUTER_ABI)
            factory_addr = router.functions.factory().call()
            if int(factory_addr, 16) == 0:
                self.logger.error(f"factory() вернул 0 для роутера {router_addr}")
                return None

            factory = self.w3.eth.contract(address=factory_addr, abi=FACTORY_ABI)
            a = Web3.to_checksum_address(tokenA)
            b = Web3.to_checksum_address(tokenB)
            
            self.logger.debug(f"   Factory.getPair({a}, {b})")
            pair_addr = factory.functions.getPair(a, b).call()
            self.logger.debug(f"   Factory returned pair: {pair_addr}")
            if int(pair_addr, 16) == 0:
                self.logger.debug(f"getPair({a},{b}) вернул 0 — пары нет на данном DEX")
                return None
            
            # Сохраняем в кэш, если передано имя DEX
            if dex_name:
                self.cache_pair_address(dex_name, tokenA, tokenB, pair_addr)
                self.logger.debug(f"   💾 Сохранил адрес пары в кэш для {dex_name}")

            pair = self.w3.eth.contract(address=pair_addr, abi=PAIR_ABI)
            token0 = self.safe_web3_call(pair.functions.token0().call)
            token1 = self.safe_web3_call(pair.functions.token1().call)
            reserves = self.safe_web3_call(pair.functions.getReserves().call)
            if not reserves:
                self.logger.error(f"Не удалось получить резервы для пары {pair_addr}")
                return None
            r0, r1, _ = reserves

            return {"pair": pair_addr, "token0": token0, "token1": token1, "reserve0": r0, "reserve1": r1}
        except Exception as e:
            error_str = str(e)
            # Проверяем rate limit ошибку
            if "rate limit" in error_str.lower() or "too many requests" in error_str.lower():
                self.logger.warning(f"⚠️ Rate limit на {self.rpc_rotator.get_current()[:30]}..., переключаюсь на следующий RPC")
                
                # Принудительно переключаем RPC
                self.rpc_rotator.force_switch()
                new_rpc = self.rpc_rotator.get_current()
                
                # Обновляем Web3 подключение
                self.w3 = Web3(Web3.HTTPProvider(new_rpc))
                if CHAIN in ['polygon', 'bsc']:
                    from web3.middleware import ExtraDataToPOAMiddleware
                    self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                
                self.logger.info(f"✅ Переключился на {new_rpc[:30]}...")
                
                # Повторяем попытку с новым RPC (рекурсивный вызов)
                await asyncio.sleep(0.1)  # Небольшая задержка перед повтором
                return await self.get_v2_reserves_by_router(router_addr, tokenA, tokenB, dex_name)
            else:
                # Другие ошибки логируем как раньше
                self.logger.exception(f"get_v2_reserves_by_router failed: {e}")
                return None

    
    async def get_pair_reserves(self, pair_address: str) -> Optional[Dict]:  # ИСПРАВЛЕНА ТИПИЗАЦИЯ
        """Получает реальные резервы из пары"""
        try:
            pair_abi = json.loads('''[
                {
                    "constant": true,
                    "inputs": [],
                    "name": "getReserves",
                    "outputs": [
                        {"name": "reserve0", "type": "uint112"},
                        {"name": "reserve1", "type": "uint112"},
                        {"name": "blockTimestampLast", "type": "uint32"}
                    ],
                    "type": "function"
                },
                {
                    "constant": true,
                    "inputs": [],
                    "name": "token0",
                    "outputs": [{"name": "", "type": "address"}],
                    "type": "function"
                },
                {
                    "constant": true,
                    "inputs": [],
                    "name": "token1",
                    "outputs": [{"name": "", "type": "address"}],
                    "type": "function"
                }
            ]''')
            
            pair_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(pair_address),
                abi=pair_abi
            )
            
            # Получаем резервы и адреса токенов
            reserves = pair_contract.functions.getReserves().call()
            token0 = pair_contract.functions.token0().call()
            token1 = pair_contract.functions.token1().call()
            
            return {
                'reserve0': reserves[0],
                'reserve1': reserves[1],
                'token0': token0,
                'token1': token1
            }
        except:
            return None

    def calculate_mid_price_from_reserves(self, reserve0: int, reserve1: int, 
                                         token0_decimals: int, token1_decimals: int,
                                         base_is_token0: bool = True) -> float:
        """
        Рассчитывает цену TRADE токена в BASE токенах
        base_is_token0=True означает что token0 это BASE токен
        Возвращает: сколько BASE токенов стоит 1 TRADE токен
        """
        if reserve0 == 0 or reserve1 == 0:
            return 0
        
        # Приводим к нормальным числам с учетом decimals
        reserve0_normalized = reserve0 / (10 ** token0_decimals)
        reserve1_normalized = reserve1 / (10 ** token1_decimals)
        
        if base_is_token0:
            # token0 = BASE, token1 = TRADE
            # Цена = сколько BASE за 1 TRADE = reserve0/reserve1
            if reserve1_normalized == 0:
                return 0
            return reserve0_normalized / reserve1_normalized
        else:
            # token0 = TRADE, token1 = BASE  
            # Цена = сколько BASE за 1 TRADE = reserve1/reserve0
            if reserve0_normalized == 0:
                return 0
            return reserve1_normalized / reserve0_normalized

    def calculate_liquidity_usd(self, reserve_base: int, base_token_address: str, 
                               base_decimals: int) -> float:
        """
        Рассчитывает ликвидность пары в USD на основе резерва базового токена
        Ликвидность = 2 * (резерв_базового_токена * цена_базового_токена_в_USD)
        """
        from config import BASE_TOKEN_USD_PRICES
        
        # Получаем цену базового токена в USD
        base_price_usd = BASE_TOKEN_USD_PRICES.get(base_token_address.lower(), 0)
        
        if base_price_usd == 0:
            # Если нет цены, пробуем найти по символу
            token_symbol = self.base_tokens.get(base_token_address.lower(), '')
            if token_symbol in ['USDC', 'USDC.e', 'USDT', 'DAI']:
                base_price_usd = 1.0  # Стейблкоины = $1
            elif token_symbol == 'WETH':
                base_price_usd = 2500  # Примерная цена ETH
            elif token_symbol == 'WBTC':  
                base_price_usd = 40000  # Примерная цена BTC
            elif token_symbol == 'WMATIC':
                base_price_usd = 0.9  # Примерная цена MATIC
            elif token_symbol == 'AAVE':
                base_price_usd = 100  # Примерная цена AAVE
        
        # Нормализуем резерв
        reserve_normalized = reserve_base / (10 ** base_decimals)
        
        # Ликвидность = 2 * резерв_базового_токена * цена_в_USD
        liquidity = 2 * reserve_normalized * base_price_usd
        
        return liquidity

    def is_stablecoin(self, token_address: str) -> bool:
        """Проверяет, является ли токен стейблкоином"""
        if not token_address:
            return False
            
        stablecoins = {
            # Polygon stablecoins
            '0x2791bca1f2de4661ed88a30c99a7a9449aa84174',  # USDC.e
            '0x3c499c542cef5e3811e1192ce70d8cc03d5c3359',  # USDC native
            '0xc2132d05d31c914a87c6611c10748aeb04b58e8f',  # USDT
            '0x8f3cf7ad23cd3cadbd9735aff958023239c6a063',  # DAI
            '0x765277eebeca2e31912c9946eae1021199b39c61',  # DAI (другой адрес)
            '0xdab529f40e671a1d4bf91361c21bf9f0c9712ab7',  # BUSD
            '0x4e3decbb3645551b8a19f0ea1678079fcb33fb4c',  # jEUR
            '0xe111178a87a3bff0c8d18decba5798827539ae99',  # EURS
            # Ethereum stablecoins
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',  # USDC
            '0xdac17f958d2ee523a2206206994597c13d831ec7',  # USDT
            '0x6b175474e89094c44da98b954eedeac495271d0f',  # DAI
        }
        return token_address.lower() in stablecoins
    
    async def load_v2_combos(self) -> List[Dict]:
        """Загружает комбинации из v2_combos.jsonl и возвращает готовые для арбитража"""
        combos = []
        try:
            with open('v2_combos.jsonl', 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        combo = json.loads(line)
                        combos.append(combo)
            self.logger.info(f"📦 Загружено {len(combos)} комбинаций из v2_combos.jsonl")
            return combos
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки v2_combos.jsonl: {e}")
            return []

    async def get_all_active_pairs(self, combos: List[Dict] = None) -> List[Dict]:
        """Получает все пары из v2_combos и преобразует в формат для арбитража"""
        # Если combos не переданы, загружаем из файла
        if combos is None:
            combos = await self.load_v2_combos()
        
        if not combos:
            self.logger.error("❌ Нет данных из v2_combos.jsonl!")
            return []
        
        # Создаем список арбитражных возможностей напрямую
        all_pairs = []
        
        for combo in combos:
            # combo содержит:
            # - trade: адрес торгуемого токена (например WETH)
            # - base: адрес базового токена (например USDC)
            # - buy: {dex, pair} - где покупаем дешево
            # - sell: {dex, pair} - где продаем дорого
            
            # Создаем псевдо-объект в формате DexScreener для каждой возможности
            # Нам нужно две пары - одна для покупки, другая для продажи
            opportunity = {
                # Данные торгуемого токена
                'trade_token': combo['trade'],
                'base_token': combo['base'],
                
                # Пара для покупки
                'buy_dex': combo['buy']['dex'],
                'buy_pair': combo['buy']['pair'],
                
                # Пара для продажи
                'sell_dex': combo['sell']['dex'],
                'sell_pair': combo['sell']['pair'],
                
                # Эти поля для совместимости, реальные значения получим через ANKR
                'pairAddress': combo['buy']['pair'],  # Используем buy pair как основную
                'baseToken': {
                    'address': combo['base'],
                    'symbol': 'BASE'  # Определится позже
                },
                'quoteToken': {
                    'address': combo['trade'],
                    'symbol': 'TRADE'  # Определится позже
                },
                'dexId': combo['buy']['dex'],
                'chainId': CHAIN,
                'liquidity': {'usd': 0, 'base': 0, 'quote': 0},
                'volume': {'h24': 1},  # Ставим 1 чтобы пройти фильтр
                'priceNative': '0',
                'priceUsd': '0',
                
                # Специальный флаг для новой логики
                '_is_v2_combo': True,
                '_combo_data': combo
            }
            all_pairs.append(opportunity)
        
        # Выводим статистику
        dex_stats = {}
        for combo in combos:
            for side in ['buy', 'sell']:
                dex = combo[side]['dex']
                dex_stats[dex] = dex_stats.get(dex, 0) + 1
        
        self.logger.info(f"📊 Статистика загруженных комбинаций:")
        self.logger.info(f"   Всего арбитражных возможностей: {len(combos)}")
        self.logger.info(f"   Статистика по DEX:")
        for dex, count in sorted(dex_stats.items(), key=lambda x: x[1], reverse=True):
            self.logger.info(f"      {dex}: {count} использований")
        
        # Подсчитываем уникальные токены
        unique_trades = set(c['trade'] for c in combos)
        unique_bases = set(c['base'] for c in combos)
        self.logger.info(f"   Уникальных торговых токенов: {len(unique_trades)}")
        self.logger.info(f"   Уникальных базовых токенов: {len(unique_bases)}")
        
        return all_pairs
     
    async def process_v2_combos(self, combos: List[Dict]) -> List[ArbitrageOpportunity]:
        """Обрабатывает комбинации из v2_combos и находит прибыльные возможности - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
        opportunities = []
        
        # Инициализация счетчиков для правильной статистики
        self.combos_scan = 0  # Количество проверенных комбинаций
        self.cands_scan = 0   # Количество кандидатов со спредом > MIN_SPREAD
        self.n_low_spread = 0  # Отсеяно по спреду
        self.n_low_liquidity_usd = 0  # Отсеяно по ликвидности
        self.n_nets0 = 0  # Отсеяно по net_spread <= 0
        self.n_nonv2 = 0  # Отсеяно не V2 DEX
        self.n_low_spread_onchain = 0  # Отсеяно по onchain спреду
        self.skip_bad_reserves_onchain = 0  # Недоступны резервы
        self.skip_non_v2_pair = 0  # Не V2 пары
        self.skip_ankr_rate_limit = 0  # ANKR rate limits
        self.skip_address_mismatch = 0  # Несовпадение адресов
        
        self.logger.info(f"Обрабатываю {len(combos)} арбитражных комбинаций...")
        
        # ШАГ 1: Собираем ВСЕ уникальные пары и токены СРАЗУ
        unique_pairs = set()
        unique_tokens = set()
        
        for combo in combos:
            unique_pairs.add(combo['buy']['pair'])
            unique_pairs.add(combo['sell']['pair'])
            unique_tokens.add(combo['base'])
            unique_tokens.add(combo['trade'])
        
        self.logger.info(f"📊 Уникальных пар: {len(unique_pairs)}, уникальных токенов: {len(unique_tokens)}")
        
        # Инициализируем ANKR провайдер если еще нет
        from config import ANKR_API_KEY
        if not hasattr(self, 'ankr_provider'):
            if ANKR_API_KEY:
                from ankr_reserves import AnkrReservesProvider
                self.ankr_provider = AnkrReservesProvider(ANKR_API_KEY, self.w3, self.logger)
                self.logger.info("✅ ANKR провайдер инициализирован")
            else:
                self.ankr_provider = None
                self.logger.warning("⚠️ ANKR_API_KEY не найден, используем прямые RPC")
        
        # ШАГ 2: Получаем ВСЕ резервы параллельно с оптимальными батчами
        self.logger.info(f"🔄 Загружаю резервы для {len(unique_pairs)} пар...")
        all_reserves = {}
        
        if self.ankr_provider:
            # Используем новый метод с параллельными запросами
            reserves_tuples = await self.ankr_provider.get_reserves_many(list(unique_pairs))
            
            # Конвертируем кортежи в словари для совместимости с остальным кодом
            for pair, (r0, r1, t0, t1) in reserves_tuples.items():
                all_reserves[pair] = {
                    'reserve0': r0,
                    'reserve1': r1,
                    'token0': t0,
                    'token1': t1
                }
            
            self.logger.info(f"✅ Загружено резервов: {len(all_reserves)}/{len(unique_pairs)}")
        else:
            self.logger.warning("⚠️ ANKR провайдер не инициализирован")
        
        # ШАГ 3: Получаем decimals ПАРАЛЛЕЛЬНО с резервами
        self.logger.info(f"🔄 Загружаю decimals для {len(unique_tokens)} токенов...")
        
        if self.ankr_provider:
            # Запускаем загрузку decimals параллельно
            decimals_task = self.ankr_provider.get_decimals_many(list(unique_tokens))
            # Ждем завершения обеих задач
            decimals_cache = await decimals_task
        else:
            # Fallback на старый синхронный метод
            decimals_cache = {}
            for token in unique_tokens:
                decimals_cache[token] = self.get_token_decimals(token)
        
        self.logger.info(f"✅ Загружено decimals: {len(decimals_cache)}")
        
        # ШАГ 4: Теперь обрабатываем комбинации БЕЗ СЕТЕВЫХ ЗАПРОСОВ
        self.logger.info("🔍 Анализирую арбитражные возможности...")
        processed = 0
        
        for combo in combos:
            processed += 1
            if processed % 500 == 0:
                self.logger.debug(f"Обработано {processed}/{len(combos)} комбинаций...")
            
            try:
                # Увеличиваем счетчик проверенных комбинаций
                self.combos_scan += 1
                
                # Получаем резервы из кэша
                buy_reserves = all_reserves.get(combo['buy']['pair'])
                sell_reserves = all_reserves.get(combo['sell']['pair'])
                
                if not buy_reserves or not sell_reserves:
                    self.skip_bad_reserves_onchain += 1
                    continue
                
                # Определяем токены с защитой от NPE
                if not combo.get('base') or not combo.get('trade'):
                    continue
                try:
                    base_addr = combo['base'].lower()
                    trade_addr = combo['trade'].lower()
                except AttributeError:
                    continue
                
                # Получаем decimals из кэша
                base_decimals = decimals_cache.get(combo['base'], 18)
                trade_decimals = decimals_cache.get(combo['trade'], 18)
                
                # Для buy пары с защитой от None
                buy_token0 = buy_reserves.get('token0', '')
                buy_token1 = buy_reserves.get('token1', '')
                if not buy_token0 or not buy_token1:
                    continue
                    
                if buy_token0.lower() == base_addr:
                    buy_base_reserve = buy_reserves['reserve0']
                    buy_trade_reserve = buy_reserves['reserve1']
                elif buy_reserves.get('token1', '').lower() == base_addr:
                    buy_base_reserve = buy_reserves['reserve1']
                    buy_trade_reserve = buy_reserves['reserve0']
                else:
                    continue
                
                # Для sell пары с защитой от None
                sell_token0 = sell_reserves.get('token0', '')
                sell_token1 = sell_reserves.get('token1', '')
                if not sell_token0 or not sell_token1:
                    continue
                    
                if sell_token0.lower() == base_addr:
                    sell_base_reserve = sell_reserves['reserve0']
                    sell_trade_reserve = sell_reserves['reserve1']
                elif sell_reserves.get('token1', '').lower() == base_addr:
                    sell_base_reserve = sell_reserves['reserve1']
                    sell_trade_reserve = sell_reserves['reserve0']
                else:
                    continue
                
                # Проверяем минимальные резервы
                if any(r == 0 for r in [buy_base_reserve, buy_trade_reserve, sell_base_reserve, sell_trade_reserve]):
                    self.skip_bad_reserves_onchain += 1
                    continue
                
                # Конвертируем резервы с учетом decimals
                buy_base_decimal = buy_base_reserve / (10 ** base_decimals)
                buy_trade_decimal = buy_trade_reserve / (10 ** trade_decimals)
                sell_base_decimal = sell_base_reserve / (10 ** base_decimals)
                sell_trade_decimal = sell_trade_reserve / (10 ** trade_decimals)
                
                # Рассчитываем цены
                buy_price = buy_base_decimal / buy_trade_decimal  # Цена покупки (сколько base за 1 trade)
                sell_price = sell_base_decimal / sell_trade_decimal  # Цена продажи
                
                # Увеличиваем счетчик проверенных комбинаций
                self.combos_scan += 1
                
                # Проверяем спред
                if sell_price <= buy_price:
                    continue  # Нет арбитража
                
                spread = (sell_price - buy_price) / buy_price
                
                # Минимальный спред для анализа
                if spread < MIN_SPREAD_PERCENT / 100:
                    self.n_low_spread = getattr(self, 'n_low_spread', 0) + 1
                    continue
                    
                # Увеличиваем счетчик кандидатов (прошли фильтр по спреду)
                self.cands_scan += 1
                
                # Рассчитываем ликвидность в USD
                base_price_usd = BASE_TOKEN_USD_PRICES.get(base_addr, 1.0)
                buy_liquidity_usd = buy_base_decimal * base_price_usd * 2
                sell_liquidity_usd = sell_base_decimal * base_price_usd * 2
                min_liquidity = min(buy_liquidity_usd, sell_liquidity_usd)
                
                # Проверяем минимальную ликвидность
                if min_liquidity < MIN_LIQUIDITY_USD:
                    self.n_low_liquidity_usd += 1
                    continue
                
                # Используем LUT для определения оптимального размера
                optimal_size = 0
                max_profit = 0
                
                if self.lut:
                    # Упрощенный расчет через LUT
                    r = (sell_base_decimal * buy_trade_decimal) / (buy_base_decimal * sell_trade_decimal)
                    
                    # Получаем оптимальный размер из LUT
                    g_optimal = size_from_lut(self.lut, spread, r)
                    
                    if g_optimal > 0:
                        # Рассчитываем размер сделки
                        L = min(buy_liquidity_usd, sell_liquidity_usd) / base_price_usd
                        optimal_size = L * g_optimal
                        
                        # Упрощенный расчет прибыли
                        max_profit = optimal_size * spread * 0.5  # Примерная оценка
                
                # Создаем объект возможности
                if optimal_size > 0:
                    # Получаем символ базового токена
                    base_symbol = self.get_base_token_symbol(combo['base'])
                    
                    opportunity = ArbitrageOpportunity(
                        token_address=combo['trade'],
                        token_symbol=combo.get('trade_symbol', 'UNKNOWN'),
                        quote_token_address=combo['base'],
                        quote_token_symbol=base_symbol,
                        buy_dex=combo['buy']['dex'],
                        sell_dex=combo['sell']['dex'],
                        buy_price=buy_price,
                        sell_price=sell_price,
                        spread_percent=spread * 100,
                        liquidity_usd=min_liquidity,
                        expected_profit=max_profit,
                        optimal_trade_size=optimal_size,
                        price_impact=0,  # Будет рассчитан позже
                        buy_pair=combo['buy']['pair'],
                        sell_pair=combo['sell']['pair'],
                        buy_router=combo['buy'].get('router'),
                        sell_router=combo['sell'].get('router'),
                        base_token_address=combo['base']
                    )
                    opportunities.append(opportunity)
                    
            except Exception as e:
                if processed <= 10:  # Логируем только первые ошибки
                    self.logger.debug(f"Ошибка обработки комбинации: {e}")
                continue
        
        self.logger.info(f"✅ Найдено {len(opportunities)} прибыльных возможностей из {len(combos)} комбинаций")
        return opportunities
     
    def calculate_price_impact_amm(self, amount_in: float, reserve_in: float, reserve_out: float, fee_percent: float = 0.3) -> Tuple[float, float]:
        """
        v6.7: Точный расчет price impact для AMM (Uniswap V2 формула)
        Возвращает: (amount_out, price_impact_percent)
        """
        if reserve_in <= 0 or reserve_out <= 0 or amount_in <= 0:
            return 0, 0
        
        # Исходная цена без impact
        original_price = reserve_out / reserve_in
        
        # AMM формула с учетом комиссии
        fee_multiplier = 1 - (fee_percent / 100)
        amount_in_with_fee = amount_in * fee_multiplier
        
        # Расчет по формуле x*y=k
        # amount_out = (amount_in_with_fee * reserve_out) / (reserve_in + amount_in_with_fee)
        amount_out = (amount_in_with_fee * reserve_out) / (reserve_in + amount_in_with_fee)
        
        # Реальная цена после свопа
        actual_price = amount_out / amount_in
        
        # Price impact в процентах
        price_impact = ((original_price - actual_price) / original_price) * 100
        
        return amount_out, price_impact
    
        
    async def calculate_real_profit(self, base_amount: float, buy_price: float, sell_price: float,
                                buy_liquidity: float, sell_liquidity: float, buy_dex: str = "", sell_dex: str = "",
                                base_token_symbol: str = "USDC", buy_pair: str = None, sell_pair: str = None,
                                base_token_address: str = None, trade_token_address: str = None,
                                buy_base_reserve: float = 0, buy_quote_reserve: float = 0,      # НОВОЕ
                                sell_base_reserve: float = 0, sell_quote_reserve: float = 0) -> Tuple[float, float]:  # НОВОЕ
            """v7.3: Точный расчет прибыли с реальными резервами из блокчейна"""
            # Убрали отладочные логи
            
            # Получаем комиссии DEX
            buy_dex_fee = get_dex_fee(buy_dex if buy_dex else 'default')
            sell_dex_fee = get_dex_fee(sell_dex if sell_dex else 'default')
            
            # ШАГ 1: Покупаем токены с учетом AMM price impact
            # v7.3: Сначала пробуем резервы из DEXScreener, потом из блокчейна
            if buy_base_reserve > 0 and buy_quote_reserve > 0:
                # Используем резервы из DEXScreener
                buy_reserve_base = buy_base_reserve
                buy_reserve_trade = buy_quote_reserve
                # Убрали отладку - слишком часто выводится
            elif buy_pair:
                reserves_data = await self.get_pair_reserves(buy_pair)
                self.logger.debug(f"Результат get_pair_reserves для {buy_pair}: {reserves_data}")
                if reserves_data and base_token_address and trade_token_address:
                    base_decimals = self.get_token_decimals(base_token_address)
                    trade_decimals = self.get_token_decimals(trade_token_address)
                    
                    # ОТЛАДКА: проверяем правильность определения токенов
                    self.logger.info(f"🔍 ОТЛАДКА РЕЗЕРВОВ ПОКУПКИ:")
                    self.logger.info(f"   token0 в паре: {reserves_data['token0']}")
                    self.logger.info(f"   token1 в паре: {reserves_data['token1']}")
                    self.logger.info(f"   base_token (WETH/USDC): {base_token_address}")
                    self.logger.info(f"   trade_token (AAVE/др.): {trade_token_address}")
                    self.logger.info(f"   reserve0 (сырой): {reserves_data['reserve0']}")
                    self.logger.info(f"   reserve1 (сырой): {reserves_data['reserve1']}")
                    
                    if reserves_data['token0'].lower() == base_token_address.lower():
                        buy_reserve_base = reserves_data['reserve0'] / (10 ** base_decimals)
                        buy_reserve_trade = reserves_data['reserve1'] / (10 ** trade_decimals)
                        self.logger.info(f"   ✅ token0 = base, token1 = trade")
                    else:
                        buy_reserve_base = reserves_data['reserve1'] / (10 ** base_decimals)
                        buy_reserve_trade = reserves_data['reserve0'] / (10 ** trade_decimals)
                        self.logger.info(f"   ✅ token0 = trade, token1 = base")
                
                    self.logger.info(f"   Итого: base_reserve={buy_reserve_base:.2f}, trade_reserve={buy_reserve_trade:.2f}")
                    self.logger.debug(f"Реальные резервы покупки из блокчейна: base={buy_reserve_base:.2f}, trade={buy_reserve_trade:.2f}")
                else:
                    # Нет онchain данных - скипаем эту возможность
                    self.logger.debug("Нет резервов для пары покупки - скипаем")
                    return 0, 0, 0
            
            tokens_received, buy_impact = self.calculate_price_impact_amm(
                    base_amount,
                    buy_reserve_base,
                    buy_reserve_trade,
                    buy_dex_fee
                )
            
            # ШАГ 2: Продаем токены с учетом AMM price impact
            # v7.3: Сначала пробуем резервы из DEXScreener, потом из блокчейна
            if sell_base_reserve > 0 and sell_quote_reserve > 0:
                # DEXScreener уже даёт в токенах - используем как есть
                sell_reserve_base = sell_base_reserve or 0.0
                sell_reserve_trade = sell_quote_reserve or 0.0
                # Убрали отладку - слишком часто выводится
            elif sell_pair:

                reserves_data = await self.get_pair_reserves(sell_pair)
                if reserves_data and base_token_address and trade_token_address:
                    base_decimals = self.get_token_decimals(base_token_address)
                    trade_decimals = self.get_token_decimals(trade_token_address)
                    
                    # ОТЛАДКА: проверяем правильность определения токенов
                    self.logger.info(f"🔍 ОТЛАДКА РЕЗЕРВОВ ПРОДАЖИ:")
                    self.logger.info(f"   token0 в паре: {reserves_data['token0']}")
                    self.logger.info(f"   token1 в паре: {reserves_data['token1']}")
                    self.logger.info(f"   base_token (WETH/USDC): {base_token_address}")
                    self.logger.info(f"   trade_token (AAVE/др.): {trade_token_address}")
                    self.logger.info(f"   reserve0 (сырой): {reserves_data['reserve0']}")
                    self.logger.info(f"   reserve1 (сырой): {reserves_data['reserve1']}")
                    
                    if reserves_data['token0'].lower() == trade_token_address.lower():
                        sell_reserve_trade = reserves_data['reserve0'] / (10 ** trade_decimals)
                        sell_reserve_base = reserves_data['reserve1'] / (10 ** base_decimals)
                        self.logger.info(f"   ✅ token0 = trade, token1 = base")
                    else:
                        sell_reserve_trade = reserves_data['reserve1'] / (10 ** trade_decimals)
                        sell_reserve_base = reserves_data['reserve0'] / (10 ** base_decimals)
                        self.logger.info(f"   ✅ token0 = base, token1 = trade")
                
                    self.logger.info(f"   Итого: trade_reserve={sell_reserve_trade:.2f}, base_reserve={sell_reserve_base:.2f}")
                    self.logger.debug(f"Резервы продажи из блокчейна: base={sell_reserve_base:.2f}, trade={sell_reserve_trade:.2f}")
            else:
                # Нет онchain данных - скипаем эту возможность
                self.logger.debug("Нет резервов для пары продажи - скипаем")
                return 0, 0, 0
            
            base_received, sell_impact = self.calculate_price_impact_amm(
                tokens_received,
                sell_reserve_trade,
                sell_reserve_base,
                sell_dex_fee
            )
            
            # ШАГ 3: Вычитаем Flash Loan fee
            flash_loan_fee = base_amount * (get_flash_loan_fee() / 100)
            base_after_flash_fee = base_received - flash_loan_fee
            
            # ШАГ 4: Вычитаем газ
            gas_cost_usd = get_gas_cost(CHAIN)
            if base_token_symbol in ["WMATIC", "WETH"]:
                token_prices = {"WMATIC": 0.5, "WETH": 2300}
                token_price = token_prices.get(base_token_symbol, 1)
                gas_cost_in_base = gas_cost_usd / token_price
            else:
                gas_cost_in_base = gas_cost_usd
            
            # ИТОГОВАЯ ПРИБЫЛЬ - используем ту же формулу что и LUT
            # expected_out_base = base_received (после второго свопа)
            # repay_base = base_amount + flash_loan_fee
            # min_profit_base = gas_cost_in_base
            expected_out_base = base_received
            repay_base = base_amount + flash_loan_fee
            min_profit_base = gas_cost_in_base

            net_profit = expected_out_base - repay_base - min_profit_base
            
            # Конвертируем в USD
            if base_token_symbol in ["WMATIC", "WETH"]:
                token_prices = {"WMATIC": 0.5, "WETH": 2300}
                net_profit_usd = net_profit * token_prices.get(base_token_symbol, 1)
            else:
                net_profit_usd = net_profit
            
            total_impact = buy_impact + sell_impact
            
            return net_profit_usd, total_impact
        
    async def find_optimal_trade_size(self, opportunity: Dict, base_token_symbol: str = "USDC") -> Tuple[float, float, float]:
        """Используем LUT для определения оптимального размера с ANKR резервами"""
        
        if not self.lut:
            self.logger.error("LUT не загружена!")
            return 0, 0, 0
        
        buy_price = opportunity.get('buy_price', 0)
        sell_price = opportunity.get('sell_price', 0)
        
        # Если цены не заданы, восстанавливаем из спреда
        if buy_price == 0 or sell_price == 0:
            spread_decimal = opportunity['spread'] / 100
            buy_price = 1.0
            sell_price = buy_price * (1 + spread_decimal)
        
        # Вычисляем спред для LUT
        spread = (sell_price - buy_price) / buy_price
        
        # ИСПОЛЬЗУЕМ ANKR ДЛЯ ПОЛУЧЕНИЯ РЕЗЕРВОВ
        from config import ANKR_API_KEY
        
        buy_pair = opportunity.get('buy_pair')
        sell_pair = opportunity.get('sell_pair')
        
        if buy_pair and sell_pair:
            # Инициализируем флаги резолва
            resolved_buy_from_factory = False
            resolved_sell_from_factory = False
            
            # v7.1.2: Проверяем что это V2-пары
            is_buy_v2 = await self.is_v2_pair(buy_pair)
            is_sell_v2 = await self.is_v2_pair(sell_pair)
            
            # Если пара была резолвлена через фабрику - считаем её V2
            if resolved_buy_from_factory:
                is_buy_v2 = True
            if resolved_sell_from_factory:
                is_sell_v2 = True
            
            if not is_buy_v2 or not is_sell_v2:
                self.logger.debug(f"Пропускаем non-V2 пары: buy_v2={is_buy_v2}, sell_v2={is_sell_v2}")
                self.logger.debug(f"   buy_pair={buy_pair}, sell_pair={sell_pair}")
                self.logger.debug(f"   resolved_buy={resolved_buy_from_factory}, resolved_sell={resolved_sell_from_factory}")
                self.skip_non_v2_pair += 1
                return 0, 0, 0
            
            # Инициализируем ANKR провайдер если еще нет
            if not hasattr(self, 'ankr_provider'):
                if ANKR_API_KEY:
                    from ankr_reserves import AnkrReservesProvider
                    self.ankr_provider = AnkrReservesProvider(ANKR_API_KEY, self.w3, self.logger)
                    self.logger.info("✅ ANKR провайдер инициализирован")
                else:
                    self.ankr_provider = None
                    self.logger.warning("⚠️ ANKR_API_KEY не найден, используем прямые RPC")
            
            # Получаем резервы через ANKR или fallback на RPC
            if self.ankr_provider:
                # Батч запрос через ANKR
                reserves_map = await self.ankr_provider.get_reserves_batch([buy_pair, sell_pair])
                buy_reserves = reserves_map.get(buy_pair, {})
                sell_reserves = reserves_map.get(sell_pair, {})
                
               # Если не получили резервы для buy_pair, пробуем через factory
                if not buy_reserves or 'reserve0' not in buy_reserves:
                    self.logger.warning(f"⚠️ Buy pair {buy_pair[:10]} failed, trying factory lookup...")
                    # КРИТИЧНО: Сначала получаем токены из самой DS-пары
                    try:
                        pair_contract = self.w3.eth.contract(
                            address=Web3.to_checksum_address(buy_pair),
                            abi=json.loads('[{"constant":true,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"}]')
                        )
                        ds_token0 = pair_contract.functions.token0().call()
                        ds_token1 = pair_contract.functions.token1().call()
                        self.logger.info(f"   DS pair={buy_pair} → factory.getPair(tokenA={ds_token0},tokenB={ds_token1}) (dex={opportunity.get('buy_dex', 'unknown')})")
                        self.logger.info(f"   DS pair tokens: token0={ds_token0}, token1={ds_token1}")
                        if len(ds_token0) != 42 or len(ds_token1) != 42:
                            self.logger.warning(f"   ⚠️ ПРЕДУПРЕЖДЕНИЕ: получены неполные адреса! Использую из opportunity")
                            ds_token0 = opportunity.get('base_token_address', ds_token0)
                            ds_token1 = opportunity.get('trade_token_address', ds_token1)
                    except:
                        ds_token0 = opportunity.get('base_token_address')
                        ds_token1 = opportunity.get('trade_token_address')
                    
                    # Пробуем получить правильный адрес пары через factory
                    buy_dex = opportunity.get('buy_dex')
                    from config import DEX_CONFIG
                    buy_router = opportunity.get('buy_router') or DEX_CONFIG.get(buy_dex, {}).get('router')
                    self.logger.info(f"   🔍 Trying factory resolution: buy_router={buy_router}, buy_dex={buy_dex}")
                    if buy_router and buy_dex:
                        # КРИТИЧНО: используем ПОЛНЫЕ адреса из opportunity, а не из DS-пары!
                        full_token0 = Web3.to_checksum_address(opportunity.get('base_token_address', ds_token0))
                        full_token1 = Web3.to_checksum_address(opportunity.get('trade_token_address', ds_token1))
                        
                        self.logger.info(f"   factory args: tokenA={full_token0}, tokenB={full_token1}")
                        
                        # Пробуем вычислить через CREATE2 BUY
                        from config import DEX_CONFIG
                        dex_config = DEX_CONFIG.get(buy_dex, {})
                        self.logger.debug(f"   DEX config for {buy_dex}: {dex_config}")
                        if 'init_code_pair_hash' in dex_config and 'factory' in dex_config:
                            computed_pair = self.compute_pair_address_create2(
                                dex_config['factory'],
                                full_token0,
                                full_token1,
                                dex_config['init_code_pair_hash']
                            )
                            self.logger.info(f"   📐 CREATE2 вычислил адрес: {computed_pair}")
                            
                            # Проверяем что по этому адресу есть контракт
                            bytecode = self.w3.eth.get_code(Web3.to_checksum_address(computed_pair))
                            if len(bytecode) > 2:
                                correct_buy_pair = computed_pair
                                self.logger.info(f"   ✅ CREATE2 адрес валидный (есть bytecode)")
                                # Получаем резервы
                                buy_reserves = await self.get_pair_reserves(correct_buy_pair)
                                if buy_reserves:
                                    # Обновляем адрес пары
                                    opportunity['buy_pair'] = correct_buy_pair
                                    buy_pair = correct_buy_pair
                                    self.logger.info(f"   ✅ Адрес обновлён через CREATE2")
                                    resolved_buy_from_factory = True
                            else:
                                self.logger.warning(f"   ⚠️ CREATE2 адрес пустой (нет bytecode), fallback на factory.getPair")
                                factory_pair_data = await self.get_v2_reserves_by_router(
                                    buy_router,
                                    full_token0,
                                    full_token1,
                                    buy_dex
                                )
                                if factory_pair_data and 'pair' in factory_pair_data:
                                    correct_buy_pair = factory_pair_data['pair']
                                    self.logger.info(f"✅ Factory gave correct pair: {correct_buy_pair}")
                                    if correct_buy_pair.lower() != buy_pair.lower():
                                        self.logger.info(f"   🔄 ПОДМЕНА АДРЕСА: old={buy_pair} → new={correct_buy_pair}")
                                    else:
                                        self.logger.info(f"   ℹ️ Адрес не изменился (уже был правильный)")
                                    buy_reserves = {
                                        'reserve0': factory_pair_data['reserve0'],
                                        'reserve1': factory_pair_data['reserve1'],
                                        'token0': factory_pair_data['token0'],
                                        'token1': factory_pair_data['token1']
                                    }
                                    opportunity['buy_pair'] = correct_buy_pair
                                    buy_pair = correct_buy_pair
                                    self.logger.info(f"✅ Buy pair updated via factory.getPair")
                                    resolved_buy_from_factory = True
                        else:
                            # Fallback на старый метод
                            factory_pair_data = await self.get_v2_reserves_by_router(
                                buy_router,
                                full_token0,
                                full_token1,
                                buy_dex
                            )
                        if factory_pair_data and 'pair' in factory_pair_data:
                            correct_buy_pair = factory_pair_data['pair']
                            self.logger.info(f"✅ Factory gave correct pair: {correct_buy_pair}")
                            if correct_buy_pair.lower() != buy_pair.lower():
                                self.logger.info(f"   🔄 ПОДМЕНА АДРЕСА: old={buy_pair} → new={correct_buy_pair}")
                            else:
                                self.logger.info(f"   ℹ️ Адрес не изменился (уже был правильный)")
                            buy_reserves = {
                                'reserve0': factory_pair_data['reserve0'],
                                'reserve1': factory_pair_data['reserve1'],
                                'token0': factory_pair_data['token0'],
                                'token1': factory_pair_data['token1']
                            }
                           # КРИТИЧНО: Обновляем адрес пары ВЕЗДЕ
                            opportunity['buy_pair'] = correct_buy_pair
                            buy_pair = correct_buy_pair  # Обновляем локальную переменную
                            self.logger.info(f"✅ Buy pair updated: old={opportunity.get('buy_pair')[:10]} → new={correct_buy_pair[:10]}")
                            # Проверяем что дальше используется новый адрес
                            assert buy_pair == correct_buy_pair, "Address not updated!"
                            self.logger.info(f"   resolved_v2_from_factory = True")
                            resolved_buy_from_factory = True
                
                # То же самое для sell_pair
                if not sell_reserves or 'reserve0' not in sell_reserves:
                    self.logger.warning(f"⚠️ Sell pair {sell_pair[:10]} failed, trying factory lookup...")
                    # КРИТИЧНО: Сначала получаем токены из самой DS-пары
                    try:
                        pair_contract = self.w3.eth.contract(
                            address=Web3.to_checksum_address(sell_pair),
                            abi=json.loads('[{"constant":true,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"}]')
                        )
                        ds_token0 = pair_contract.functions.token0().call()
                        ds_token1 = pair_contract.functions.token1().call()
                        self.logger.info(f"   DS pair={sell_pair} → factory.getPair(tokenA={ds_token0},tokenB={ds_token1}) (dex={opportunity.get('sell_dex', 'unknown')})")
                        self.logger.info(f"   DS pair tokens: token0={ds_token0}, token1={ds_token1}")
                        if len(ds_token0) != 42 or len(ds_token1) != 42:
                            self.logger.warning(f"   ⚠️ ПРЕДУПРЕЖДЕНИЕ: получены неполные адреса! Использую из opportunity")
                            ds_token0 = opportunity.get('base_token_address', ds_token0)
                            ds_token1 = opportunity.get('trade_token_address', ds_token1)
                    except:
                        ds_token0 = opportunity.get('base_token_address')
                        ds_token1 = opportunity.get('trade_token_address')
                   
                    # Пробуем получить правильный адрес пары через factory
                    sell_dex = opportunity.get('sell_dex')
                    from config import DEX_CONFIG
                    sell_router = opportunity.get('sell_router') or DEX_CONFIG.get(sell_dex, {}).get('router')
                    self.logger.info(f"   🔍 Trying factory resolution: sell_router={sell_router}, sell_dex={sell_dex}")
                    if sell_router and sell_dex:
                        # КРИТИЧНО: используем ПОЛНЫЕ адреса из opportunity, а не из DS-пары!
                        full_token0 = Web3.to_checksum_address(opportunity.get('base_token_address', ds_token0))
                        full_token1 = Web3.to_checksum_address(opportunity.get('trade_token_address', ds_token1))
                        
                        self.logger.info(f"   factory args: tokenA={full_token0}, tokenB={full_token1}")
                        
                        # Пробуем вычислить через CREATE2 SELL
                        from config import DEX_CONFIG
                        dex_config = DEX_CONFIG.get(sell_dex, {})
                        self.logger.debug(f"   DEX config for {sell_dex}: {dex_config}")
                        if 'init_code_pair_hash' in dex_config and 'factory' in dex_config:
                            computed_pair = self.compute_pair_address_create2(
                                dex_config['factory'],
                                full_token0,
                                full_token1,
                                dex_config['init_code_pair_hash']
                            )
                            self.logger.info(f"   📐 CREATE2 вычислил адрес: {computed_pair}")
                            
                            # Проверяем что по этому адресу есть контракт
                            bytecode = self.w3.eth.get_code(Web3.to_checksum_address(computed_pair))
                            if len(bytecode) > 2:
                                correct_sell_pair = computed_pair
                                self.logger.info(f"   ✅ CREATE2 адрес валидный (есть bytecode)")
                                # Получаем резервы
                                sell_reserves = await self.get_pair_reserves(correct_sell_pair)
                                if sell_reserves:
                                    # Обновляем адрес пары
                                    opportunity['sell_pair'] = correct_sell_pair
                                    sell_pair = correct_sell_pair
                                    self.logger.info(f"   ✅ Адрес обновлён через CREATE2")
                                    resolved_sell_from_factory = True
                            else:
                                self.logger.warning(f"   ⚠️ CREATE2 адрес пустой (нет bytecode), fallback на factory.getPair")
                                factory_pair_data = await self.get_v2_reserves_by_router(
                                    sell_router,
                                    full_token0,
                                    full_token1,
                                    sell_dex
                                )
                                if factory_pair_data and 'pair' in factory_pair_data:
                                    correct_sell_pair = factory_pair_data['pair']
                                    self.logger.info(f"✅ Factory gave correct pair: {correct_sell_pair}")
                                    if correct_sell_pair.lower() != sell_pair.lower():
                                        self.logger.info(f"   🔄 ПОДМЕНА АДРЕСА: old={sell_pair} → new={correct_sell_pair}")
                                    else:
                                        self.logger.info(f"   ℹ️ Адрес не изменился (уже был правильный)")
                                    sell_reserves = {
                                        'reserve0': factory_pair_data['reserve0'],
                                        'reserve1': factory_pair_data['reserve1'],
                                        'token0': factory_pair_data['token0'],
                                        'token1': factory_pair_data['token1']
                                    }
                                    opportunity['sell_pair'] = correct_sell_pair
                                    sell_pair = correct_sell_pair
                                    self.logger.info(f"✅ Sell pair updated via factory.getPair")
                                    resolved_sell_from_factory = True
                        else:
                            # Fallback на старый метод
                            factory_pair_data = await self.get_v2_reserves_by_router(
                                sell_router,
                                full_token0,
                                full_token1,
                                sell_dex
                            )
                        if factory_pair_data and 'pair' in factory_pair_data:
                            correct_sell_pair = factory_pair_data['pair']
                            self.logger.info(f"✅ Factory gave correct pair: {correct_sell_pair}")
                            if correct_sell_pair.lower() != sell_pair.lower():
                                self.logger.info(f"   🔄 ПОДМЕНА АДРЕСА: old={sell_pair} → new={correct_sell_pair}")
                            else:
                                self.logger.info(f"   ℹ️ Адрес не изменился (уже был правильный)")
                            sell_reserves = {
                                'reserve0': factory_pair_data['reserve0'],
                                'reserve1': factory_pair_data['reserve1'],
                                'token0': factory_pair_data['token0'],
                                'token1': factory_pair_data['token1']
                            }
                            # КРИТИЧНО: Обновляем адрес пары ВЕЗДЕ
                            opportunity['sell_pair'] = correct_sell_pair
                            sell_pair = correct_sell_pair  # Обновляем локальную переменную
                            self.logger.info(f"✅ Sell pair updated: old={opportunity.get('sell_pair')[:10]} → new={correct_sell_pair[:10]}")
                            # Проверяем что дальше используется новый адрес
                            assert sell_pair == correct_sell_pair, "Address not updated!"
                            self.logger.info(f"   resolved_v2_from_factory = True")
                            resolved_sell_from_factory = True
                
                # Статистика использования ANKR vs RPC
                ankr_success = 0
                rpc_fallback = 0
                
                # Если ANKR не вернул резервы - пробуем прямой RPC
                if not buy_reserves or 'reserve0' not in buy_reserves:
                    self.logger.warning(f"⚠️ ANKR FAIL для buy_pair {buy_pair} -> используем RPC fallback")
                    # Проверяем, это rate limit или другая ошибка
                    self.skip_ankr_rate_limit = getattr(self, 'skip_ankr_rate_limit', 0) + 1
                    buy_reserves = await self.get_pair_reserves(buy_pair)
                    rpc_fallback += 1
                    if not buy_reserves:
                        self.logger.error(f"❌ Не удалось получить резервы ни через ANKR, ни через RPC для buy_pair")
                        self.skip_bad_reserves_onchain += 1
                        return 0, 0, 0
                else:
                    ankr_success += 1
                    self.logger.debug(f"✅ ANKR SUCCESS для buy_pair")
                    
                if not sell_reserves or 'reserve0' not in sell_reserves:
                    self.logger.warning(f"⚠️ ANKR FAIL для sell_pair {sell_pair} -> используем RPC fallback")
                    sell_reserves = await self.get_pair_reserves(sell_pair)
                    rpc_fallback += 1
                    if not sell_reserves:
                        self.logger.error(f"❌ Не удалось получить резервы ни через ANKR, ни через RPC для sell_pair")
                        self.skip_bad_reserves_onchain += 1
                        return 0, 0, 0
                else:
                    ankr_success += 1
                    self.logger.debug(f"✅ ANKR SUCCESS для sell_pair")
                
                # Выводим статистику
                self.logger.info(f"📊 ANKR статистика: успешно {ankr_success}/2, RPC fallback {rpc_fallback}/2")
                
                # Накапливаем общую статистику
                if not hasattr(self, 'ankr_stats'):
                    self.ankr_stats = {'success': 0, 'fail': 0}
                self.ankr_stats['success'] += ankr_success
                self.ankr_stats['fail'] += rpc_fallback
                    
                self.logger.debug(f"📡 Резервы получены (ANKR + RPC fallback)")
                
                # ========== НОВЫЙ КОД: ФИЛЬТР ПО ONCHAIN СПРЕДУ ==========
                # После получения резервов рассчитываем реальный спред
                if buy_reserves and sell_reserves:
                    # Определяем токены
                    base_token = opportunity.get('base_token_address')
                    trade_token = opportunity.get('trade_token_address', opportunity.get('token_address'))
                    
                    # Проверяем что резервы получены
                    if ('reserve0' in buy_reserves and 'reserve1' in buy_reserves and 
                        'reserve0' in sell_reserves and 'reserve1' in sell_reserves):
                        
                        # Используем новый метод для расчета цен
                        # Для buy пары
                        buy_token0_is_base = buy_reserves.get('token0', '').lower() == base_token.lower()
                        buy_price_onchain = self.calculate_mid_price_from_reserves(
                            buy_reserves['reserve0'],
                            buy_reserves['reserve1'],
                            self.get_token_decimals(buy_reserves.get('token0')),
                            self.get_token_decimals(buy_reserves.get('token1')),
                            base_is_token0=buy_token0_is_base
                        )
                        
                        # Для sell пары  
                        sell_token0_is_base = sell_reserves.get('token0', '').lower() == base_token.lower()
                        sell_price_onchain = self.calculate_mid_price_from_reserves(
                            sell_reserves['reserve0'],
                            sell_reserves['reserve1'],
                            self.get_token_decimals(sell_reserves.get('token0')),
                            self.get_token_decimals(sell_reserves.get('token1')),
                            base_is_token0=sell_token0_is_base
                        )
                        
                        # Рассчитываем onchain спред
                        if buy_price_onchain > 0 and sell_price_onchain > 0:
                            # Проверяем правильность направления
                            if sell_price_onchain <= buy_price_onchain:
                                # Цены перевернуты - меняем местами buy и sell
                                buy_price_onchain, sell_price_onchain = sell_price_onchain, buy_price_onchain
                                buy_reserves, sell_reserves = sell_reserves, buy_reserves
                                
                                # Флаг что нужно поменять местами DEX
                                flipped = True
                                self.logger.debug(f"🔄 Обнаружен переворот цен: sell_price <= buy_price")
                            else:
                                flipped = False
                            
                            spread_onchain = (sell_price_onchain - buy_price_onchain) / buy_price_onchain * 100
                            
                            self.logger.debug(f"📊 Onchain спред: {spread_onchain:.3f}% (buy: {buy_price_onchain:.6f}, sell: {sell_price_onchain:.6f})")
                        
                            # ФИЛЬТРУЕМ по onchain спреду
                            if spread_onchain < MIN_SPREAD_PERCENT:
                                self.logger.debug(f"❌ Отсеяно по onchain спреду: {spread_onchain:.3f}% < {MIN_SPREAD_PERCENT}%")
                                self.n_low_spread_onchain = getattr(self, 'n_low_spread_onchain', 0) + 1
                                return 0, 0, 0, False
                            
                            # НОВОЕ: Проверяем USD ликвидность на основе onchain резервов
                            if buy_reserves and 'reserve0' in buy_reserves and 'reserve1' in buy_reserves:
                                # Определяем какой резерв - базовый токен
                                if buy_reserves.get('token0', '').lower() == base_token.lower():
                                    base_reserve_raw = buy_reserves['reserve0']
                                else:
                                    base_reserve_raw = buy_reserves['reserve1']
                                
                                # Рассчитываем USD ликвидность
                                base_decimals = self.get_token_decimals(base_token)
                                liquidity_usd = self.calculate_liquidity_usd(
                                    base_reserve_raw, 
                                    base_token, 
                                    base_decimals
                                )
                                
                                if liquidity_usd < MIN_LIQUIDITY_USD:
                                    self.logger.debug(f"❌ Отсеяно по USD ликвидности: ${liquidity_usd:.0f} < ${MIN_LIQUIDITY_USD}")
                                    self.n_low_liquidity_usd = getattr(self, 'n_low_liquidity_usd', 0) + 1
                                    return 0, 0, 0, False
                                
                            # Обновляем спред для LUT
                            spread = spread_onchain / 100  # LUT ожидает спред в долях
                            
                            # Обновляем цены в словаре opportunity
                            opportunity['buy_price'] = buy_price_onchain
                            opportunity['sell_price'] = sell_price_onchain
                        else:
                            self.logger.warning("⚠️ Не удалось рассчитать onchain цену покупки")
# ========== КОНЕЦ НОВОГО КОДА ==========
                
            else:
                # Fallback на прямые RPC вызовы
                buy_reserves = await self.get_pair_reserves(buy_pair)
                sell_reserves = await self.get_pair_reserves(sell_pair)
                self.logger.debug(f"📡 Резервы через RPC получены")
            
            # Определяем базовый токен
            base_token = opportunity.get('base_token_address')
            base_decimals = self.get_token_decimals(base_token)
            
            # Парсим резервы для buy пары
            if buy_reserves:
                # Нужно определить какой токен в паре базовый
                # Для этого делаем дополнительный вызов token0/token1
                try:
                    pair_contract = self.w3.eth.contract(
                        address=Web3.to_checksum_address(buy_pair),
                        abi=json.loads('[{"constant":true,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"}]')
                    )
                    token0 = pair_contract.functions.token0().call()
                    token1 = pair_contract.functions.token1().call()
                    
                    self.logger.debug(f"Buy pair tokens: token0={token0}, token1={token1}")
                    self.logger.debug(f"Looking for base_token={base_token}")
                    
                    if token0.lower() == base_token.lower():
                        b1 = buy_reserves.get('reserve0', 0) / (10 ** base_decimals)
                        self.logger.debug(f"Buy: base is token0, b1={b1:.2f} {base_token_symbol}")
                    elif token1.lower() == base_token.lower():
                        b1 = buy_reserves.get('reserve1', 0) / (10 ** base_decimals)
                        self.logger.debug(f"Buy: base is token1, b1={b1:.2f} {base_token_symbol}")
                    else:
                        self.logger.warning(f"⚠️ Buy pair address mismatch: base token {base_token} not found in pair")
                        return 0, 0, 0
                except Exception as e:
                    self.logger.warning(f"⚠️ Cannot get buy pair tokens (non-V2?): {e}")
                    return 0, 0, 0
            else:
                b1 = 0
            
            # Парсим резервы для sell пары (аналогично)
            if sell_reserves:
                try:
                    pair_contract = self.w3.eth.contract(
                        address=Web3.to_checksum_address(sell_pair),
                        abi=json.loads('[{"constant":true,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"}]')
                    )
                    token0 = pair_contract.functions.token0().call()
                    token1 = pair_contract.functions.token1().call()
                    
                    self.logger.debug(f"Sell pair tokens: token0={token0}, token1={token1}")
                    self.logger.debug(f"Looking for base_token={base_token}")
                    
                    if token0.lower() == base_token.lower():
                        b2 = sell_reserves.get('reserve0', 0) / (10 ** base_decimals)
                        self.logger.debug(f"Sell: base is token0, b2={b2:.2f} {base_token_symbol}")
                    elif token1.lower() == base_token.lower():
                        b2 = sell_reserves.get('reserve1', 0) / (10 ** base_decimals)
                        self.logger.debug(f"Sell: base is token1, b2={b2:.2f} {base_token_symbol}")
                    else:
                        self.logger.warning(f"⚠️ Sell pair address mismatch: base token {base_token} not found in pair")
                        return 0, 0, 0
                except Exception as e:
                    self.logger.warning(f"⚠️ Cannot get sell pair tokens (non-V2?): {e}")
                    return 0, 0, 0
            else:
                b2 = 0
                    
            self.logger.debug(f"Онchain резервы (ANKR): b1={b1:.2f} {base_token_symbol}, b2={b2:.2f} {base_token_symbol}")
        else:
            # Если нет адресов пар - скипаем
            self.logger.warning("⚠️ Нет адресов пар для получения резервов - скипаю")
            self.skip_bad_reserves_onchain += 1
            return 0, 0, 0
                
        # Проверяем валидность резервов
        if b1 <= 0 or b2 <= 0:
            self.logger.warning(f"🚫 Недоступные резервы онchain: b1={b1}, b2={b2} - скипаю (не вызываю LUT)")
            self.skip_bad_reserves_onchain += 1
            # Сохраняем причину отсеивания
            if hasattr(opportunity, 'symbol'):
                self.rejection_reasons[opportunity['symbol']] = 'onchain_reserves_failed'
            return 0, 0, 0
                
        # Минимальные резервы для стабильности ($500 в базовом токене)
        MIN_RESERVE_USD = 500
        base_price_usd = self.BASE_TOKEN_USD_PRICES.get(
            opportunity.get('base_token_address', '').lower(), 1.0
        )
        min_reserve_units = MIN_RESERVE_USD / base_price_usd
                
        if b1 < min_reserve_units or b2 < min_reserve_units:
            self.logger.debug(f"🚫 Резервы < ${MIN_RESERVE_USD}: b1=${b1*base_price_usd:.0f}, b2=${b2*base_price_usd:.0f}")
            return 0, 0, 0
                
        # Получаем размер из LUT (в единицах BASE токена)
        optimal_size = size_from_lut(self.lut, spread, b1, b2)
                
        if optimal_size > 0:
            self.logger.debug(f"LUT вернула: size={optimal_size:.2f}, spread={spread:.4f}, b1={b1:.0f}, b2={b2:.0f}")
                
        # Оценка прибыли с учетом реальных комиссий
        buy_dex = opportunity.get('buy_dex', 'default')
        sell_dex = opportunity.get('sell_dex', 'default')
                
        # Расчет ожидаемой прибыли (упрощенный)
        total_fees = get_dex_fee(buy_dex) + get_dex_fee(sell_dex) + get_flash_loan_fee()
        net_spread = spread * 100 - total_fees  # в процентах
                
        if net_spread <= 0:
            expected_profit = 0
            price_impact = 0
        else:
            expected_profit = optimal_size * (net_spread / 100) * base_price_usd
            price_impact = 5.0  # Примерная оценка, будет уточнена при исполнении
                
        # Возвращаем также флаг переворота если он был
        flipped_flag = flipped if 'flipped' in locals() else False
        return optimal_size, expected_profit, price_impact, flipped_flag

        
    async def find_arbitrage_opportunities(self, all_pairs: List[Dict]) -> List[ArbitrageOpportunity]:
        """v6.3: Анализирует все пары и находит арбитражные возможности
        Теперь работает с WMATIC/WETH в дополнение к стейблкоинам
        """
        # Счётчики для расширенной статистики
        self.combos_scan = 0
        self.cands_scan = 0
        self.n_low_spread = 0
        self.n_low_liquidity_usd = 0  # НОВОЕ: счетчик недостаточной ликвидности
        self.n_low_spread_onchain = 0  # Уже есть
        self.all_spreads_before_v2 = []  # Спреды до V2-фильтра
        self.all_spreads_after_v2 = []   # Спреды после V2-фильтра
        self.rejection_reasons = {}  # Причины отсеивания по токенам
        self.n_nets0 = 0
        self.n_econ = 0
        self.n_bad_res = 0
        self.n_nonv2 = 0
        self.n_no_router = 0
        self.skip_bad_reserves_onchain = 0
        self.skip_non_v2_pair = 0
        self.skip_ankr_rate_limit = 0
        self.skip_address_mismatch = 0
    
        token_pairs = defaultdict(list)
        skipped_dex = set()
        
        # DEBUG: Счетчики для диагностики
        debug_stats = {
            'pairs_same_tokens': 0,
            'pairs_both_base': 0,  # v6.3: оба токена базовые
            'pairs_no_base': 0,    # v6.3: нет базового токена
            'pairs_no_router': 0,
            'pairs_low_liquidity': 0,
            'pairs_processed': 0
        }
        
        
        for pair in all_pairs:
            base = pair.get('baseToken', {})
            quote = pair.get('quoteToken', {})
            
            # ВАЖНО: Пропускаем пары где base и quote одинаковые
            if base.get('address', '').lower() == quote.get('address', '').lower():
                debug_stats['pairs_same_tokens'] += 1
                continue
            
            # Фильтруем по минимальному объему торгов
            volume_24h = float(pair.get('volume', {}).get('h24', 0))
            if volume_24h < MIN_VOLUME_24H:
                debug_stats['low_volume'] = debug_stats.get('low_volume', 0) + 1
                continue
            
            # НЕ пропускаем пары типа USDC-WMATIC или WETH-USDT - они нужны для арбитража!
            base_addr = base.get('address', '')
            quote_addr = quote.get('address', '')
            if self.is_stablecoin(base_addr) and self.is_stablecoin(quote_addr):
                debug_stats['pairs_both_base'] += 1  # Переименуем потом в pairs_stable_stable
                continue
            
            # Получаем DEX ID
            dex_id = pair.get('dexId', '').lower()
            pair_address = pair.get('pairAddress', '')
            
            # Определяем реальное имя DEX если это factory адрес
            if dex_id.startswith('0x') and len(dex_id) == 42:
                # Это factory адрес, ищем соответствующий DEX
                found_dex = None
                for dex_name, config in self.dex_config.items():
                    if config.get('factory', '').lower() == dex_id:
                        found_dex = dex_name
                        break
                
                if found_dex:
                    dex_id = found_dex
                else:
                    # Неизвестный factory
                    skipped_dex.add(dex_id)
                    continue
            
            # Теперь проверяем включен ли DEX
            if dex_id not in self.enabled_dexes:
                skipped_dex.add(dex_id)
                continue
            
            # Получаем конфигурацию DEX
            dex_info = self.dex_config.get(dex_id)
            if not dex_info:
                continue
                
            router_address = dex_info['router']
            
                
            # Пропускаем только явно несовместимые DEX
            if dex_id not in self.enabled_dexes:
                skipped_dex.add(dex_id)
                continue
            
            # Пропускаем если нет роутера
            if not router_address:
                debug_stats['pairs_no_router'] += 1
                skipped_dex.add(dex_id)
                continue
            
            # v6.4: Используем priceUsd для всех пар - универсальный подход
            # Определяем базовый токен для Flash Loan
            if self.is_base_token(quote.get('address')):
                trade_token = base
                base_token = quote
                base_token_symbol = self.get_base_token_symbol(quote.get('address'))
                is_inverted = True  # ИСПРАВЛЕНО: резервы нужно поменять местами
            elif self.is_base_token(base.get('address')):
                trade_token = quote
                base_token = base
                base_token_symbol = self.get_base_token_symbol(base.get('address'))
                is_inverted = False  # ИСПРАВЛЕНО: резервы в правильном порядке

            # Получаем правильную цену в базовом токене
            base_in_pair = pair.get('baseToken', {})
            quote_in_pair = pair.get('quoteToken', {})

            # Определяем правильную цену в зависимости от того, какой токен базовый
            if is_inverted:
                # Базовый токен - это quote в паре, используем priceQuote
                price_in_base = float(pair.get('priceQuote', 0))
            else:
                # Базовый токен - это base в паре, используем priceNative
                price_native = float(pair.get('priceNative', 0))
                # priceNative показывает сколько quote за 1 base, нам нужно инвертировать
                price_in_base = 1 / price_native if price_native > 0 else 0

            # Fallback на USD если нет прямой цены
            if price_in_base <= 0:
                price_usd = float(pair.get('priceUsd', 0))
                # Цены базовых токенов в USD
                base_prices_usd = {
                    '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174': 1.0,    # USDC.e
                    '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359': 1.0,    # USDC
                    '0xc2132D05D31c914a87C6611C10748AEb04B58e8F': 1.0,    # USDT
                    '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063': 1.0,    # DAI
                    '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270': 0.5,    # WMATIC (примерно)
                    '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619': 2300,   # WETH (примерно)
                }
                base_token_price_usd = base_prices_usd.get(base_token.get('address', '').lower(), 1.0)
                price_in_base = price_usd / base_token_price_usd
            
            
            if price_in_base <= 0:
                continue
            
            # Фильтруем только известные базовые токены (защита от fake/scam токенов)
            # Используем BASE_TOKENS из конфигурации
            VERIFIED_BASE_TOKENS = {addr.lower() for addr in BASE_TOKENS.get(CHAIN, {}).keys()}
            
            if base_token.get('address', '').lower() not in VERIFIED_BASE_TOKENS:
                self.logger.debug(f"Пропускаем неверифицированный токен: {base_token.get('symbol')} ({base_token.get('address')})")
                continue
                
            key = f"{trade_token.get('address')}|{base_token.get('address')}"
            
            liquidity = float(pair.get('liquidity', {}).get('usd', 0))
            
            
            if liquidity < MIN_LIQUIDITY_USD:
                debug_stats['pairs_low_liquidity'] += 1
                continue
                
                
            if liquidity >= MIN_LIQUIDITY_USD:
                debug_stats['pairs_processed'] += 1
                token_pairs[key].append({
                    'symbol': trade_token.get('symbol'),
                    'trade_token_address': trade_token.get('address'),
                    'base_token_address': base_token.get('address'),
                    'base_token_symbol': base_token_symbol,  # v6.3: добавляем символ
                    'dex': dex_id if dex_id else f"unknown_{pair_address[-6:]}",
                    'pair_address': pair_address,
                    'price': price_in_base,  # Теперь это цена в базовом токене, а не в USD
                    'liquidity': liquidity,
                    # DEXScreener уже дает резервы в токенах!
                    'base_reserve': float(pair.get('liquidity', {}).get('base', 0)) if not is_inverted else float(pair.get('liquidity', {}).get('quote', 0)),
                    'quote_reserve': float(pair.get('liquidity', {}).get('quote', 0)) if not is_inverted else float(pair.get('liquidity', {}).get('base', 0)),
                    'volume_24h': float(pair.get('volume', {}).get('h24', 0)),
                    'is_inverted': is_inverted,
                    'router': router_address  # Сохраняем найденный роутер
                })
        
        
        # DEBUG: Выводим статистику фильтрации
        self.logger.info(f"📊 Статистика фильтрации пар:")
        self.logger.info(f"   Пары с одинаковыми токенами: {debug_stats['pairs_same_tokens']}")
        self.logger.info(f"   Пары база-база (USDC-USDT и т.д.): {debug_stats['pairs_both_base']}")
        self.logger.info(f"   Пары без базового токена: {debug_stats['pairs_no_base']}")
        self.logger.info(f"   Пары с низкой ликвидностью: {debug_stats['pairs_low_liquidity']}")
        self.logger.info(f"   Пары с низким объемом (<${MIN_VOLUME_24H}): {debug_stats.get('low_volume', 0)}")
        self.logger.info(f"   Отсеяно: нет роутера (по белому списку): {debug_stats['pairs_no_router']}")
        self.logger.info(f"   Пары обработано успешно: {debug_stats['pairs_processed']}")
        
        self.logger.info(f"Сгруппировано уникальных пар токенов: {len(token_pairs)}")
        
        opportunities = []
        
        # DEBUG: Счетчики для анализа
        debug_opportunities = {
            'tokens_with_single_dex': 0,
            'low_spread_count': 0,
            'no_profit_count': 0,
            'total_combinations': 0,
            'ds_spreads_all': []  # НОВОЕ: все DS-спреды без фильтрации
        }
        
        for token_key, dex_list in token_pairs.items():
            if len(dex_list) < 2:
                debug_opportunities['tokens_with_single_dex'] += 1
                continue
                
            valid_dexes = [d for d in dex_list if d['price'] > 0]
            
            if len(valid_dexes) < 2:
                continue
            
            # v6.3: Берем символ базового токена из первого DEX
            base_token_symbol = valid_dexes[0].get('base_token_symbol', 'USDC')
            
            # Находим все комбинации DEX для арбитража
            for i, buy_dex in enumerate(valid_dexes):
                for sell_dex in valid_dexes[i+1:]:
                    debug_opportunities['total_combinations'] += 1
                    
                    # Проверяем что DEX разные
                    if buy_dex['dex'] == sell_dex['dex']:
                        continue

                    
                    # Определяем направление арбитража
                    if buy_dex['price'] < sell_dex['price']:
                        min_price_dex = buy_dex
                        max_price_dex = sell_dex
                    else:
                        min_price_dex = sell_dex
                        max_price_dex = buy_dex
                    
                    # ИСПРАВЛЕНИЕ: Вычисляем спред ЗДЕСЬ
                    spread = (max_price_dex['price'] - min_price_dex['price']) / min_price_dex['price'] * 100
                    self.all_spreads_scan.append(spread)  # Собираем ВСЕ спреды для статистики
                    
                    # Проверяем что у обоих DEX есть роутеры
                    if not min_price_dex.get('router') or not max_price_dex.get('router'):
                        continue
                    
                    
                    # ДОБАВИТЬ детальную проверку для значимых спредов
                    if spread > 0.5 and min_price_dex.get('router') == max_price_dex.get('router'):
                        self.logger.warning(
                            f"🔍 АНАЛИЗ ПОДОЗРИТЕЛЬНОЙ ПАРЫ:\n"
                            f"   Токен: {min_price_dex['symbol']}\n"
                            f"   DEX1: {min_price_dex['dex']} | Пара: {min_price_dex['pair_address']}\n"
                            f"   DEX2: {max_price_dex['dex']} | Пара: {max_price_dex['pair_address']}\n"
                            f"   Один роутер: {min_price_dex.get('router')}\n"
                            f"   Пары {'РАЗНЫЕ' if min_price_dex['pair_address'] != max_price_dex['pair_address'] else 'ОДИНАКОВЫЕ'}\n"
                            f"   Цены: ${min_price_dex['price']:.6f} → ${max_price_dex['price']:.6f}\n"
                            f"   Ликвидность: ${min_price_dex['liquidity']:,.0f} → ${max_price_dex['liquidity']:,.0f}"
                        )
    
                    # НОВОЕ: Детальное логирование для значимых спредов
                    if spread > 0.5:  # Для спредов больше 0.5%
                        self.logger.info(
                            f"📍 Потенциал: {min_price_dex['symbol']} | Спред {spread:.2f}% | "
                            f"{min_price_dex['dex']}→{max_price_dex['dex']}\n"
                            f"   Роутеры: {min_price_dex.get('router', 'none')[-6:]} → "
                            f"{max_price_dex.get('router', 'none')[-6:]}\n"
                            f"   Пары: {min_price_dex['pair_address'][-6:]} → "
                            f"{max_price_dex['pair_address'][-6:]}\n"
                            f"   Одинаковый роутер: {'❌ ДА' if min_price_dex.get('router') == max_price_dex.get('router') else '✅ НЕТ'}"
                        )
                    
                    # ===== РАСШИРЕННАЯ ОТЛАДКА АРБИТРАЖА =====
                    # Отладка отключена для ускорения 
                    
                    # РЕВОЛЮЦИОННЫЙ ПОДХОД: проверяем ВСЕ возможности!
                    # Фильтр спреда
                    self.combos_scan += 1  # Считаем все проверенные комбинации
                    
                    # Сохраняем ВСЕ спреды для статистики
                    if not hasattr(self, 'all_spreads_scan'):
                        self.all_spreads_scan = []
                    self.all_spreads_scan.append(spread)

                    # УДАЛЕНО: Ранний фильтр по DS-спреду - теперь фильтруем после onchain
                    # if spread < MIN_SPREAD_PERCENT:
                    #     debug_opportunities['low_spread_count'] += 1
                    #     self.n_low_spread += 1
                    #     continue

                    # Сохраняем спред для статистики, но НЕ фильтруем
                    debug_opportunities['ds_spreads_all'].append(spread)  # Новый счетчик
                        
                    self.cands_scan += 1  # Прошли фильтр по спреду
    
                    # Проверяем что обе DEX - это V2 (для LUT)
                    buy_dex_type = self.dex_config.get(min_price_dex['dex'], {}).get('type', 'UNKNOWN')
                    sell_dex_type = self.dex_config.get(max_price_dex['dex'], {}).get('type', 'UNKNOWN')
                    
                    # Определяем причину отсеивания
                    rejection_reason = 'ok'
                    if buy_dex_type != 'V2' or sell_dex_type != 'V2':
                        rejection_reason = 'non-V2'
                    elif min_price_dex['symbol'] in self.rejection_reasons:
                        rejection_reason = self.rejection_reasons[min_price_dex['symbol']]
                    
                    # Сохраняем спред ДО V2-фильтра
                    self.all_spreads_before_v2.append((spread, min_price_dex['symbol'], min_price_dex['dex'], max_price_dex['dex'], rejection_reason))
                    
                    if buy_dex_type != 'V2' or sell_dex_type != 'V2':
                        # Пропускаем не-V2 пары (V3/Algebra не поддерживаются LUT)
                        self.n_nonv2 += 1
                        continue
                    
                    # Сохраняем спред ПОСЛЕ V2-фильтра
                    self.all_spreads_after_v2.append((spread, min_price_dex['symbol'], min_price_dex['dex'], max_price_dex['dex']))
                    
                    # Даже малый спред может дать прибыль на большом объеме!
                        
                    # ОТЛАДКА резервов перед передачей в LUT
                    if spread > 0.5:  # Для спредов больше 0.5%
                        self.logger.debug(f"Резервы для {min_price_dex['symbol']}: buy_base={min_price_dex.get('base_reserve', 0):.0f}, sell_base={max_price_dex.get('base_reserve', 0):.0f}")
                    
                    # Проверяем был ли переворот при расчете onchain спреда
                    if 'flipped' in locals() and flipped:
                        # Меняем местами buy и sell DEX
                        opportunity_data = {
                            'spread': spread,
                            'buy_price': sell_price_onchain if 'sell_price_onchain' in locals() else max_price_dex['price'],
                            'sell_price': buy_price_onchain if 'buy_price_onchain' in locals() else min_price_dex['price'],
                            'buy_liquidity': max_price_dex['liquidity'],
                            'sell_liquidity': min_price_dex['liquidity'],
                            'buy_dex': max_price_dex['dex'],
                            'sell_dex': min_price_dex['dex'],
                            'buy_pair': max_price_dex['pair_address'],
                            'sell_pair': min_price_dex['pair_address'],
                            'base_token_address': min_price_dex['base_token_address'],
                            'trade_token_address': min_price_dex['trade_token_address'],
                            'buy_base_reserve': max_price_dex.get('base_reserve', 0),
                            'buy_quote_reserve': max_price_dex.get('quote_reserve', 0),
                            'sell_base_reserve': min_price_dex.get('base_reserve', 0),
                            'sell_quote_reserve': min_price_dex.get('quote_reserve', 0),
                            'buy_router': max_price_dex.get('router'),
                            'sell_router': min_price_dex.get('router')
                        }
                        self.logger.info(f"🔄 Применен переворот: buy={opportunity_data['buy_dex']}, sell={opportunity_data['sell_dex']}")
                    else:
                        opportunity_data = {
                            'spread': spread,
                            'buy_price': buy_price_onchain if 'buy_price_onchain' in locals() else min_price_dex['price'],
                            'sell_price': sell_price_onchain if 'sell_price_onchain' in locals() else max_price_dex['price'],
                            'buy_liquidity': min_price_dex['liquidity'],
                            'sell_liquidity': max_price_dex['liquidity'],
                            'buy_dex': min_price_dex['dex'],
                            'sell_dex': max_price_dex['dex'],
                            'buy_pair': min_price_dex['pair_address'],
                            'sell_pair': max_price_dex['pair_address'],
                            'base_token_address': min_price_dex['base_token_address'],
                            'trade_token_address': min_price_dex['trade_token_address'],
                            'buy_base_reserve': min_price_dex.get('base_reserve', 0),
                            'buy_quote_reserve': min_price_dex.get('quote_reserve', 0),
                            'sell_base_reserve': max_price_dex.get('base_reserve', 0),
                            'sell_quote_reserve': max_price_dex.get('quote_reserve', 0),
                            'buy_router': min_price_dex.get('router'),
                            'sell_router': max_price_dex.get('router')
                        }
                    
                    # v7.3: Передаем символ базового токена и адреса пар для получения резервов
                    opportunity_data['buy_pair'] = min_price_dex['pair_address']
                    opportunity_data['sell_pair'] = max_price_dex['pair_address']
                    opportunity_data['base_token_address'] = min_price_dex['base_token_address']
                    opportunity_data['trade_token_address'] = min_price_dex['trade_token_address']
                    
                    result = await self.find_optimal_trade_size(
                        opportunity_data, base_token_symbol
                    )
                    
                    # Распаковываем результат с учетом возможного флага переворота
                    if len(result) == 4:
                        optimal_size, expected_profit, price_impact, flipped = result
                    else:
                        optimal_size, expected_profit, price_impact = result
                        flipped = False
                    
                    # Диагностика переворота направления
                    if 'flipped' in locals() and flipped:
                        self.logger.info(f"🔍 ПЕРЕВОРОТ: ds_buy={min_price_dex['dex']}, ds_sell={max_price_dex['dex']} → теперь buy={max_price_dex['dex']}, sell={min_price_dex['dex']}")
                        
                    # Если find_optimal_trade_size вернула 0 из-за плохих резервов - скипаем
                    if optimal_size == 0 and expected_profit == 0 and price_impact == 0:
                        # Это означает что резервы не получены или некорректны
                        continue
                    
                    # Статистика по net_spread (если size = 0, значит net_spread <= 0)
                    if optimal_size <= 0:
                        self.n_nets0 += 1

                    # Если формула говорит 0 - значит возможности нет
                    if optimal_size <= 0:
                        debug_opportunities['no_profit_count'] += 1
                        self.logger.debug(f"   Размер = 0 для {min_price_dex['symbol']}: net_spread <= 0")
                        continue  # Пропускаем эту возможность
                    
                    if expected_profit < MIN_PROFIT_USD:
                        debug_opportunities['no_profit_count'] += 1
                        # DEBUG: Выводим близкие к прибыли
                        if expected_profit > MIN_PROFIT_USD * 0.5 and MIN_PROFIT_USD > 0:
                            self.logger.debug(f"   Близкая прибыль ${expected_profit:.3f} для {min_price_dex['symbol']}")
                        # ВСЕГДА пропускаем непрофитные, даже в режиме отладки
                        continue  # Убираем условие на MIN_PROFIT_USD
                        
                    # Проверяем что обе пары существуют и читаются
                    if min_price_dex['pair_address'] and max_price_dex['pair_address']:
                        # Быстрая проверка что пары валидные
                        try:
                            # Проверяем bytecode обеих пар
                            min_pair_code = self.w3.eth.get_code(Web3.to_checksum_address(min_price_dex['pair_address']))
                            max_pair_code = self.w3.eth.get_code(Web3.to_checksum_address(max_price_dex['pair_address']))
                            
                            if len(min_pair_code) <= 2 or len(max_pair_code) <= 2:
                                self.logger.debug(f"Пропускаем невалидные пары для {min_price_dex['symbol']}")
                                continue
                        except:
                            continue    
                        
                    # НОВОЕ: Проверяем реальную профитность до добавления
                    if optimal_size <= 0 or expected_profit < MIN_PROFIT_USD:
                        continue  # Пропускаем непрофитные
                    
                    opportunities.append(ArbitrageOpportunity(
                            token_symbol=min_price_dex['symbol'],
                            token_address=min_price_dex['trade_token_address'],
                            quote_token_address=min_price_dex['base_token_address'],
                            quote_token_symbol=base_token_symbol,  # v6.3: добавляем символ базового токена
                            buy_dex=min_price_dex['dex'],
                            sell_dex=max_price_dex['dex'],
                            buy_price=min_price_dex['price'],
                            sell_price=max_price_dex['price'],
                            spread_percent=spread,
                            optimal_trade_size=optimal_size,
                            expected_profit=expected_profit,
                            buy_liquidity=min_price_dex['liquidity'],
                            sell_liquidity=max_price_dex['liquidity'],
                            price_impact_percent=price_impact,
                            buy_dex_pair=min_price_dex['pair_address'],
                            sell_dex_pair=max_price_dex['pair_address'],
                            buy_router=self.dex_config.get(min_price_dex['dex'], {}).get('router') or min_price_dex['router'],
                            sell_router=self.dex_config.get(max_price_dex['dex'], {}).get('router') or max_price_dex['router']
                        ))
        
        # Сортируем: сначала с разными роутерами, потом по прибыли
        opportunities.sort(
            key=lambda x: (
                x.expected_profit
            ), 
            reverse=True
)
        
        # DEBUG: Выводим статистику по возможностям
        self.logger.info(f"📊 Статистика возможностей:")
        self.logger.info(f"   Токены с одним DEX: {debug_opportunities['tokens_with_single_dex']}")
        self.logger.info(f"   Всего комбинаций проверено: {debug_opportunities['total_combinations']}")
        self.logger.info(f"   Отсеяно: спред (<{MIN_SPREAD_PERCENT}%): {debug_opportunities['low_spread_count']}")
        self.logger.info(f"   Нет прибыли (<${MIN_PROFIT_USD}): {debug_opportunities['no_profit_count']}")
        # Дополнительная статистика для отладки
        if MIN_PROFIT_USD < 0:  # Если в режиме отладки
            self.logger.info(f"📊 РЕЖИМ ОТЛАДКИ: Показаны даже убыточные возможности")
            self.logger.info(f"   Проверьте логи выше для детального анализа комиссий")
        
        
    
        # НЕ выводим пока тут "профитные", так как проверка происходит ПОСЛЕ
        # real_profitable определяются позже, после проверки пред-чека
        
        return opportunities
        
    async def prepare_universal_params(self, opportunity: ArbitrageOpportunity) -> Tuple[str, int, bytes]:
        # КРИТИЧЕСКАЯ ПРОВЕРКА: размер должен быть > 0
        if opportunity.optimal_trade_size <= 0:
            self.logger.error(f"ОШИБКА: Попытка подготовить параметры для сделки с size=0!")
            self.logger.error(f"  Токен: {opportunity.token_symbol}")
            self.logger.error(f"  Спред: {opportunity.spread_percent:.3f}%")
            raise ValueError("Trade size is 0")
        
        # v7.0: Умный выбор токена и маршрута для Flash Loan
        flash_loan_asset, flash_loan_symbol, swap_routes = await self.choose_flash_loan_asset(opportunity)

        # Проверка поддержки
        if flash_loan_asset is None:
            self.logger.error("Не найден подходящий токен для Flash Loan")
            raise ValueError("No suitable flash loan token")

        # Определяем направление свопов из маршрута
        first_swap = swap_routes[0]
        second_swap = swap_routes[1]

        # Используем маршрут из стратегии
        # Должны быть полные адреса с Web3.to_checksum_address
        swap_token_in_1 = Web3.to_checksum_address(first_swap.token_in)
        swap_token_out_1 = Web3.to_checksum_address(first_swap.token_out)
        swap_token_in_2 = Web3.to_checksum_address(second_swap.token_in)
        swap_token_out_2 = Web3.to_checksum_address(second_swap.token_out)

        first_dex = first_swap.dex
        first_router = first_swap.router
        second_dex = second_swap.dex
        second_router = second_swap.router
        
        # ⚠️ Форсим порядок buy → sell
        if first_dex != opportunity.buy_dex:
            # меняем местами first/second
            first_swap, second_swap = second_swap, first_swap

            swap_token_in_1,  swap_token_out_1  = first_swap.token_in,  first_swap.token_out
            swap_token_in_2,  swap_token_out_2  = second_swap.token_in, second_swap.token_out
            first_dex, first_router             = first_swap.dex,       first_swap.router
            second_dex, second_router           = second_swap.dex,      second_swap.router

        self.logger.debug(f"DEBUG: Using first_dex={first_dex}, first_router={first_router}")
        self.logger.debug(f"DEBUG: Using second_dex={second_dex}, second_router={second_router}")


        self.logger.info(f"📍 Маршрут арбитража:")
        self.logger.info(f"   Flash Loan: {flash_loan_symbol}")
        self.logger.info(f"   Своп 1: {first_dex} ({swap_token_in_1[-6:]} → {swap_token_out_1[-6:]})")
        self.logger.info(f"   Своп 2: {second_dex} ({swap_token_in_2[-6:]} → {swap_token_out_2[-6:]})")
        
        # Получаем decimals для базового токена
        base_decimals = self.get_token_decimals(flash_loan_asset)
        
        # Размер займа с правильными decimals
        loan_amount = int(opportunity.optimal_trade_size * (10 ** base_decimals))
        
        # Минимальный размер loan для стабильности  
        MIN_FLASH_STEP_BASE = 1e-6  # например, 0.000001 WETH (подбери под свою базу)
        MIN_FLASH_LOAN = int(MIN_FLASH_STEP_BASE * (10 ** base_decimals))
        
        if loan_amount < MIN_FLASH_LOAN:
            # Если LUT вернул size=0, значит экономически невыгодно - НЕ корректируем, а пропускаем
            if opportunity.optimal_trade_size <= 0:
                self.logger.warning(f"🚫 LUT вернул size=0, сделка экономически невыгодна")
                raise ValueError("Trade not profitable according to LUT")
            # Иначе корректируем до минимума
            self.logger.warning(f"⚠️ Размер {loan_amount/(10**base_decimals):.8f} меньше минимума {MIN_FLASH_STEP_BASE}, корректирую")
            loan_amount = MIN_FLASH_LOAN
        
        # Определяем типы DEX
        buy_dex_type = self.dex_config.get(first_dex, {}).get('type', 'V2')
        sell_dex_type = self.dex_config.get(second_dex, {}).get('type', 'V2')
        
        # Проверяем что роутеры есть
        if not first_router or not second_router:
            self.logger.error(f"❌ Отсутствуют роутеры!")
            self.logger.error(f"   First router: {first_router}")
            self.logger.error(f"   Second router: {second_router}")
            raise ValueError("Missing router addresses")
        # ДОБАВИТЬ ОТЛАДКУ:
        self.logger.debug(f"DEBUG: Opportunity buy_dex={opportunity.buy_dex}, buy_router={opportunity.buy_router}")
        self.logger.debug(f"DEBUG: Opportunity sell_dex={opportunity.sell_dex}, sell_router={opportunity.sell_router}")
        self.logger.debug(f"DEBUG: Using first_dex={first_dex}, first_router={first_router}")
        self.logger.debug(f"DEBUG: Using second_dex={second_dex}, second_router={second_router}") 

        
        # Deadline для свопов
        deadline = self.w3.eth.get_block('latest')['timestamp'] + 300  # +5 минут
        
        # Минимальный выход с правильными decimals
        flash_loan_fee_amount = int(loan_amount * get_flash_loan_fee() / 100)
        # Временно устанавливаем в 0 - будет пересчитан после расчета реального выхода
        min_amount_out = 0  # Будет обновлен позже
        
        # Массивы для универсального контракта
        swap_data_list = []
        routers = []
        tokens = []
        
        # СВОП 1: Flash Loan токен -> Другой токен
        self.logger.info(f"Подготовка свопа 1: {Web3.to_checksum_address(swap_token_in_1)[-6:]} -> {Web3.to_checksum_address(swap_token_out_1)[-6:]} на {first_dex} (тип: {buy_dex_type})")
        self.logger.info(f"  Используем роутер: {first_router}")
        
        if buy_dex_type == 'V2' or buy_dex_type == 'UNKNOWN':
            # V2 swap или неизвестный - пробуем V2
            buy_calldata = CalldataBuilder.build_v2_swap(
                router=first_router,
                amount_in=loan_amount,
                amount_out_min=0,
                path=[swap_token_in_1, swap_token_out_1],
                to=self.contract.address,
                deadline=deadline
            )
        elif buy_dex_type == 'V3':
            # V3 swap
            buy_calldata = CalldataBuilder.build_v3_swap(
                router=first_router,
                token_in=swap_token_in_1,
                token_out=swap_token_out_1,
                fee=3000,
                recipient=self.contract.address,
                deadline=deadline,
                amount_in=loan_amount,
                amount_out_min=0
            )
        elif buy_dex_type == 'ALGEBRA':
            # Algebra swap
            buy_calldata = CalldataBuilder.build_algebra_swap(
                router=first_router,
                token_in=swap_token_in_1,
                token_out=swap_token_out_1,
                recipient=self.contract.address,
                deadline=deadline,
                amount_in=loan_amount,
                amount_out_min=0
            )
        else:
            # По умолчанию V2
            buy_calldata = CalldataBuilder.build_v2_swap(
                router=first_router,
                amount_in=loan_amount,
                amount_out_min=0,
                path=[swap_token_in_1, swap_token_out_1],
                to=self.contract.address,
                deadline=deadline
            )
        
        swap_data_list.append(buy_calldata)
        routers.append(Web3.to_checksum_address(first_router))
        tokens.append(Web3.to_checksum_address(swap_token_in_1))
        
        # СВОП 2: Другой токен -> Flash Loan токен
        self.logger.info(f"Подготовка свопа 2: {Web3.to_checksum_address(swap_token_in_2)[-6:]} → {Web3.to_checksum_address(swap_token_out_2)[-6:]} на {second_dex} (тип: {sell_dex_type})")
        self.logger.info(f"  Используем роутер: {second_router}")
        
        # Получаем decimals
        intermediate_decimals = self.get_token_decimals(swap_token_out_1)  # TRADE
        # base_decimals уже получен выше
        
        # --- ВАЖНО: считаем по РЕЗЕРВАМ V2, без USD-реконструкций ---
        # 1) Выход ПЕРВОГО свопа по buy-паре (BASE -> TRADE)
        buy_res = await self.get_v2_reserves_by_router(
            router_addr=first_router,
            tokenA=swap_token_in_1,
            tokenB=swap_token_out_1,
            dex_name=first_dex
        )
        if not buy_res:
            self.logger.error("Не удалось получить резервы buy-пары (router→factory.getPair) — пропускаю возможность")
            return None
        
        base_addr  = Web3.to_checksum_address(swap_token_in_1)   # BASE (flash loan asset)
        trade_addr = Web3.to_checksum_address(swap_token_out_1)  # TRADE
        if buy_res['token0'].lower() == base_addr.lower():
            r_in_base_raw, r_out_trade_raw = buy_res['reserve0'], buy_res['reserve1']
        else:
            r_in_base_raw, r_out_trade_raw = buy_res['reserve1'], buy_res['reserve0']
        
        r_in_base_dec   = r_in_base_raw   / (10 ** base_decimals)
        r_out_trade_dec = r_out_trade_raw / (10 ** intermediate_decimals)
        amount_in_base_dec = loan_amount / (10 ** base_decimals)
        
        buy_fee_pct = get_dex_fee(first_dex)  # в процентах, напр. 0.3
        trade_out_dec, buy_impact = self.calculate_price_impact_amm(
            amount_in=amount_in_base_dec,
            reserve_in=r_in_base_dec,
            reserve_out=r_out_trade_dec,
            fee_percent=buy_fee_pct
        )
        
        # 2) Минимальный выход ВТОРОГО свопа по sell-паре (TRADE -> BASE)
        sell_res = await self.get_v2_reserves_by_router(
            router_addr=second_router,
            tokenA=swap_token_in_2,
            tokenB=swap_token_out_2,
            dex_name=second_dex
        )
        if not sell_res:
            self.logger.error("Не удалось получить резервы sell-пары (router→factory.getPair) — пропускаю возможность")
            return None
        
        if sell_res['token0'].lower() == trade_addr.lower():
            r_in_trade2_raw, r_out_base2_raw = sell_res['reserve0'], sell_res['reserve1']
        else:
            r_in_trade2_raw, r_out_base2_raw = sell_res['reserve1'], sell_res['reserve0']
        
        r_in_trade2_dec = r_in_trade2_raw / (10 ** intermediate_decimals)
        r_out_base2_dec = r_out_base2_raw / (10 ** base_decimals)
        
        sell_fee_pct = get_dex_fee(second_dex)  # в процентах
        base_out_dec, sell_impact = self.calculate_price_impact_amm(
            amount_in=trade_out_dec,
            reserve_in=r_in_trade2_dec,
            reserve_out=r_out_base2_dec,
            fee_percent=sell_fee_pct
        )
        
        # Консервативный минимум с буфером 1%
        second_swap_min_out = int(base_out_dec * (10 ** base_decimals) * 0.99)
        
        # Теперь обновляем min_amount_out на основе реального ожидаемого выхода
        expected_out_wei = int(base_out_dec * (10 ** base_decimals))
        required_repay = loan_amount + flash_loan_fee_amount
        
        # Проверяем экономический смысл
        if expected_out_wei <= required_repay:
            self.logger.warning(f"❌ Убыточная сделка: ожидаемый выход {base_out_dec:.4f} <= необходимо вернуть {required_repay/(10**base_decimals):.4f}")
            return None
        
        # Устанавливаем min_amount_out с небольшим slippage (0.5%)
        slippage = 0.005  # 0.5% для безопасности
        min_amount_out = int(expected_out_wei * (1 - slippage))
        
        # Но не меньше чем loan + fee (иначе транзакция провалится)
        if min_amount_out < required_repay:
            min_amount_out = required_repay + 1  # +1 wei чтобы точно хватило
        
        self.logger.info(f"  Ожидаемый выход: {base_out_dec:.4f} {flash_loan_symbol}")
        self.logger.info(f"  Минимальный выход (со slippage): {min_amount_out/(10**base_decimals):.4f} {flash_loan_symbol}")
        
        # Для V2 используем sentinel amount_in=MAX_UINT, чтобы контракт подставил фактический баланс TRADE
        estimated_token_amount = int(trade_out_dec * (10 ** intermediate_decimals))  # для V3/Algebra
        
        self.logger.debug("DEBUG своп-2 (по резервам): "
                          f"buy_impact≈{buy_impact:.4f}% sell_impact≈{sell_impact:.4f}% | "
                          f"trade_out_dec={trade_out_dec:.6f} => est_token_amount={estimated_token_amount} | "
                          f"base_out_dec_min={base_out_dec:.6f} => second_swap_min_out={second_swap_min_out}")
        
        MAX_UINT = (1 << 256) - 1
        
        if sell_dex_type == 'V2' or sell_dex_type == 'UNKNOWN':
            sell_calldata = CalldataBuilder.build_v2_swap(
                router=second_router,
                amount_in=MAX_UINT,
                amount_out_min=second_swap_min_out,
                path=[swap_token_in_2, swap_token_out_2],
                to=self.contract.address,
                deadline=deadline
            )
        elif sell_dex_type == 'V3':
            sell_calldata = CalldataBuilder.build_v3_swap(
                router=second_router,
                token_in=swap_token_in_2,
                token_out=swap_token_out_2,
                fee=3000,
                recipient=self.contract.address,
                deadline=deadline,
                amount_in=estimated_token_amount,
                amount_out_min=second_swap_min_out
            )
        elif sell_dex_type == 'ALGEBRA':
            sell_calldata = CalldataBuilder.build_algebra_swap(
                router=second_router,
                token_in=swap_token_in_2,
                token_out=swap_token_out_2,
                recipient=self.contract.address,
                deadline=deadline,
                amount_in=estimated_token_amount,
                amount_out_min=second_swap_min_out
            )
        else:
            # по умолчанию V2
            sell_calldata = CalldataBuilder.build_v2_swap(
                router=second_router,
                amount_in=MAX_UINT,
                amount_out_min=second_swap_min_out,
                path=[swap_token_in_2, swap_token_out_2],
                to=self.contract.address,
                deadline=deadline
            )
        
        swap_data_list.append(sell_calldata)
        routers.append(Web3.to_checksum_address(second_router))
        tokens.append(Web3.to_checksum_address(swap_token_in_2))
        
        
        # Кодируем параметры
        encoded_params = encode(
            ['bytes[]', 'address[]', 'address[]', 'uint256'],
            [swap_data_list, routers, tokens, min_amount_out]
        )
        
        self.logger.info(f"Параметры готовы:")
        self.logger.info(f"  Flash loan: {loan_amount/(10**base_decimals):.2f} {flash_loan_symbol}")
        self.logger.info(f"  Путь: {Web3.to_checksum_address(swap_token_in_1)[-6:]} -> {Web3.to_checksum_address(swap_token_out_1)[-6:]} -> {Web3.to_checksum_address(swap_token_out_2)[-6:]}")
        self.logger.info(f"  Минимальный общий выход: {min_amount_out/(10**base_decimals):.2f}")
        self.logger.info(f"  Роутеры: {[r[-6:] for r in routers]}")
        
        return flash_loan_asset, loan_amount, encoded_params
        
    async def execute_arbitrage(self, opportunity: ArbitrageOpportunity) -> bool:
        """Исполняет арбитражную сделку через универсальный смарт-контракт"""
        try:
            # Форматирование размера в USD
            size_usd = opportunity.optimal_trade_size
            if opportunity.quote_token_symbol in ["WMATIC", "WETH", "WBTC"]:
                token_prices = {"WMATIC": 0.5, "WETH": 2300, "WBTC": 58000}
                size_usd = opportunity.optimal_trade_size * token_prices.get(opportunity.quote_token_symbol, 1)
            
            self.logger.info(f"Исполняю арбитраж {opportunity.token_symbol}")
            self.logger.info(f"   Размер: ${size_usd:,.0f} ({opportunity.optimal_trade_size:.4f} {opportunity.quote_token_symbol})")
            self.logger.info(f"   Ожидаемая прибыль: ${opportunity.expected_profit:,.2f}")
            self.logger.info(f"   DEX: {opportunity.buy_dex} -> {opportunity.sell_dex}")
            
            # Проверка симуляции
            if SIMULATION_MODE:
                self.logger.info("РЕЖИМ СИМУЛЯЦИИ - сделка не исполнена")
                self.trades_executed += 1
                self.total_profit += opportunity.expected_profit
                return True
            
            # v7.0: Проверка будет внутри prepare_universal_params
            # Убираем дублирующую проверку
            
            # v7.0: Быстрая проверка DS-пар отключена — резолв адресов выполняется внутри prepare_universal_params()
            self.logger.debug("Пропускаю быструю проверку DS-пар; резолв пар выполняется в prepare_universal_params()")


            # Подготовка параметров
            params = await self.prepare_universal_params(opportunity)
            if not params:
                self.logger.debug("Возможность отфильтрована пред-чеком — пропускаю без ошибки.")
                return None
            flash_loan_asset, loan_amount, encoded_params = params
            
            # Получаем nonce и gas price
            nonce = self.w3.eth.get_transaction_count(self.account.address)
            gas_price = self.w3.eth.gas_price
            
            # Готовим транзакцию
            transaction = self.contract.functions.executeArbitrage(
                flash_loan_asset,
                loan_amount,
                encoded_params
            ).build_transaction({
                'from': self.account.address,
                'nonce': nonce,
                'gas': 1200000,
                'gasPrice': int(gas_price * 1.2),
                'chainId': self.get_chain_id()
            })
            
           # Симуляция с детальной диагностикой
            try:
                # Теперь нормальная симуляция
                tx_for_call = {
                    'to': transaction['to'],
                    'from': self.account.address,
                    'data': transaction['data'],
                    'value': transaction.get('value', 0),
                    'gas': transaction.get('gas', 1200000),
                }
                if 'maxFeePerGas' in transaction and 'maxPriorityFeePerGas' in transaction:
                    tx_for_call['maxFeePerGas'] = transaction['maxFeePerGas']
                    tx_for_call['maxPriorityFeePerGas'] = transaction['maxPriorityFeePerGas']
                else:
                    tx_for_call['gasPrice'] = transaction.get('gasPrice', int(self.w3.eth.gas_price * 1.2))

                # 1) Сначала eth_call — убедиться, что контракт не ревертит
                self.w3.eth.call(tx_for_call, block_identifier='latest')

                # 2) Затем estimate_gas — реальная оценка
                estimated_gas = self.w3.eth.estimate_gas(tx_for_call)
                self.logger.info(f"✅ Симуляция успешна! Газ: {estimated_gas}")

                
                # ДОБАВИТЬ ОТЛАДКУ:
                try:
                    self.logger.info("Подписываю транзакцию...")
                    signed_txn = self.w3.eth.account.sign_transaction(transaction, PRIVATE_KEY)
                    
                    self.logger.info("Отправляю транзакцию...")
                    tx_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
                    
                    self.logger.info(f"Транзакция отправлена: {tx_hash.hex()}")
                except Exception as send_error:
                    self.logger.error(f"Ошибка при отправке: {send_error}")
                    return False

                # Ждем подтверждения
                self.logger.info("Жду подтверждения...")
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            except Exception as call_error:
                self.logger.error(f"❌ Отклонено на симуляции (router revert): {call_error}")

                # Попытка декодировать revert-сообщение
                try:
                    if hasattr(call_error, 'args') and len(call_error.args) > 0:
                        # Web3 часто кладёт данные ошибки в первый аргумент
                        err_data = call_error.args[0]
                        if isinstance(err_data, dict) and 'data' in err_data:
                            # Берём хекс revert'а
                            hex_data = err_data['data']
                            if isinstance(hex_data, str) and hex_data.startswith('0x'):
                                # Пытаемся декодировать revert reason (ABI encode для Error(string))
                                import binascii
                                reason_bytes = bytes.fromhex(hex_data[10:])  # убираем 0x + 4 байта селектора
                                try:
                                    reason_str = reason_bytes.decode('utf-8', errors='ignore').strip()
                                    self.logger.error(f"Причина реверта: {reason_str}")
                                except:
                                    self.logger.error(f"Не удалось декодировать причину реверта: {hex_data}")
                except Exception as decode_error:
                    self.logger.error(f"Ошибка при декодировании реверта: {decode_error}")

                
                # v6.5.1: Расширенная диагностика revert
                try:
                    # Пробуем получить детальную информацию через eth_call
                    self.logger.info("🔍 Запускаю детальную диагностику...")
                    
                        
                    # Декодируем ошибку если возможно
                    if hasattr(call_error, 'args') and len(call_error.args) > 0:
                        if isinstance(call_error.args[0], dict):
                            error_dict = call_error.args[0]
                            if 'data' in error_dict:
                                error_data = error_dict['data']
                                    
                                # Пробуем декодировать custom error
                                if isinstance(error_data, str) and error_data.startswith('0x'):
                                    # Известные селекторы ошибок
                                    ERROR_SELECTORS = {
                                        '0x08c379a0': 'Error(string)',  # Стандартная revert строка
                                        '0x4e487b71': 'Panic(uint256)',  # Panic ошибка
                                        '0x1425ea42': 'FailedInnerCall()',  # Custom error из нашего контракта
                                        '0x8cc95a87': 'MinProfitNotMet(uint256,uint256)',  # Custom error
                                        '0xb119b3b4': 'InvalidSwapPath()',  # Custom error
                                    }
                                        
                                    selector = error_data[:10] if len(error_data) >= 10 else error_data
                                        
                                    if selector in ERROR_SELECTORS:
                                        self.logger.error(f"   ⚠️ Тип ошибки: {ERROR_SELECTORS[selector]}")
                                            
                                        # Если это Error(string), пробуем декодировать сообщение
                                        if selector == '0x08c379a0' and len(error_data) > 10:
                                            try:
                                                # Убираем селектор и декодируем
                                                error_bytes = bytes.fromhex(error_data[10:])
                                                # Пропускаем offset и length (первые 64 байта)
                                                if len(error_bytes) > 64:
                                                    message_bytes = error_bytes[64:]
                                                    message = message_bytes.decode('utf-8', errors='ignore').strip('\x00')
                                                    self.logger.error(f"   📝 Сообщение: {message}")
                                            except:
                                                pass
                                    else:
                                        self.logger.error(f"   ❓ Неизвестный селектор ошибки: {selector}")
                                        self.logger.error(f"   📦 Полные данные: {error_data}")
                    
                    # Дополнительная проверка: баланс Flash Loan провайдера
                    self.logger.info("🔍 Проверяю состояние Flash Loan...")
                    
                    # Проверяем РЕАЛЬНУЮ ликвидность Aave через DataProvider
                    aave_data_provider = "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"  # Aave V3 PoolDataProvider на Polygon
                    
                    # ABI для getReserveData
                    data_provider_abi = json.loads('''[{
                        "inputs": [{"name": "asset", "type": "address"}],
                        "name": "getReserveData",
                        "outputs": [
                            {"name": "unbacked", "type": "uint256"},
                            {"name": "accruedToTreasuryScaled", "type": "uint256"},
                            {"name": "totalAToken", "type": "uint256"},
                            {"name": "totalStableDebt", "type": "uint256"},
                            {"name": "totalVariableDebt", "type": "uint256"},
                            {"name": "liquidityRate", "type": "uint256"},
                            {"name": "variableBorrowRate", "type": "uint256"},
                            {"name": "stableBorrowRate", "type": "uint256"},
                            {"name": "averageStableBorrowRate", "type": "uint256"},
                            {"name": "liquidityIndex", "type": "uint256"},
                            {"name": "variableBorrowIndex", "type": "uint256"},
                            {"name": "lastUpdateTimestamp", "type": "uint40"}
                        ],
                        "type": "function"
                    }]''')
                    
                    try:
                        data_provider_contract = self.w3.eth.contract(
                            address=Web3.to_checksum_address(aave_data_provider), 
                            abi=data_provider_abi
                        )
                        
                        # Получаем данные резерва
                        reserve_data = data_provider_contract.functions.getReserveData(flash_loan_asset).call()
                        
                        # totalAToken - это общая ликвидность (поставленные токены)
                        total_liquidity = reserve_data[2]  # totalAToken
                        total_stable_debt = reserve_data[3]  # totalStableDebt
                        total_variable_debt = reserve_data[4]  # totalVariableDebt
                        
                        # Доступная ликвидность = totalAToken - totalStableDebt - totalVariableDebt
                        available_liquidity = total_liquidity - total_stable_debt - total_variable_debt
                        
                        decimals = self.get_token_decimals(flash_loan_asset)
                        available_liquidity_decimal = available_liquidity / (10 ** decimals)
                        total_liquidity_decimal = total_liquidity / (10 ** decimals)
                        loan_amount_decimal = loan_amount / (10 ** decimals)
                        
                        self.logger.info(f"   💰 Aave доступная ликвидность {opportunity.quote_token_symbol}: {available_liquidity_decimal:,.2f}")
                        self.logger.info(f"   📊 Общая ликвидность в пуле: {total_liquidity_decimal:,.2f}")
                        self.logger.info(f"   📉 Занято в долг: {(total_stable_debt + total_variable_debt) / (10 ** decimals):,.2f}")
                        self.logger.info(f"   🎯 Запрашиваемый loan: {loan_amount_decimal:,.2f}")
                        
                        if available_liquidity < loan_amount:
                            self.logger.error(f"   ❌ НЕДОСТАТОЧНО ЛИКВИДНОСТИ В AAVE!")
                            self.logger.info(f"   💡 Максимально доступный loan: {available_liquidity_decimal:,.2f} {opportunity.quote_token_symbol}")
                        else:
                            self.logger.info(f"   ✅ Ликвидности достаточно для Flash Loan")
                            
                    except Exception as aave_error:
                        self.logger.error(f"   Ошибка получения данных Aave: {aave_error}")
                        
                        # Fallback: проверяем хотя бы баланс токенов на пуле
                        self.logger.info("   Пробую альтернативный метод...")
                        aave_pool = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
                        erc20_abi = json.loads('[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}]')
                        token_contract = self.w3.eth.contract(address=Web3.to_checksum_address(flash_loan_asset), abi=erc20_abi)
                        
                        pool_balance = token_contract.functions.balanceOf(aave_pool).call()
                        pool_balance_decimal = pool_balance / (10 ** self.get_token_decimals(flash_loan_asset))
                        self.logger.info(f"   💰 Баланс токенов на пуле: {pool_balance_decimal:,.2f} {opportunity.quote_token_symbol}")
                    
                    self.logger.info("🔍 Проверяю резервы на DEX...")
                    pairs_to_check = [
                        (opportunity.get('buy_pair'),  opportunity.get('buy_router'),  True,  opportunity.get('buy_dex')),
                        (opportunity.get('sell_pair'), opportunity.get('sell_router'), False, opportunity.get('sell_dex')),
                    ]

                    for i, (pair_address, router, is_buy, dex_name) in enumerate(pairs_to_check, 1):
                        action = "покупка" if is_buy else "продажа"
                        if not pair_address:
                            self.logger.warning(f"   DEX {i} ({action} на {dex_name or 'unknown'}): нет адреса пары — пропускаю")
                            continue
                        try:
                            # Минимальный ABI пары (UniswapV2/Quickswap): token0, token1, getReserves
                            pair_abi = json.loads("""
                            [
                              {"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},
                              {"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},
                              {"constant":true,"inputs":[],"name":"getReserves","outputs":[
                                {"internalType":"uint112","name":"reserve0","type":"uint112"},
                                {"internalType":"uint112","name":"reserve1","type":"uint112"},
                                {"internalType":"uint32","name":"blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"}
                            ]""".strip())
                            pair_contract = self.w3.eth.contract(address=Web3.to_checksum_address(pair_address), abi=pair_abi)
                            r0, r1, _ = pair_contract.functions.getReserves().call()
                            t0 = pair_contract.functions.token0().call()
                            t1 = pair_contract.functions.token1().call()

                            # Определяем, какой из резервов — базовый (quote), а какой — торгуемый
                            quote_addr = opportunity.quote_token_address
                            trade_addr = opportunity.token_address

                            if t0.lower() == quote_addr.lower():
                                base_reserve_raw, trade_reserve_raw = r0, r1
                            elif t1.lower() == quote_addr.lower():
                                base_reserve_raw, trade_reserve_raw = r1, r0
                            else:
                                self.logger.warning(f"   DEX {i} ({action} на {dex_name or 'unknown'}): ни один из токенов пары не совпадает с quote — пропускаю")
                                continue

                            base_decimals  = self.get_token_decimals(quote_addr)
                            trade_decimals = self.get_token_decimals(trade_addr)
                            base_reserve  = base_reserve_raw  / (10 ** base_decimals)
                            trade_reserve = trade_reserve_raw / (10 ** trade_decimals)

                            self.logger.info(f"   DEX {i} ({action} на {dex_name or 'unknown'}): резервы base={base_reserve:.2f}, trade={trade_reserve:.2f}")

                            
                        except Exception as reserve_error:
                            self.logger.warning(f"   DEX {i} ({action} на {dex_name or 'unknown'}): Отсеяно: некорректные резервы (0/1/None): {reserve_error}")

                    
                except Exception as diag_error:
                    self.logger.error(f"   Ошибка диагностики: {diag_error}")
                
                # Логируем параметры транзакции для отладки
                self.logger.info("📋 Параметры транзакции:")
                self.logger.info(f"   Flash loan токен: {flash_loan_asset}")

                # Получаем символ токена для Flash Loan
                flash_loan_symbol_local = "UNKNOWN"
                for addr, symbol in self.base_tokens.items():
                    if addr.lower() == flash_loan_asset.lower():
                        flash_loan_symbol_local = symbol
                        break
                if flash_loan_asset.lower() == opportunity.token_address.lower():
                    flash_loan_symbol_local = opportunity.token_symbol

                self.logger.info(f"   Flash loan amount: {loan_amount} ({loan_amount / (10 ** self.get_token_decimals(flash_loan_asset)):.4f} {flash_loan_symbol_local})")
                # self.logger.info(f"   Min profit: {min_profit_wei}")  # Закомментировано - переменная не определена
                self.logger.info(f"   Buy router: {opportunity.get('buy_router')}")
                self.logger.info(f"   Sell router: {opportunity.get('sell_router')}")
                self.logger.info(f"   Buy DEX: {opportunity.get('buy_dex')}")
                self.logger.info(f"   Sell DEX: {opportunity.get('sell_dex')}")

                
                return False
            
            # Подписываем и отправляем
            signed_txn = self.w3.eth.account.sign_transaction(transaction, PRIVATE_KEY)
            tx_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
            
            self.logger.info(f"Транзакция отправлена: {tx_hash.hex()}")
            
            # Ждем подтверждения
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            
            if receipt['status'] == 1:
                self.logger.info(f"✅ Арбитраж успешно исполнен!")
                self.trades_executed += 1
                self.total_profit += opportunity.expected_profit
                return True
            else:
                self.logger.error(f"❌ Транзакция отклонена")
                return False
                
        except Exception as e:
            self.logger.error(f"Ошибка исполнения: {e}")
            return False
            
    def get_chain_id(self) -> int:
        """Получает chain ID для текущей сети"""
        chain_ids = {
            "polygon": 137,
            "ethereum": 1,
            "bsc": 56,
            "arbitrum": 42161,
            "optimism": 10
        }
        return chain_ids.get(CHAIN, 137)
            
    async def run_single_scan(self):
        """Один цикл сканирования"""
        self.total_scans += 1
        
        # Каждые 5 сканирований меняем RPC
        if self.total_scans % 5 == 0:
            new_rpc = self.rpc_rotator.get_next()
            self.w3 = Web3(Web3.HTTPProvider(new_rpc))
            if CHAIN in ['polygon', 'bsc']:
                self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"СКАНИРОВАНИЕ #{self.total_scans}")
        self.logger.info(f"RPC: {self.rpc_rotator.get_current()[:30]}...")
        self.logger.info(f"{'='*60}")
        
        # Сброс статистики за скан
        self.combos_scan = 0
        self.cands_scan = 0
        self.n_low_spread = 0
        self.n_low_spread_onchain = 0  # НОВАЯ СТРОКА
        self.n_nets0 = 0
        self.n_nonv2 = 0
        self.skip_bad_reserves_onchain = 0
        self.all_spreads_scan = []
        
       # Инициализируем all_pairs (будет заполнено позже)
        self.logger.info("Загружаю данные о парах...")
        all_pairs = []  # Будет заполнено ниже
        
        # Ищем арбитражные возможности
        self.logger.info("Анализирую возможности...")
        
        # Проверяем, используем ли v2_combos
        if hasattr(self, 'load_v2_combos'):
            # Загружаем комбинации ОДИН РАЗ
            combos = await self.load_v2_combos()
            if combos:
                # Передаем combos в get_all_active_pairs чтобы не загружать повторно
                all_pairs = await self.get_all_active_pairs(combos)
                # Обрабатываем v2_combos напрямую
                opportunities = await self.process_v2_combos(combos)
            else:
                # Fallback на старый метод
                all_pairs = await self.get_all_active_pairs()
                opportunities = await self.find_arbitrage_opportunities(all_pairs)
        else:
            opportunities = await self.find_arbitrage_opportunities(all_pairs)
        
        # Выводим статистику ANKR если есть
        if hasattr(self, 'ankr_stats'):
            total = self.ankr_stats['success'] + self.ankr_stats['fail']
            if total > 0:
                success_rate = (self.ankr_stats['success'] / total) * 100
                self.logger.info(f"📈 ANKR общая статистика за скан: {self.ankr_stats['success']}/{total} ({success_rate:.1f}% успешных)")
                if success_rate < 50:
                    self.logger.warning(f"⚠️ ANKR работает плохо! Возможно DexScreener дает мусорные адреса пар")
        
        self.opportunities_found += len(opportunities)
        
        if opportunities:
            # Фильтруем только реально профитные (с size > 0)
            real_profitable = [opp for opp in opportunities if opp.optimal_trade_size > 0]
            
            if real_profitable:
                self.logger.info(f"🎯 Найдено {len(real_profitable)} прибыльных возможностей! (из {len(opportunities)} кандидатов)")
            else:
                self.logger.info(f"📊 Найдено {len(opportunities)} кандидатов, но все с size=0 (net_spread ≤ 0)")
            
            # Показываем топ-10 РЕАЛЬНО профитных
            for i, opp in enumerate(real_profitable[:10], 1):
                # Форматирование в USD для читаемости
                size_usd = opp.optimal_trade_size
                if opp.quote_token_symbol in ["WMATIC", "WETH"]:
                    token_prices = {"WMATIC": 0.5, "WETH": 2300}
                    size_usd = opp.optimal_trade_size * token_prices.get(opp.quote_token_symbol, 1)
                
                self.logger.info(
                    f"{i}. {opp.token_symbol}: "
                    f"спред {opp.spread_percent:.2f}% | "
                    f"размер ${size_usd:,.0f} ({opp.optimal_trade_size:.6f} {opp.quote_token_symbol}) | "  # ДОБАВИЛИ размер в токенах
                    f"прибыль ${opp.expected_profit:,.2f} | "
                    f"impact {opp.price_impact_percent:.2f}%"
                )
                self.logger.info(f"   {opp.buy_dex} -> {opp.sell_dex} | База: {opp.quote_token_symbol}")
            
            # Пробуем исполнить первые 3 возможности (если первая не сработает)
            for i, opportunity in enumerate(opportunities[:3], 1):
                if opportunity.expected_profit >= MIN_PROFIT_USD:
                    self.logger.info(f"Попытка #{i}: исполняю {opportunity.token_symbol}")
                    result = await self.execute_arbitrage(opportunity)
                    
                    if result is None:
                        self.logger.info(f"⏭️ Пропущена #{i}: пред-чек/валидация.")
                        continue
                    elif result is True:
                        self.logger.info(f"✅ Успешно исполнена возможность #{i}")
                        break
                    else:
                        self.logger.warning(f"❌ Не удалось исполнить #{i}, пробую следующую...")

                
        else:
            self.logger.info("Прибыльных возможностей не найдено")
            
        # Расширенная статистика с перцентилями
        self.logger.info(f"\n— СТАТИСТИКА —")
        
        # Выводим TOP5 спредов ДО V2-фильтра (из всех проверенных комбинаций)
        if hasattr(self, 'all_spreads_before_v2') and self.all_spreads_before_v2:
            sorted_before = sorted(self.all_spreads_before_v2, key=lambda x: x[0], reverse=True)[:5]
            self.logger.info("TOP5 спредов (ДО V2-фильтра):")
            for spread, token, buy_dex, sell_dex, reason in sorted_before:
                self.logger.info(f"  {spread:.3f}% | {token[:8]} | {buy_dex}->{sell_dex} | отсеян: {reason}")
        
        # Выводим TOP5 спредов ПОСЛЕ V2-фильтра (что реально торгуем)
        if hasattr(self, 'all_spreads_after_v2') and self.all_spreads_after_v2:
            sorted_after = sorted(self.all_spreads_after_v2, key=lambda x: x[0], reverse=True)[:5]
            self.logger.info("TOP5 спредов (ПОСЛЕ V2-фильтра):")
            for spread, token, buy_dex, sell_dex in sorted_after:
                self.logger.info(f"  {spread:.3f}% | {token[:8]} | {buy_dex}->{sell_dex}")
        
        self.logger.info(f"Комбинаций проверено: {self.combos_scan if hasattr(self, 'combos_scan') else 0}")
        self.logger.info(f"Кандидаты (спред >= {MIN_SPREAD_PERCENT}%): {self.cands_scan if hasattr(self, 'cands_scan') else 0}")
        
        # Считаем реально профитные по тем же критериям, что и пред-чек
        real_profitable = []
        for opp in opportunities:
            if opp.optimal_trade_size <= 0:
                continue
            # Эмулируем пред-чек экономики
            base_decimals = self.get_token_decimals(opp.quote_token_address) 
            loan_amount = int(opp.optimal_trade_size * (10 ** base_decimals))
            MIN_FLASH_LOAN = int(1e-6 * (10 ** base_decimals))  # MIN_FLASH_STEP
            
            if loan_amount >= MIN_FLASH_LOAN and opp.expected_profit >= MIN_PROFIT_USD:
                real_profitable.append(opp)
        
        self.logger.info(f"Профитные (за скан): {len(real_profitable)} из {len(opportunities)} найденных")
        
        self.logger.info(f"")
        self.logger.info(f"")
        self.logger.info(f"Отсеяно:")
        self.logger.info(f"  спред < {MIN_SPREAD_PERCENT}% (DS): {getattr(self, 'n_low_spread', 0)}")
        self.logger.info(f"  спред < {MIN_SPREAD_PERCENT}% (onchain): {getattr(self, 'n_low_spread_onchain', 0)}")
        self.logger.info(f"  ликвидность < ${MIN_LIQUIDITY_USD} (onchain USD): {getattr(self, 'n_low_liquidity_usd', 0)}")  # НОВОЕ
        self.logger.info(f"  net_spread ≤ 0 (по LUT): {getattr(self, 'n_nets0', 0)}")
        self.logger.info(f"  тип DEX не V2: {getattr(self, 'n_nonv2', 0)}")
        self.logger.info(f"  недоступны резервы onchain: {getattr(self, 'skip_bad_reserves_onchain', 0)}")
        self.logger.info(f"  не V2-пары: {getattr(self, 'skip_non_v2_pair', 0)}")
        self.logger.info(f"  ANKR rate limits: {getattr(self, 'skip_ankr_rate_limit', 0)}")
        self.logger.info(f"  несовпадение адресов: {getattr(self, 'skip_address_mismatch', 0)}")

        # Перцентили спредов по ВСЕМ проверенным комбинациям (не только профитным)
        if hasattr(self, 'all_spreads_scan') and self.all_spreads_scan:
            all_spreads_sorted = sorted(self.all_spreads_scan)
            if len(all_spreads_sorted) > 20:  # Только если достаточно данных
                p95_all = all_spreads_sorted[int(len(all_spreads_sorted) * 0.95)]
                p99_all = all_spreads_sorted[int(len(all_spreads_sorted) * 0.99)]
                max_all = all_spreads_sorted[-1]
                self.logger.info(f"")
                self.logger.info(f"Спреды ВСЕХ комбинаций: max={max_all:.3f}% p95={p95_all:.3f}% p99={p99_all:.3f}%")
        
        # Анализ спредов для профитных возможностей  
        if real_profitable:
            spreads = [opp.spread_percent for opp in real_profitable]
            if spreads:
                max_spread = max(spreads)
                p95_spread = sorted(spreads)[int(len(spreads) * 0.95)] if len(spreads) > 1 else max_spread
                p99_spread = sorted(spreads)[int(len(spreads) * 0.99)] if len(spreads) > 1 else max_spread
                
                # Оценка net_spread (примерная)
                fees_total = 0.0065  # 0.3% + 0.3% + 0.05%
                net_spreads = [(s/100 - fees_total) for s in spreads]
                max_net = max(net_spreads) * 100
                
                self.logger.info(f"Спреды профитных: max={max_spread:.3f}% p95={p95_spread:.3f}% p99={p99_spread:.3f}%")
                self.logger.info(f"Max net_spread: {max_net:.3f}%")
                
        self.logger.info(f"")
        self.logger.info(f"Комиссии (факт): buy=0.300% sell=0.300% flash=0.050% → всего=0.650%")
        self.logger.info(f"")
        self.logger.info(f"Всего сканирований: {self.total_scans}")
        self.logger.info(f"Всего найдено возможностей: {self.opportunities_found}")
        self.logger.info(f"Исполнено сделок: {self.trades_executed}")
        self.logger.info(f"Общая прибыль: ${self.total_profit:,.2f}")
        
    async def run(self):
        """Основной цикл бота"""
        self.logger.info("🚀 Запуск Универсального Арбитражного Бота v6.3")
        self.logger.info(f"Настройки:")
        self.logger.info(f"   Сеть: {CHAIN}")
        self.logger.info(f"   Контракт: УНИВЕРСАЛЬНЫЙ")
        self.logger.info(f"   Поддержка: V2, V3, Algebra DEX")
        self.logger.info(f"   Известные роутеры: {len(self.dex_config)}")
        self.logger.info(f"   Включенные DEX: {', '.join(self.enabled_dexes)}")
        self.logger.info(f"   Расширенные базовые токены: ВКЛ ✅ (v6.3)")
        self.logger.info(f"   Базовые токены: {', '.join(self.base_tokens.values())}")
        self.logger.info(f"   Мин. прибыль: ${MIN_PROFIT_USD}")
        self.logger.info(f"   Мин. ликвидность: ${MIN_LIQUIDITY_USD:,}")
        
        # Создаем HTTP сессию с ограничением параллельных запросов
        from config import MAX_CONCURRENT_REQUESTS
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
        self.session = aiohttp.ClientSession(connector=connector)
        
        try:
            while True:
                try:
                    await self.run_single_scan()
                    
                        
                except Exception as e:
                    self.logger.error(f"Ошибка в цикле сканирования: {e}")
                    
                # Ждем перед следующим сканированием
                self.logger.info(f"\nЖду {SCAN_INTERVAL} секунд...")
                await asyncio.sleep(SCAN_INTERVAL)
                
        finally:
            await self.session.close()


async def main():
    """Точка входа"""
    print("\n🔍 DEBUG: Функция main() запущена")
    
    # Проверяем переменные окружения
    if not CONTRACT_ADDRESS or not PRIVATE_KEY:
        print("ОШИБКА: установите CONTRACT_ADDRESS и PRIVATE_KEY в .env файле")
        return
    
    print("🔍 DEBUG: Переменные окружения проверены")
    print(f"🔍 DEBUG: CONTRACT_ADDRESS = {CONTRACT_ADDRESS}")
    print(f"🔍 DEBUG: PRIVATE_KEY = {'*' * 10 if PRIVATE_KEY else 'НЕ УСТАНОВЛЕН'}")
    
    # Создаем и запускаем бота
    print("🔍 DEBUG: Создаю экземпляр бота...")
    bot = ArbitrageBot()
    print("🔍 DEBUG: Запускаю бота...")
    await bot.run()


if __name__ == "__main__":
    print("\n🔍 DEBUG: Скрипт запущен как главный модуль")
    print("🔍 DEBUG: Запускаю asyncio.run(main())...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Остановлено пользователем")
    except Exception as e:
        print(f"\n❌ ОШИБКА В MAIN: {e}")
        traceback.print_exc()