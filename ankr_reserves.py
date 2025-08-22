import asyncio
import aiohttp
from web3 import Web3
from typing import Dict, List, Optional, Tuple
import json

class AnkrReservesProvider:
    def __init__(self, api_key: str, w3: Web3, logger):
        self.api_key = api_key
        self.w3 = w3
        self.logger = logger
        self.ankr_url = f"https://rpc.ankr.com/polygon/{api_key}"
        self.session = None  # ВАЖНО: инициализируем session как None
    
    async def ensure_session(self):
        """Создаем или переиспользуем существующую сессию"""
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
            self.logger.debug("Создана новая HTTP сессия для ANKR")
    
    async def get_reserves_batch(self, pairs: List[str]) -> Dict:
        """Получает ТОЛЬКО резервы для списка пар через Ankr (без token0/token1)"""
        
        if not pairs:
            return {}
            
        # Убеждаемся что сессия создана ОДИН РАЗ
        await self.ensure_session()
        
        reserves_map = {}
        
        # Формируем батч ТОЛЬКО для getReserves (1 запрос на пару вместо 3)
        batch_payload = []
        
        for i, pair_address in enumerate(pairs):
            batch_payload.append({
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{
                    "to": pair_address,
                    "data": "0x0902f1ac"  # getReserves()
                }, "latest"],
                "id": i + 1
            })
            
        try:
            self.logger.debug(f"Отправляю ANKR батч из {len(batch_payload)} запросов...")
            # ОДИН запрос со ВСЕМИ вызовами сразу
            async with self.session.post(self.ankr_url, json=batch_payload, timeout=30) as response:
                results = await response.json()
                
                # Парсим результаты
                if isinstance(results, list):
                    for i, pair_address in enumerate(pairs):
                        if i < len(results) and "result" in results[i]:
                            data = results[i]["result"]
                            if data and data != "0x":
                                # Декодируем резервы
                                reserve0 = int(data[2:66], 16) if len(data) >= 66 else 0
                                reserve1 = int(data[66:130], 16) if len(data) >= 130 else 0
                                
                                reserves_map[pair_address] = {
                                    'reserve0': reserve0,
                                    'reserve1': reserve1,
                                    'token0': None,  # Не загружаем
                                    'token1': None   # Не загружаем
                                }
                else:
                    # ANKR вернул ошибку в виде dict (rate limit или другая ошибка)
                    if isinstance(results, dict):
                        if "error" in results:
                            self.logger.debug(f"ANKR ошибка: {results.get('error', {}).get('message', 'Unknown error')}")
                        else:
                            self.logger.debug(f"ANKR вернул неожиданный dict: {results}")
                    else:
                        self.logger.debug(f"ANKR вернул неожиданный формат: {type(results)}")
                            
        except asyncio.TimeoutError:
            self.logger.error(f"⏱️ ANKR timeout для батча из {len(pairs)} пар")
        except Exception as e:
            self.logger.error(f"❌ Ошибка ANKR батч-запроса: {e}")
        
        return reserves_map
    
    async def get_reserves_many(self, pairs: List[str]) -> Dict[str, Tuple]:
        """Массовое получение резервов с параллельными запросами через ANKR"""
        if not pairs:
            return {}
        
        import time
        start_time = time.time()
        unique_pairs = list(set(pairs))  # Убираем дубликаты
        
        # Константы для батчевой обработки - МАКСИМАЛЬНО БЕЗОПАСНЫЕ ПАРАМЕТРЫ
        BATCH_SIZE = 30  # Размер батча (10 пар = 30 запросов) - проверенный размер
        CONCURRENCY = 1   # БЕЗ параллельности - только последовательно
        MIN_BATCH_SIZE = 2  # Минимальный размер при делении пополам
        
        # Логируем план
        n_batches = (len(unique_pairs) + BATCH_SIZE - 1) // BATCH_SIZE
        self.logger.info(f"🔄 fetch_reserves: {len(unique_pairs)} pairs → {n_batches} батчей ({CONCURRENCY} параллельно)")
        
        # Начальная задержка чтобы не нагружать ANKR сразу
        await asyncio.sleep(1.0)
        
        # Разбиваем на батчи
        batches = []
        for i in range(0, len(unique_pairs), BATCH_SIZE):
            batch = unique_pairs[i:i + BATCH_SIZE]
            batches.append(batch)
        
        # Обрабатываем батчи параллельно
        all_reserves = {}
        for i in range(0, len(batches), CONCURRENCY):
            concurrent_batches = batches[i:i + CONCURRENCY]
            
            # Запускаем батчи параллельно
            tasks = []
            for batch in concurrent_batches:
                task = self._process_batch_with_retry(batch)
                tasks.append(task)
            
            # Ждем завершения всех батчей в группе
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Собираем результаты
            for result in results:
                if isinstance(result, dict):
                    self.logger.debug(f"Батч вернул {len(result)} пар")
                    all_reserves.update(result)
                elif isinstance(result, Exception):
                    self.logger.warning(f"Batch failed: {result}")
            
            # Логируем прогресс - показываем реальное количество обработанных пар
            processed = len(all_reserves)  # Реальное количество обработанных пар
            self.logger.info(f"📊 Обработано {processed}/{len(unique_pairs)} пар")
            
            # Задержка между группами батчей чтобы не превысить rate limit
            if processed < len(unique_pairs):
                await asyncio.sleep(1.0)  # Гарантированная задержка 1 секунда
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ Загружено резервов: {len(all_reserves)}/{len(unique_pairs)} за {elapsed:.2f}s")
        
        # Конвертируем в кортежи для совместимости
        result = {}
        for pair, data in all_reserves.items():
            if isinstance(data, dict):
                result[pair] = (
                    data.get('reserve0', 0),
                    data.get('reserve1', 0),
                    data.get('token0'),
                    data.get('token1')
                )
        return result
    
    async def _process_batch_with_retry(self, batch: List[str], retry_count: int = 0) -> Dict:
        """Обработка батча с retry и делением пополам при ошибке"""
        MAX_RETRIES = 3
        MIN_BATCH_SIZE = 2  # Минимальный размер при делении пополам
        
        try:
            # Пробуем обработать батч целиком через существующий метод
            result = await self.get_reserves_batch(batch)
            if result:
                return result
            
            # Если пустой результат и можно разделить
            if len(batch) > MIN_BATCH_SIZE and retry_count < MAX_RETRIES:
                self.logger.debug(f"Batch failed, splitting {len(batch)} → {len(batch)//2}")
                
                # Делим пополам
                mid = len(batch) // 2
                left_batch = batch[:mid]
                right_batch = batch[mid:]
                
                # Обрабатываем половинки параллельно
                left_task = self._process_batch_with_retry(left_batch, retry_count + 1)
                right_task = self._process_batch_with_retry(right_batch, retry_count + 1)
                
                left_result, right_result = await asyncio.gather(left_task, right_task)
                
                # Объединяем результаты
                combined = {}
                if left_result:
                    combined.update(left_result)
                if right_result:
                    combined.update(right_result)
                return combined
            
            return {}
            
        except asyncio.TimeoutError:
            if retry_count < MAX_RETRIES:
                # Backoff: 2s, 4s, 8s - больше времени при rate limit
                backoff = min(2.0 * (2 ** retry_count), 10.0)
                self.logger.debug(f"Timeout, retry {retry_count + 1}/{MAX_RETRIES} after {backoff}s")
                await asyncio.sleep(backoff)
                return await self._process_batch_with_retry(batch, retry_count + 1)
            else:
                self.logger.error(f"Batch timeout after {MAX_RETRIES} retries")
                return {}
        except Exception as e:
            error_str = str(e).lower()
            # Проверяем на rate limit
            if 'rate limit' in error_str or 'too many' in error_str or '429' in error_str:
                if retry_count < MAX_RETRIES:
                    backoff = min(2.0 * (2 ** retry_count), 10.0)  # Увеличиваем задержку: 2s, 4s, 8s
                    self.logger.debug(f"Rate limit, retry {retry_count + 1}/{MAX_RETRIES} after {backoff}s")
                    await asyncio.sleep(backoff)
                    return await self._process_batch_with_retry(batch, retry_count + 1)
            
            self.logger.error(f"Batch error: {e}")
            return {}
    
    async def get_decimals_many(self, tokens: List[str]) -> Dict[str, int]:
        """Массовое получение decimals с кэшированием через ANKR"""
        if not tokens:
            return {}
        
        import time
        import json
        from pathlib import Path
        
        start_time = time.time()
        unique_tokens = list(set(tokens))
        
        # Загружаем кэш
        cache_dir = Path("cache")
        cache_file = cache_dir / "decimals.json"
        decimals_cache = {}
        
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    decimals_cache = json.load(f)
                self.logger.debug(f"📂 Загружен кэш decimals: {len(decimals_cache)} токенов")
            except Exception as e:
                self.logger.warning(f"⚠️ Не удалось загрузить кэш decimals: {e}")
        else:
            cache_dir.mkdir(exist_ok=True)
        
        # Проверяем кэш
        result = {}
        tokens_to_fetch = []
        cache_hits = 0
        
        for token in unique_tokens:
            token_lower = token.lower()
            if token_lower in decimals_cache:
                result[token] = decimals_cache[token_lower]
                cache_hits += 1
            else:
                tokens_to_fetch.append(token)
        
        if not tokens_to_fetch:
            self.logger.info(f"✅ Все {len(unique_tokens)} decimals из кэша (100% cache hit)")
            return result
        
        # Получаем недостающие decimals батчами через ANKR
        BATCH_SIZE = 100
        n_batches = (len(tokens_to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
        cache_percent = (cache_hits / len(unique_tokens)) * 100 if unique_tokens else 0
        self.logger.info(f"🔄 fetch_decimals: кэш {cache_hits}/{len(unique_tokens)} ({cache_percent:.1f}%), загружаю {len(tokens_to_fetch)} новых")
        
        # Убеждаемся что сессия создана
        await self.ensure_session()
        
        for i in range(0, len(tokens_to_fetch), BATCH_SIZE):
            batch = tokens_to_fetch[i:i + BATCH_SIZE]
            
            # Формируем батч-запрос для decimals
            batch_payload = []
            for j, token_address in enumerate(batch):
                batch_payload.append({
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [{
                        "to": token_address,
                        "data": "0x313ce567"  # decimals()
                    }, "latest"],
                    "id": j + 1
                })
            
            try:
                # Отправляем батч через ANKR
                async with self.session.post(self.ankr_url, json=batch_payload, timeout=30) as response:
                    results = await response.json()
                    
                    if isinstance(results, list):
                        for j, token_address in enumerate(batch):
                            if j < len(results) and "result" in results[j]:
                                try:
                                    decimals = int(results[j]["result"], 16)
                                    result[token_address] = decimals
                                    decimals_cache[token_address.lower()] = decimals
                                except:
                                    # По умолчанию 18
                                    result[token_address] = 18
                                    decimals_cache[token_address.lower()] = 18
                            else:
                                result[token_address] = 18
                                decimals_cache[token_address.lower()] = 18
            except Exception as e:
                self.logger.warning(f"Failed to get decimals for batch: {e}")
                # Используем значение по умолчанию для всего батча
                for token_address in batch:
                    result[token_address] = 18
                    decimals_cache[token_address.lower()] = 18
        
        # Сохраняем обновленный кэш
        try:
            with open(cache_file, 'w') as f:
                json.dump(decimals_cache, f, indent=2)
        except Exception as e:
            self.logger.warning(f"⚠️ Не удалось сохранить кэш decimals: {e}")
        
        elapsed = time.time() - start_time
        new_fetched = len(tokens_to_fetch)
        self.logger.info(f"✅ Decimals готовы: {cache_hits} из кэша + {new_fetched} новых = {len(result)} total за {elapsed:.2f}s")
        
        return result
    
    def _parse_reserves(self, result, pairs):
        """Парсит результаты от Ankr"""
        reserves_map = {}
        
        if "result" in result:
            data = result["result"]
            # Декодируем getReserves() результат
            # Returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)
            reserve0 = int(data[2:66], 16) if len(data) > 66 else 0
            reserve1 = int(data[66:130], 16) if len(data) > 130 else 0
            
            reserves_map[pairs[0]] = {
                'reserve0': reserve0,
                'reserve1': reserve1
            }
        
        return reserves_map
    
    async def close(self):
        """Закрываем сессию при завершении работы"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
            self.logger.debug("HTTP сессия ANKR закрыта")