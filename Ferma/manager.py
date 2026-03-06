import asyncio
from datetime import datetime

class TokenManager:
    def __init__(self):
        self.farmers = {}      # {bot_id: tokens_available}
        self.seller_queue = [] # Очередь заказов
        self.transfer_log = [] # Лог передач
        
    async def balance_farmers(self):
        """Распределение нагрузки между фармерами"""
        while True:
            # Находим фармера с максимумом токенов
            best_farmer = max(self.farmers, key=self.farmers.get)
            
            if self.farmers[best_farmer] >= 10:
                # Переводим токены на бота-продавца
                await self.transfer_to_seller(best_farmer, 10)
                
            await asyncio.sleep(300)  # Проверка каждые 5 мин
            
    async def transfer_to_seller(self, from_bot, amount):
        """Автоматический трансфер между ботами"""
        # Бот-фармер заходит в Trade Realm
        # Передаёт токены боту-продавцу
        # Обновляем балансы
        
        self.farmers[from_bot] -= amount
        self.farmers['seller'] = self.farmers.get('seller', 0) + amount
        
        self.transfer_log.append({
            'time': datetime.now(),
            'from': from_bot,
            'to': 'seller',
            'amount': amount
        })
        
        print(f"[Менеджер] Переведено {amount} токенов от бота {from_bot}")
        
    def get_seller_balance(self):
        """Баланс для продажи клиентам"""
        return self.farmers.get('seller', 0)