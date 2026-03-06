import pyautogui
import time
import random
import cv2
import numpy as np
from threading import Thread

class SonariaFarmer:
    def __init__(self, account_id):
        self.account_id = account_id
        self.tokens_earned = 0
        self.running = True
        self.respawn_timer = 0
        
        # Настройки анти-детекта
        pyautogui.FAILSAFE = True
        pyautogui.PAUSE = 0.1
        
    def start_farming(self):
        """Главный цикл фарма"""
        print(f"[Бот {self.account_id}] Запуск фарма...")
        
        while self.running:
            try:
                self.check_respawn()
                self.find_and_kill_targets()
                self.suicide_for_token()
                
                # Случайная пауза (анти-паттерн)
                time.sleep(random.uniform(0.5, 1.5))
                
            except Exception as e:
                print(f"Ошибка: {e}")
                time.sleep(5)
                
    def check_respawn(self):
        """Проверка экрана смерти и респавн"""
        if self.detect_death_screen():
            print(f"[Бот {self.account_id}] Смерть, ожидание респавна...")
            time.sleep(8)  # Таймер респавна
            pyautogui.click(960, 600)  # Кнопка Respawn
            time.sleep(3)
            
    def find_and_kill_targets(self):
        """Поиск травоядных и убийство"""
        # Скриншот экрана
        screen = self.take_screenshot()
        
        # Поиск целей по цвету (травоядные обычно зелёные/жёлтые)
        targets = self.detect_targets(screen)
        
        if targets:
            nearest = self.find_nearest(targets)
            self.move_to_target(nearest)
            self.attack_target()
        else:
            self.patrol_random()
            
    def detect_targets(self, screen):
        """OpenCV: поиск целей по цветовой маске"""
        hsv = cv2.cvtColor(screen, cv2.COLOR_RGB2HSV)
        
        # Диапазон цветов для травоядных (настрой под Sonaria)
        lower_green = np.array([35, 50, 50])
        upper_green = np.array([85, 255, 255])
        
        mask = cv2.inRange(hsv, lower_green, upper_green)
        contours, _ = cv2.findContours(mask, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        
        targets = []
        for cnt in contours:
            if cv2.contourArea(cnt) > 500:  # Минимальный размер
                x, y, w, h = cv2.boundingRect(cnt)
                targets.append((x + w//2, y + h//2))
                
        return targets
        
    def move_to_target(self, coords):
        """Движение к цели с рандомизацией"""
        x, y = coords
        
        # Добавляем случайное отклонение (человечность)
        x += random.randint(-30, 30)
        y += random.randint(-30, 30)
        
        # WASD движение
        if x < 800:
            pyautogui.keyDown('a')
            time.sleep(random.uniform(0.1, 0.3))
            pyautogui.keyUp('a')
        elif x > 1120:
            pyautogui.keyDown('d')
            time.sleep(random.uniform(0.1, 0.3))
            pyautogui.keyUp('d')
            
        if y < 450:
            pyautogui.keyDown('w')
            time.sleep(random.uniform(0.1, 0.3))
            pyautogui.keyUp('w')
        elif y > 630:
            pyautogui.keyDown('s')
            time.sleep(random.uniform(0.1, 0.3))
            pyautogui.keyUp('s')
            
        # Прыжок если препятствие
        if random.random() > 0.7:
            pyautogui.press('space')
            
    def attack_target(self):
        """Атака с рандомизацией"""
        # Левая кнопка мыши (укус/удар)
        pyautogui.click()
        time.sleep(random.uniform(0.2, 0.5))
        
        # Иногда специальная атака (правая кнопка)
        if random.random() > 0.6:
            pyautogui.rightClick()
            time.sleep(0.3)
            
    def suicide_for_token(self):
        """Самоубийство для получения Death Gacha Token"""
        # Проверка Death Points (через OCR или память)
        current_dp = self.check_death_points()
        
        if current_dp >= 1200:  # Порог для токена
            print(f"[Бот {self.account_id}] DP: {current_dp}, суицид...")
            
            # Прыжок в воду/с высоты или атака сильного моба
            pyautogui.keyDown('space')
            time.sleep(2)
            pyautogui.keyUp('space')
            
            self.tokens_earned += 1
            self.save_progress()
            
    def check_death_points(self):
        """OCR: чтение DP с экрана"""
        # Скриншот области с DP
        dp_region = pyautogui.screenshot(region=(1700, 50, 100, 30))
        dp_region.save('dp_temp.png')
        
        # Tesseract OCR
        import pytesseract
        text = pytesseract.image_to_string('dp_temp.png', config='--psm 7')
        
        try:
            return int(''.join(filter(str.isdigit, text)))
        except:
            return 0
            
    def patrol_random(self):
        """Случайное блуждание если целей нет"""
        moves = ['w', 'a', 's', 'd']
        for _ in range(random.randint(3, 8)):
            key = random.choice(moves)
            pyautogui.keyDown(key)
            time.sleep(random.uniform(0.3, 0.8))
            pyautogui.keyUp(key)
            
    def take_screenshot(self):
        """Скриншот для OpenCV"""
        img = pyautogui.screenshot()
        return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
        
    def save_progress(self):
        """Сохранение в БД"""
        import sqlite3
        conn = sqlite3.connect('farm_data.db')
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO bots 
                     (id, tokens, last_active) VALUES (?, ?, ?)''',
                  (self.account_id, self.tokens_earned, time.time()))
        conn.commit()
        conn.close()

# Запуск фермы
def start_farm_legion(bot_count=5):
    """Запуск N ботов параллельно"""
    bots = []
    for i in range(bot_count):
        bot = SonariaFarmer(account_id=i+1)
        thread = Thread(target=bot.start_farming)
        thread.daemon = True
        thread.start()
        bots.append(bot)
        time.sleep(10)  # Задержка между запусками
        
    return bots

# Старт
if __name__ == "__main__":
    farm = start_farm_legion(bot_count=3)  # 3 фармера
    
    try:
        while True:
            time.sleep(60)
            # Статистика каждую минуту
    except KeyboardInterrupt:
        print("Остановка фермы...")