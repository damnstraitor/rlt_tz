import os
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("TELEGRAM_BOT_TOKEN" )
print(f"Токен из .env: {token}")
print(f"Тип токена: {type(token)}")
print(f"Длина токена: {len(token) if token else 0}")

# Проверка структуры токена
if token:
    if ':' in token:
        print("✅ Токен содержит ':' - правильно")
    else:
        print("❌ Токен не содержит ':' - неправильный формат")
else:
    print("❌ Токен не найден!")