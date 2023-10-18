import json
import os

import dotenv
import requests


dotenv.load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

def send_telegram_notification(message):
    telegram_api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    data = {'chat_id': TELEGRAM_CHAT_ID, 'text': message}
    response = requests.post(telegram_api_url, json=data)
    if response.status_code != 200:
        print('Failed to send Telegram notification:', response.content)
