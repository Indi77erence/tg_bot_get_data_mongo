import asyncio
import json
import logging
import sys

from aiogram.enums import ParseMode
from aiogram import Bot, Dispatcher, types
from config import BOT_TOKEN
from algorithm import my_algo, check_collection, QUERY

"""Токен не прятал)"""

dp = Dispatcher()


@dp.message()
async def handler_name(message: types.Message):
    try:
        string = message.text
        input_data = json.loads(string)
        answer_user = json.dumps(my_algo(input_data=input_data,
                                         collection=check_collection(collection_name='mycollection'),
                                         query=QUERY))
        await message.answer(answer_user)
    except json.decoder.JSONDecodeError:
        primer = '{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'
        await message.answer(f"Невалидный запрос. Пример запроса: {primer}")


async def main():
    bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
