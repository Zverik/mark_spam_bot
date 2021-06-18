#!/usr/bin/env python3
import logging
import aiosqlite
import config
import aiohttp
import asyncio
from aiogram import Bot, Dispatcher, executor, types, exceptions
from aiogram.types import ChatType, ContentType


logging.basicConfig(level=logging.INFO)
bot = Bot(token=config.TOKEN)
dp = Dispatcher(bot)
_db = None


async def get_db():
    global _db
    if _db is not None and _db._running:
        return _db
    _db = await aiosqlite.connect(config.DATABASE)
    exists_query = ("select count(*) from sqlite_master where type = 'table' "
                    "and name = 'admins'")
    async with _db.execute(exists_query) as cursor:
        has_tables = (await cursor.fetchone())[0] == 1
    if not has_tables:
        logging.info('Creating tables')
        queries = [
            "create table admins (user_id integer not null, chat_id integer not null)",
            "create unique index admin_idx on admins (user_id, chat_id)",
            "create index chat_idx on admins (chat_id)",
        ]
        for q in queries:
            await _db.execute(q)
    return _db


async def shutdown(dp):
    if _db is not None and _db._running:
        await _db.close()


async def test_spammer(user: types.User) -> bool:
    url = f'https://api.cas.chat/check?user_id={user.id}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            logging.info('Testing user "%s": %s %s', user.full_name,
                         response.status, await response.text())
            if response.status == 200:
                data = await response.json()
                if data.get('ok') and 'result' in data:
                    return True
    return False


async def forget_user(user_id: int):
    db = await get_db()
    await db.execute("delete from admins where user_id = ?", (user_id,))
    await db.commit()


async def send_message(user_id, text, inner=False):
    try:
        await bot.send_message(user_id, text, disable_web_page_preview=True)
        return True
    except exceptions.BotBlocked:
        logging.info(f"Target [ID:{user_id}]: blocked by user")
        await forget_user(user_id)
    except exceptions.ChatNotFound:
        logging.info(f"Target [ID:{user_id}]: invalid user ID")
        await forget_user(user_id)
    except exceptions.UserDeactivated:
        logging.info(f"Target [ID:{user_id}]: user is deactivated")
        await forget_user(user_id)
    except exceptions.RetryAfter as e:
        if not inner:
            logging.info(f"Flood limit is exceeded. Sleep {e.timeout} seconds.")
            await asyncio.sleep(e.timeout)
            return await send_message(user_id, text, True)
        else:
            logging.info(f"Flood limit is again exceeded. Needs {e.timeout} more seconds.")
    except exceptions.TelegramAPIError:
        logging.exception('Failed to send a broadcast message')
    return False


def message_url(msg: types.Message) -> str:
    url = "https://t.me/"
    if msg.chat.username:
        url += f"{msg.chat.username}/"
    else:
        url += f"c/{msg.chat.id}/"
    url += f"{msg.message_id}"
    return url


async def broadcast(message: types.Message, text: str):
    ids = set()

    # First get group admins, if there are not too many.
    try:
        admins = await bot.get_chat_administrators(message.chat.id)
        admin_ids = [adm.user.id for adm in admins]
        if len(admin_ids) <= config.MAX_ADMINS:
            ids.update(admin_ids)
    except exceptions.TelegramAPIError:
        pass

    # Add users subscribed to the group events.
    db = await get_db()
    cursor = await db.execute("select user_id from admins where chat_id = ?",
                              (message.chat.id,))
    ids.update([row[0] async for row in cursor])

    # Broadcast the message to the users.
    content = text + f' Chat "{message.chat.title}": ' + message_url(message)
    count = 0
    for user_id in ids:
        if await send_message(user_id, content):
            count += 1
        await asyncio.sleep(0.05)
    return count > 0


@dp.message_handler(commands=['start', 'help'], chat_type=[ChatType.PRIVATE])
async def welcome(message: types.Message):
    await message.answer(
        'This bot listens to the /spam command in groups and '
        'super groups and notifies admins. Also it requests CAS '
        'status for every new user.\n\n'
        'Type /spamme in a group to subscribe, /spamnot to '
        'unsubscribe. This bot has no settings or other commands.\n\n'
        'Powered by <a href="https://cas.chat/">CAS</a>, '
        'see source code <a href="https://github.com/Zverik/mark_spam_bot">on github</a>',
        disable_web_page_preview=True, parse_mode='HTML'
    )


@dp.message_handler(commands='spamme', chat_type=[ChatType.GROUP, ChatType.SUPERGROUP])
async def spam_me(message: types.Message):
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        db = await get_db()
        await db.execute("insert or ignore into admins (user_id, chat_id) values (?, ?)",
                         (message.from_user.id, message.chat.id))
        await db.commit()
        await bot.send_message(
            message.from_user.id,
            f"You've been subscribed to events in \"{message.chat.title}\". "
            "Send /spamnot to the group to unsubscribe.")


@dp.message_handler(commands='spamnot', chat_type=[ChatType.GROUP, ChatType.SUPERGROUP])
async def spam_not(message: types.Message):
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        db = await get_db()
        await db.execute("delete from admins where user_id = ? and chat_id = ?",
                         (message.from_user.id, message.chat.id))
        await db.commit()
        await bot.send_message(
            message.from_user.id,
            f"No more notifications from \"{message.chat.title}\".")


async def delete_timeout(chat_id: int, message_id: int, timeout_sec: int = 30):
    await asyncio.sleep(timeout_sec)
    try:
        await bot.delete_message(chat_id, message_id)
    except exceptions.TelegramAPIError:
        pass


@dp.message_handler(commands='spam', chat_type=[ChatType.GROUP, ChatType.SUPERGROUP])
async def mark_spam(message: types.Message):
    sent = await broadcast(message, 'You have been summoned to delete spam.')
    if sent:
        try:
            msg = await message.reply('🔜')
        except exceptions.TelegramAPIError:
            pass
        asyncio.create_task(delete_timeout(msg.chat.id, msg.message_id))
    else:
        await message.answer('Please ask your admins to type /spamme.')


@dp.message_handler(chat_type=[ChatType.GROUP, ChatType.SUPERGROUP],
                    content_types=[ContentType.NEW_CHAT_MEMBERS])
async def test_chat_member(message: types.Message):
    for user in message.new_chat_members:
        if await test_spammer(user):
            await broadcast(message, f'This user is ComBot-detected spammer: {user.full_name}.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    executor.start_polling(dp, skip_updates=True, on_shutdown=shutdown)
