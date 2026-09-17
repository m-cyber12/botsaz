```python
import asyncio
import logging
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


API_BASE = "https://botapi.rubika.ir/v1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("rubika-bot-builder")

app = FastAPI(title="Rubika Bot Builder")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# اطلاعات ربات‌ها در حافظه نگهداری می‌شود
bots: Dict[str, Dict[str, Any]] = {}
tasks: Dict[str, asyncio.Task] = {}


class ConnectRequest(BaseModel):
    token: str


class ConfigRequest(BaseModel):
    token: str
    welcome: str = "سلام! 👋\nبه ربات ما خوش آمدی."
    fallback: str = "پیامت دریافت شد. 🤖"


async def rubika(
    method: str,
    token: str,
    data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:

    url = f"{API_BASE}/{token}/{method}"

    timeout = httpx.Timeout(
        connect=15.0,
        read=35.0,
        write=15.0,
        pool=15.0,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:

        response = await client.post(
            url,
            json=data or {},
        )

        logger.info(
            "Rubika API %s -> HTTP %s",
            method,
            response.status_code,
        )

        # اگر API خطای HTTP داد
        response.raise_for_status()

        result = response.json()

        if not isinstance(result, dict):
            raise RuntimeError(
                "پاسخ API روبیکا معتبر نیست."
            )

        # لاگ پاسخ برای عیب‌یابی
        logger.info(
            "Rubika API %s response: %s",
            method,
            result,
        )

        return result


def get_result(data: Dict[str, Any]) -> Dict[str, Any]:

    result = data.get("result")

    if isinstance(result, dict):
        return result

    return data


def extract_bot_info(
    data: Dict[str, Any],
) -> Dict[str, Any]:

    result = get_result(data)

    bot = result.get("bot")

    if isinstance(bot, dict):
        return bot

    nested = result.get("data")

    if isinstance(nested, dict):

        bot = nested.get("bot")

        if isinstance(bot, dict):
            return bot

    return {}


def extract_updates(
    data: Dict[str, Any],
) -> list:

    result = get_result(data)

    candidates = [
        result.get("updates"),
        data.get("updates"),
    ]

    for container in (
        result.get("data"),
        data.get("data"),
    ):

        if isinstance(container, dict):
            candidates.append(
                container.get("updates")
            )

    for value in candidates:

        if isinstance(value, list):
            return value

    return []


def extract_next_offset(
    data: Dict[str, Any],
) -> Optional[str]:

    result = get_result(data)

    candidates = [
        result.get("next_offset_id"),
        data.get("next_offset_id"),
    ]

    for container in (
        result.get("data"),
        data.get("data"),
    ):

        if isinstance(container, dict):
            candidates.append(
                container.get("next_offset_id")
            )

    for value in candidates:

        if value is not None and str(value) != "":
            return str(value)

    return None


def find_value(
    obj: Any,
    keys: tuple[str, ...],
) -> Any:

    if isinstance(obj, dict):

        # ابتدا کلیدهای موردنظر را بررسی می‌کنیم
        for key in keys:

            if key in obj and obj[key] is not None:
                return obj[key]

        # سپس داخل ساختارهای تو در تو می‌گردیم
        for value in obj.values():

            found = find_value(
                value,
                keys,
            )

            if found is not None:
                return found

    elif isinstance(obj, list):

        for item in obj:

            found = find_value(
                item,
                keys,
            )

            if found is not None:
                return found

    return None


def extract_message(
    update: Dict[str, Any],
) -> tuple[Optional[str], Optional[str]]:

    if not isinstance(update, dict):
        return None, None

    # --------------------------------------------------
    # نوع آپدیت
    # --------------------------------------------------

    update_type = update.get("type")

    if update_type:

        normalized_type = str(
            update_type
        ).lower().replace("-", "_")

        # انواع معمول پیام
        allowed_types = {
            "new_message",
            "newmessage",
            "message",
        }

        if normalized_type not in allowed_types:
            return None, None

    # --------------------------------------------------
    # chat_id
    # --------------------------------------------------

    chat_id = (
        update.get("chat_id")
        or update.get("chatId")
        or update.get("object_guid")
        or update.get("chat_guid")
    )

    # --------------------------------------------------
    # خود پیام
    # --------------------------------------------------

    message = (
        update.get("new_message")
        or update.get("newMessage")
        or update.get("message")
    )

    if not isinstance(message, dict):
        message = {}

    # --------------------------------------------------
    # متن پیام
    # --------------------------------------------------

    text = (
        message.get("text")
        or message.get("message")
        or message.get("body")
    )

    # اگر در سطح بالا بود
    if text is None:

        text = update.get("text")

    # جستجوی عمیق‌تر
    if text is None:

        text = find_value(
            update,
            (
                "text",
                "raw_text",
            ),
        )

    # --------------------------------------------------
    # اگر chat_id پیدا نشد
    # --------------------------------------------------

    if chat_id is None:

        chat_id = find_value(
            update,
            (
                "chat_id",
                "chatId",
                "object_guid",
                "chat_guid",
            ),
        )

    return (
        str(chat_id)
        if chat_id is not None
        else None,

        str(text).strip()
        if text is not None
        else None,
    )


async def send_text(
    token: str,
    chat_id: str,
    text: str,
) -> None:

    logger.info(
        "Sending reply to chat_id=%s: %r",
        chat_id,
        text,
    )

    result = await rubika(
        "sendMessage",
        token,
        {
            "chat_id": chat_id,
            "text": text,
        },
    )

    logger.info(
        "sendMessage result: %s",
        result,
    )


async def poll_bot(
    token: str,
) -> None:

    logger.info(
        "Polling started for bot token ending ...%s",
        token[-6:],
    )

    offset_id: Optional[str] = None

    while True:

        try:

            # --------------------------------------------------
            # اگر ربات از سیستم حذف شده، polling متوقف شود
            # --------------------------------------------------

            if token not in bots:

                logger.info(
                    "Bot no longer exists. Polling stopped."
                )

                return

            # --------------------------------------------------
            # ساخت درخواست getUpdates
            # --------------------------------------------------

            payload: Dict[str, Any] = {
                "limit": 100,
            }

            if offset_id:

                payload["offset_id"] = offset_id

            # --------------------------------------------------
            # دریافت آپدیت‌ها
            # --------------------------------------------------

            data = await rubika(
                "getUpdates",
                token,
                payload,
            )

            # ==================================================
            # نکته مهم:
            #
            # updates باید مستقل از next_offset_id پردازش شود.
            # ==================================================

            updates = extract_updates(data)

            logger.info(
                "Received %d update(s)",
                len(updates),
            )

            # --------------------------------------------------
            # پردازش تمام پیام‌ها
            # --------------------------------------------------

            for update in updates:

                try:

                    logger.info(
                        "RAW UPDATE: %s",
                        update,
                    )

                    chat_id, text = extract_message(
                        update
                    )

                    logger.info(
                        "Parsed message: chat_id=%s text=%r",
                        chat_id,
                        text,
                    )

                    # اگر پیام قابل تشخیص نیست
                    if not chat_id:

                        logger.warning(
                            "chat_id پیدا نشد."
                        )

                        continue

                    if text is None:

                        logger.warning(
                            "text پیدا نشد."
                        )

                        continue

                    # --------------------------------------------------
                    # تنظیمات ربات
                    # --------------------------------------------------

                    config = bots.get(token)

                    if config is None:
                        return

                    # --------------------------------------------------
                    # پاسخ به /start
                    # --------------------------------------------------

                    if text.strip().lower() == "/start":

                        reply = config.get(
                            "welcome",
                            "سلام! 👋\nبه ربات ما خوش آمدی.",
                        )

                    # --------------------------------------------------
                    # پاسخ به پیام‌های عادی
                    # --------------------------------------------------

                    else:

                        reply = config.get(
                            "fallback",
                            "پیامت دریافت شد. 🤖",
                        )

                    # --------------------------------------------------
                    # ارسال پاسخ
                    # --------------------------------------------------

                    await send_text(
                        token,
                        chat_id,
                        reply,
                    )

                    # فاصله کوتاه بین پاسخ‌ها
                    await asyncio.sleep(0.2)

                except Exception:

                    logger.exception(
                        "Error while processing one update"
                    )

            # --------------------------------------------------
            # offset بعدی
            #
            # این قسمت بعد از پردازش updates انجام می‌شود،
            # ولی وابسته به آن نیست.
            # --------------------------------------------------

            next_offset = extract_next_offset(
                data
            )

            if next_offset:

                offset_id = next_offset

                logger.info(
                    "Next offset_id: %s",
                    offset_id,
                )

            # --------------------------------------------------
            # ادامه polling
            # --------------------------------------------------

            await asyncio.sleep(0.5)

        except asyncio.CancelledError:

            logger.info(
                "Polling cancelled"
            )

            raise

        except Exception:

            logger.exception(
                "Polling error; retrying in 5 seconds"
            )

            await asyncio.sleep(5)


@app.get("/health")
async def health():

    return {
        "ok": True,
        "service": "rubika-bot-builder",
    }


@app.post("/connect")
async def connect(
    request: ConnectRequest,
):

    token = request.token.strip()

    if not token:

        raise HTTPException(
            status_code=400,
            detail="توکن وارد نشده است.",
        )

    try:

        # بررسی اتصال به ربات
        data = await rubika(
            "getMe",
            token,
            {},
        )

    except httpx.HTTPStatusError as exc:

        logger.exception(
            "getMe HTTP error"
        )

        raise HTTPException(
            status_code=400,
            detail=(
                f"API روبیکا خطای HTTP "
                f"{exc.response.status_code} داد."
            ),
        )

    except Exception as exc:

        logger.exception(
            "getMe error"
        )

        raise HTTPException(
            status_code=400,
            detail=(
                f"اتصال به API روبیکا ناموفق بود: {exc}"
            ),
        )

    bot_info = extract_bot_info(
        data
    )

    # تنظیمات قبلی حفظ می‌شوند
    old = bots.get(
        token,
        {},
    )

    bots[token] = {

        "token": token,

        "welcome": old.get(
            "welcome",
            "سلام! 👋\nبه ربات ما خوش آمدی.",
        ),

        "fallback": old.get(
            "fallback",
            "پیامت دریافت شد. 🤖",
        ),

        "bot": bot_info,
    }

    # --------------------------------------------------
    # فقط یک polling برای هر ربات
    # --------------------------------------------------

    old_task = tasks.get(
        token
    )

    if old_task is None or old_task.done():

        tasks[token] = asyncio.create_task(
            poll_bot(token)
        )

        logger.info(
            "Polling task created"
        )

    else:

        logger.info(
            "Polling task already running"
        )

    logger.info(
        "Bot connected successfully"
    )

    return {
        "ok": True,
        "message": "ربات با موفقیت متصل شد.",
        "bot": bot_info,
    }


@app.post("/config")
async def config(
    request: ConfigRequest,
):

    token = request.token.strip()

    if not token:

        raise HTTPException(
            status_code=400,
            detail="توکن وارد نشده است.",
        )

    if token not in bots:

        raise HTTPException(
            status_code=400,
            detail="ابتدا ربات را متصل کنید.",
        )

    # حفظ قابلیت تنظیم پیام خوش‌آمدگویی
    bots[token]["welcome"] = request.welcome

    # حفظ قابلیت تنظیم پاسخ پیام‌های عادی
    bots[token]["fallback"] = request.fallback

    logger.info(
        "Bot configuration updated"
    )

    return {
        "ok": True,
        "message": "تنظیمات با موفقیت ذخیره شد.",
    }


@app.post("/disconnect")
async def disconnect(
    request: ConnectRequest,
):

    token = request.token.strip()

    if not token:

        raise HTTPException(
            status_code=400,
            detail="توکن وارد نشده است.",
        )

    task = tasks.pop(
        token,
        None,
    )

    if task is not None and not task.done():

        task.cancel()

        try:

            await task

        except asyncio.CancelledError:

            pass

    bots.pop(
        token,
        None,
    )

    logger.info(
        "Bot disconnected"
    )

    return {
        "ok": True,
        "message": "اتصال ربات قطع شد.",
    }


if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
```
