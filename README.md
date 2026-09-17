# ربات‌ساز روبیکا — نسخه ۱

این پروژه یک نمونه اولیه از «سایت ربات‌ساز» است:

- Frontend: HTML/CSS/JS و قابل انتشار روی GitHub Pages
- Backend: FastAPI/Python
- اتصال ربات با توکن
- بررسی توکن با `getMe`
- دریافت پیام‌ها با polling (`getUpdates`)
- پاسخ به `/start`
- پاسخ پیش‌فرض به پیام‌های دیگر
- تنظیم پیام خوش‌آمد و پاسخ پیش‌فرض

## اجرای بک‌اند روی کامپیوتر

```bash
cd backend
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
# source .venv/bin/activate

pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

سپس `frontend/index.html` را باز کن و آدرس بک‌اند را روی:

`http://localhost:8000`

بگذار.

## استقرار

Frontend را می‌توان روی GitHub Pages گذاشت.

Backend را باید روی یک سرویس اجرای Python/Container مستقر کرد. متغیرهای محیطی و ذخیره امن توکن‌ها برای نسخه واقعی باید اضافه شوند.

## نکات امنیتی

- توکن را داخل JavaScript یا GitHub ذخیره نکن.
- برای نسخه عمومی، احراز هویت کاربر و دیتابیس اضافه کن.
- CORS را از `*` به دامنه واقعی سایت محدود کن.
- توکن‌ها را رمزنگاری/Secret-store کن.
- برای هر ربات یک worker جدا یا صف وظایف داشته باش.
- قبل از استفاده عمومی، schema دقیق پاسخ‌های API روبیکا را با مستندات فعلی حساب رباتت تطبیق بده.

## API

این نمونه بر اساس مستندات عمومی موجود برای `getMe`, `sendMessage` و `getUpdates` نوشته شده است. اگر API روبیکا تغییر کرده باشد، بخش `rubika()` و parser آپدیت‌ها باید اصلاح شود.
