# راه‌اندازی روی Railway

1. این پوشه را در یک ریپوی **private** گیت‌هاب بگذار (همه‌ی فایل‌ها، از جمله `.gitignore`).
2. Railway ← New Project ← Deploy from GitHub repo.
3. تب **Variables** (۵ مقدار):
   - `API_ID` و `API_HASH` ← از https://my.telegram.org
   - `BOT_TOKEN` ← از @BotFather
   - `ADMIN_ID` ← آیدی عددی خودت از @userinfobot
   - `SESSION_STRING` ← خروجی `python gen_session.py` (یک‌بار روی گوشی/کامپیوتر؛ حدود ۳۵۳ کاراکتر، بدون فاصله و کوتیشن)
4. تب **Volumes** ← Mount path: `/data` (بدون این، با هر دیپلوی دیتابیس پاک می‌شود).
5. بعد از دیپلوی، به ربات `/start` بزن.

اختیاری: `DAILY_LIMIT`، `MIN_DELAY`، `MAX_DELAY`، `AUTO_COLLECT_HOURS`، `SENDERS`، `HUBS_PER_RUN`، `TZ_OFFSET` (پیش‌فرض ایران 3.5).

⚠️ فایل `config.json` و `SESSION_STRING` مثل رمز اکانتند؛ جایی منتشر نکن.
