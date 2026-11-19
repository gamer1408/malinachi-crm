import random
﻿import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

from database import (
    clear_admin_state,
    get_admin_state,
    get_due_followups,
    get_session,
    init_db,
    is_ig_comment_processed,
    is_ig_user_recently_messaged,
    mark_followup_sent,
    mark_ig_comment_processed,
    set_admin_state,
    upsert_session,
)
from sheets_crm import crm_sheets

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("MalinachiCRM")

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
ADMIN_CHAT_ID = str(os.getenv("ADMIN_CHAT_ID", "")).strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")

IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "")
IG_BUSINESS_ACCOUNT_ID = os.getenv("IG_BUSINESS_ACCOUNT_ID", "")
IG_VERIFY_TOKEN = os.getenv("IG_VERIFY_TOKEN", "malinachi_angren_2026_secret")
IG_GRAPH_URL = "https://graph.instagram.com/v21.0"

if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN .env faylida topilmadi!")

bot = Bot(token=TG_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Yangi kommentlar uchun 5 soniyalik navbat
ig_comment_queue: asyncio.Queue = asyncio.Queue()


# =====================================================================
# YORDAMCHI BIZNES MANTIQLAR (SEGMENTATSIYA, TELEFON, IG GRAPH API)
# =====================================================================

def classify_volume(text: str) -> Tuple[str, str]:
    clean = text.strip()
    if clean == "1":
        return "100-300 ta", "Tomorqa"
    if clean == "2":
        return "500-2000 ta", "Biznes"
    if clean == "3":
        return "3000+ ta", "VIP_Ulgurji"

    low = clean.lower()
    numbers = [int(n) for n in re.findall(r"\d+", low.replace(" ", ""))]
    max_num = max(numbers) if numbers else 0

    if any(k in low for k in ("gektar", "гектар", "ga", "га", "ming", "минг")):
        segment = "VIP_Ulgurji" if (max_num >= 3 or "gektar" in low or "гектар" in low or "ga" in low or "га" in low) else "Biznes"
    elif any(k in low for k in ("sotix", "сотих", "sotih", "sotik", "сотик")):
        segment = "VIP_Ulgurji" if max_num >= 20 else ("Biznes" if max_num >= 4 else "Tomorqa")
    elif max_num >= 3000:
        segment = "VIP_Ulgurji"
    elif max_num >= 500:
        segment = "Biznes"
    else:
        segment = "Tomorqa"

    return clean, segment


def normalize_uz_phone(text: str) -> Optional[str]:
    digits = re.sub(r"\D", "", text)
    if len(digits) == 9 and digits[0] in ("9", "8", "7", "5", "3", "2"):
        return f"+998{digits}"
    if len(digits) == 12 and digits.startswith("998"):
        return f"+{digits}"
    return None


async def ig_get_user_profile(igsid: str) -> Tuple[str, str]:
    """Unikal user_id orqali foydalanuvchining username va ismini olish."""
    if not IG_ACCESS_TOKEN or not igsid:
        return "", f"ID:{igsid}"
    url = f"{IG_GRAPH_URL}/{igsid}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                url,
                params={"fields": "name,username", "access_token": IG_ACCESS_TOKEN},
            )
            if resp.status_code == 200:
                data = resp.json()
                uname = str(data.get("username", ""))
                fname = str(data.get("name", "")) or uname or f"ID:{igsid}"
                return uname, fname
    except Exception as exc:
        logger.warning("IG user profile olishda xato (%s): %s", igsid, exc)
    return "", f"ID:{igsid}"


_IN_FLIGHT_COMMENTS: set[str] = set()


async def ig_reply_to_comment(comment_id: str, message_text: str) -> bool:
    """Instagram post/Reels izohi ostiga ommaviy javob yozish."""
    if not IG_ACCESS_TOKEN or not message_text:
        return False
    url = f"{IG_GRAPH_URL}/{comment_id}/replies"
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            url,
            params={"access_token": IG_ACCESS_TOKEN},
            json={"message": message_text},
        )
        if resp.status_code != 200:
            logger.error("IG comment reply xatosi: %s -> %s", resp.status_code, resp.text)
            return False
        return True


async def ig_send_private_reply(comment_id: str, message_text: str) -> Tuple[bool, str]:
    """Izoh yozgan foydalanuvchiga Instagram Private Reply (DM) yuborish."""
    if not IG_ACCESS_TOKEN or not message_text:
        return False, "no_token"
    url = f"{IG_GRAPH_URL}/me/messages"
    payload = {
        "recipient": {"comment_id": comment_id},
        "message": {"text": message_text},
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            url,
            params={"access_token": IG_ACCESS_TOKEN},
            json=payload,
        )
        if resp.status_code != 200:
            err_text = resp.text
            logger.error("IG Private Reply xatosi: %s -> %s", resp.status_code, err_text)
            return False, err_text
        return True, "ok"


async def ig_send_dm(
    recipient_igsid: str,
    message_text: str,
    quick_replies: Optional[List[str]] = None,
) -> bool:
    """Instagram Direct ichida xabar (va ixtiyoriy Quick Reply tugmalari) yuborish."""
    if not IG_ACCESS_TOKEN or not message_text:
        return False
    url = f"{IG_GRAPH_URL}/me/messages"
    msg_body: Dict[str, Any] = {"text": message_text}

    if quick_replies:
        msg_body["quick_replies"] = [
            {
                "content_type": "text",
                "title": title[:20],
                "payload": title.upper(),
            }
            for title in quick_replies
        ]

    payload = {
        "recipient": {"id": recipient_igsid},
        "message": msg_body,
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            url,
            params={"access_token": IG_ACCESS_TOKEN},
            json=payload,
        )
        if resp.status_code != 200:
            logger.error("IG DM yuborish xatosi: %s -> %s", resp.status_code, resp.text)
            return False
        return True


_MEDIA_PERMALINK_CACHE: Dict[str, str] = {}


async def get_ig_media_permalink(media_id: str) -> str:
    if not media_id or not IG_ACCESS_TOKEN:
        return ""
    if media_id in _MEDIA_PERMALINK_CACHE:
        return _MEDIA_PERMALINK_CACHE[media_id]
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{IG_GRAPH_URL}/{media_id}",
                params={"fields": "permalink", "access_token": IG_ACCESS_TOKEN},
            )
            if resp.status_code == 200:
                link = resp.json().get("permalink", "")
                if link:
                    _MEDIA_PERMALINK_CACHE[media_id] = link
                    return link
    except Exception as exc:
        logger.warning("Media permalink olishda xato (%s): %s", media_id, exc)
    return ""


async def send_initial_comment_dm(
    comment_id: str,
    commenter_id: str,
    commenter_username: str,
    comment_text: str,
    media_permalink: str = "",
) -> None:
    """
    Google Sheets (Reels_KalitSozlar) qoidalari asosida:
    - Kommentga yoki userga allaqachon yozilgan bo'lsa o'tkazib yuboradi.
    - Reels_Link va Kalit_Soz mos kelsagina javob beradi.
    - Javob_1_Samimiy, Javob_2_Rasmiy, Javob_3_Dehqoncha dan tasodifiy bittasini tanlaydi.
    """
    if comment_id in _IN_FLIGHT_COMMENTS or await is_ig_comment_processed(comment_id):
        return
    _IN_FLIGHT_COMMENTS.add(comment_id)

    if await is_ig_user_recently_messaged(commenter_id):
        await mark_ig_comment_processed(comment_id, commenter_id, commenter_username)
        return

    kw_match = await crm_sheets.get_keyword_data(comment_text, media_permalink=media_permalink)
    if not kw_match:
        await mark_ig_comment_processed(comment_id, commenter_id, commenter_username)
        return

    matched_kw, kw_data = kw_match
    intro_text = kw_data.get("dm_text", "").strip() or (
        "Assalomu alaykum! 🌱 Angrendagi «Malinachi_» ko‘chatxonasiga xush kelibsiz (Atlant va Maravilla navlari)."
    )
    sheet_replies = kw_data.get("replies", [])
    default_variants = [
        "Directingizga batafsil ma'lumot va narxlarni yubordik, bir ko‘rib chiqing 😊📩",
        "So‘rovingiz bo‘yicha narxlar va ekish tartibini Directingizga yubordik 📩",
        "Lichkangizga narx-navo va ko‘chat haqida hamma ma'lumotni tashlab qo‘ydik, aka 🤝🌱",
    ]
    public_reply = random.choice(sheet_replies if sheet_replies else default_variants)

    q1 = await crm_sheets.get_funnel_text("QADAM_1_HAJM")
    full_dm = f"{intro_text}\n\n{q1}"
    dm_ok, dm_err = await ig_send_private_reply(comment_id, full_dm)

    if dm_ok:
        if public_reply:
            await ig_reply_to_comment(comment_id, public_reply)
        if commenter_id:
            await upsert_session(
                platform="IG_DM",
                user_id=commenter_id,
                username=commenter_username,
                full_name=commenter_username or f"ID:{commenter_id}",
                step="WAIT_VOLUME",
                keyword=matched_kw,
                schedule_followup_hours=23.0,
            )
    else:
        err_low = str(dm_err).lower()
        if "already" not in err_low and "duplicate" not in err_low and "10903" not in err_low:
            fallback_comment = (
                "Assalomu alaykum! 🌱 Ko‘chatlar narxi va ekish bo‘yicha batafsil ma’lumot olish uchun "
                "Directimizga «HA» deb yozib yuboring, mutaxassisimiz darhol javob beradi 📩"
            )
            await ig_reply_to_comment(comment_id, fallback_comment)

    # 1.1: Har qanday holatda ham kommentni "ko'rildi" deb belgilaymiz
    await mark_ig_comment_processed(comment_id, commenter_id, commenter_username)


def build_admin_lead_keyboard(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📞 Aloqaga chiqildi",
                    callback_data=f"crm_status:{lead_id}:Aloqaga_chiqildi",
                ),
                InlineKeyboardButton(
                    text="💰 Zaklad olindi",
                    callback_data=f"crm_deposit:{lead_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="❌ Rad etildi",
                    callback_data=f"crm_status:{lead_id}:Rad_etildi",
                )
            ],
        ]
    )


async def notify_admin_new_lead(
    lead_id: str,
    source: str,
    username_or_id: str,
    full_name: str,
    phone: str,
    region: str,
    volume_text: str,
    segment: str,
    keyword: str,
) -> None:
    if not ADMIN_CHAT_ID:
        return

    vip_banner = ""
    if segment == "VIP_Ulgurji":
        vip_banner = "🔥🔥🔥 <b>DIQQAT: VIP ULGURJI MIJOZ!</b> 🔥🔥🔥\n\n"

    text = (
        f"{vip_banner}"
        f"🌱 <b>YANGI LID — {lead_id}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 <b>Manba:</b> {source}\n"
        f"👤 <b>Mijoz:</b> {full_name} ({username_or_id})\n"
        f"📞 <b>Telefon:</b> <code>{phone}</code>\n"
        f"📍 <b>Hudud:</b> {region}\n"
        f"🌿 <b>Hajm:</b> {volume_text}\n"
        f"🏷 <b>Segment:</b> <b>{segment}</b>\n"
        f"рџ”‘ <b>Kalit so'z:</b> {keyword or 'ToвЂgвЂridan-toвЂgвЂri'}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Holatni boshqarish uchun tugmani bosing:</i>"
    )

    try:
        await bot.send_message(
            chat_id=int(ADMIN_CHAT_ID),
            text=text,
            reply_markup=build_admin_lead_keyboard(lead_id),
        )
    except Exception as exc:
        logger.error("Admin Telegramga xabar yuborishda xato: %s", exc)


# =====================================================================
# UMUMIY VORONKA MANTIQI + OXIRIDA TASDIQLASH LOOPI (1.4 & 2.2)
# =====================================================================

async def process_funnel_message(
    platform: str,
    user_id: str,
    username: str,
    full_name: str,
    text: str,
) -> Tuple[str, Optional[Any], Optional[List[str]]]:
    """
    Qaytaradi: (javob_matni, telegram_keyboard, ig_quick_replies)
    Bosqichlar:
    0) IDLE -> Salomlashuv + "✅ Ha" tugmasi (IG DM uchun)
    1) WAIT_START_CONFIRM -> "✅ Ha" bossa QADAM_1_HAJM ga o'tadi
    2) WAIT_VOLUME -> Hajm va Segment
    3) WAIT_REGION -> Viloyat/tuman
    4) WAIT_PHONE -> Telefon raqam -> Tasdiqlash so'rovi (WAIT_CONFIRM)
    5) WAIT_CONFIRM -> Tasdiqlasa CRM ga yozadi, tasdiqlamasa boshidan boshlaydi (1.4)
    """
    session = await get_session(platform, user_id)
    step = session["step"] if session else "IDLE"
    clean_lower = text.strip().lower()

    # 0-BOSQICH: YANGI SUHBAT (IG DM uchun "✅ Ha" tugmasi bilan kutib olish - 2.2)
    if step == "IDLE":
        if platform == "IG_DM" and clean_lower not in ("ha", "✅ ha", "yes"):
            await upsert_session(
                platform=platform,
                user_id=user_id,
                username=username,
                full_name=full_name,
                step="WAIT_START_CONFIRM",
                keyword="Direct_Kirish",
                schedule_followup_hours=23.0,
            )
            welcome_dm = (
                "Assalomu alaykum, xush kelibsiz! 🌱\n\nBu «Malinachi_» (Angren ko‘chatzor) rasmiy sahifasi. Atlant va Maravilla malina ko‘chatlari haqida ma’lumot olishni istasangiz, pastdagi «✅ Ha» tugmasini bosing 👇"
            )
            return welcome_dm, None, ["✅ Ha"]

        # Telegram yoki darhol "✅ Ha" deb yozganlar uchun 1-savolga o'tish
        step = "WAIT_START_CONFIRM"

    # 0.5-BOSQICH: "HA" TUGMASI BOSILGANDA 1-SAVOLNI BERISH
    if step == "WAIT_START_CONFIRM":
        q1 = await crm_sheets.get_funnel_text("QADAM_1_HAJM")
        await upsert_session(
            platform=platform,
            user_id=user_id,
            username=username,
            full_name=full_name,
            step="WAIT_VOLUME",
            keyword=session["keyword"] if session and session.get("keyword") else "Direct_Kirish",
            schedule_followup_hours=23.0,
        )
        kb = None
        if platform == "TG_Bot":
            kb = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="1️⃣ 100 – 300 ta (Tomorqa)")],
                    [KeyboardButton(text="2️⃣ 500 – 2000 ta (Biznes)")],
                    [KeyboardButton(text="3пёЏвѓЈ 3000+ ta (VIP Ulgurji)")],
                ],
                resize_keyboard=True,
                one_time_keyboard=True,
            )
        return q1, kb, None

    # 1-BOSQICH: HAJM
    if step == "WAIT_VOLUME":
        vol_text, segment = classify_volume(text)
        q2 = await crm_sheets.get_funnel_text("QADAM_2_MANZIL")
        await upsert_session(
            platform=platform,
            user_id=user_id,
            username=username,
            full_name=full_name,
            step="WAIT_REGION",
            volume_text=vol_text,
            segment=segment,
            schedule_followup_hours=23.0,
        )
        return q2, ReplyKeyboardRemove() if platform == "TG_Bot" else None, None

    # 2-BOSQICH: VILOYAT / TUMAN
    if step == "WAIT_REGION":
        region = text.strip()
        if len(region) < 2:
            return "Iltimos, viloyat va tuman nomini toвЂliqroq yozing (masalan: FargвЂona, Quva tumani):", None, None

        q3 = await crm_sheets.get_funnel_text("QADAM_3_TELEFON")
        await upsert_session(
            platform=platform,
            user_id=user_id,
            username=username,
            full_name=full_name,
            step="WAIT_PHONE",
            region=region,
            schedule_followup_hours=23.0,
        )

        kb = None
        if platform == "TG_Bot":
            kb = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="📱 Telefon raqamni yuborish", request_contact=True)]],
                resize_keyboard=True,
                one_time_keyboard=True,
            )
        return q3, kb, None

    # 3-BOSQICH: TELEFON RAQAM -> TASDIQLASHGA O'TKAZISH (1.4)
    if step == "WAIT_PHONE":
        phone = normalize_uz_phone(text)
        if not phone:
            return (
                "Telefon raqam formati biroz noto‘g‘ri kiritildi 🙏\n"
                "Iltimos, 9 ta raqam yoki +998 bilan yozing (Masalan: +998901234567):",
                None,
                None,
            )

        updated = await upsert_session(
            platform=platform,
            user_id=user_id,
            username=username,
            full_name=full_name,
            step="WAIT_CONFIRM",
            phone=phone,
            schedule_followup_hours=23.0,
        )

        confirm_text = (
            f"📋 Ma'lumotlaringizni tekshirib oling:\n\n🌱 Ko'chat hajmi: {updated.get('volume_text', '-')}\n📍 Hudud: {updated.get('region', '-')}\n📞 Telefon: {phone}\n\nBarchasi to'g'rimi? Tasdiqlaysizmi?"
        )
        tg_kb = None
        if platform == "TG_Bot":
            tg_kb = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ Ha, to'g'ri"), KeyboardButton(text="🔄 Yo'q, qaytadan")]],
                resize_keyboard=True,
                one_time_keyboard=True,
            )
        return confirm_text, tg_kb, ["✅ Ha", "🔄 Yo‘q"]

    # 4-BOSQICH: TASDIQLASH LOOPI (1.4)
    if step == "WAIT_CONFIRM":
        if any(w in clean_lower for w in ("ha", "yes", "to'g'ri", "togri", "tasdiqlayman")):
            updated = await upsert_session(
                platform=platform,
                user_id=user_id,
                username=username,
                full_name=full_name,
                step="COMPLETED",
                cancel_followup=True,
            )

            display_user = f"@{username}" if username else f"ID:{user_id}"
            lead_id = await crm_sheets.add_lead(
                source=platform,
                username_or_id=display_user,
                full_name=full_name or display_user,
                phone=updated["phone"],
                region=updated["region"],
                seedling_count=updated["volume_text"],
                segment=updated["segment"],
                keyword=updated["keyword"],
            )

            await notify_admin_new_lead(
                lead_id=lead_id,
                source=platform,
                username_or_id=display_user,
                full_name=full_name or display_user,
                phone=updated["phone"],
                region=updated["region"],
                volume_text=updated["volume_text"],
                segment=updated["segment"],
                keyword=updated["keyword"],
            )

            final_msg = (
                "Rahmat! ✅ Ma'lumotlaringiz muvaffaqiyatli qabul qilindi.\n\nMutaxassisimiz tez orada siz bilan bog'lanib, ekish sxemasi va narxlar bo'yicha batafsil ma'lumot beradi 🌱"
            )
            return final_msg, ReplyKeyboardRemove() if platform == "TG_Bot" else None, None

        else:
            # Tasdiqlamasa -> Qaytadan 1-bosqichdan boshlaydi!
            q1 = await crm_sheets.get_funnel_text("QADAM_1_HAJM")
            await upsert_session(
                platform=platform,
                user_id=user_id,
                username=username,
                full_name=full_name,
                step="WAIT_VOLUME",
                schedule_followup_hours=23.0,
            )
            tg_kb = None
            if platform == "TG_Bot":
                tg_kb = ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="1️⃣ 100 – 300 ta (Tomorqa)")],
                        [KeyboardButton(text="2️⃣ 500 – 2000 ta (Biznes)")],
                        [KeyboardButton(text="3пёЏвѓЈ 3000+ ta (VIP Ulgurji)")],
                    ],
                    resize_keyboard=True,
                    one_time_keyboard=True,
                )
            return f"Tushunarli, ma'lumotlarni qaytadan kiritamiz 🔄\n\n{q1}", tg_kb, None

    # COMPLETED holatidan keyin yana yozsa -> Loop qilib qaytadan boshlash taklifi
    if any(w in clean_lower for w in ("ha", "malina", "qaytadan", "yana")):
        q1 = await crm_sheets.get_funnel_text("QADAM_1_HAJM")
        await upsert_session(
            platform=platform,
            user_id=user_id,
            username=username,
            full_name=full_name,
            step="WAIT_VOLUME",
            schedule_followup_hours=23.0,
        )
        return f"Yangi so‘rov boshlaymiz 🌱\n\n{q1}", None, None

    return (
        "Sizning so‘rovingiz bazamizga qabul qilingan ✅ Mutaxassisimiz tez orada bog‘lanadi.\n"
        "Yangi hisob-kitob boshlash uchun «Ha» deb yozing.",
        None,
        ["✅ Ha"],
    )


# =====================================================================
# TELEGRAM BOT HANDLERLARI
# =====================================================================

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user = message.from_user
    if not user:
        return
    await upsert_session(
        platform="TG_Bot",
        user_id=str(user.id),
        username=user.username or "",
        full_name=user.full_name,
        step="IDLE",
    )
    reply_text, kb, _ = await process_funnel_message(
        platform="TG_Bot",
        user_id=str(user.id),
        username=user.username or "",
        full_name=user.full_name,
        text="ha",
    )
    await message.answer(reply_text, reply_markup=kb)


@router.message(Command("stat"))
async def cmd_stat(message: Message) -> None:
    if not message.from_user or str(message.from_user.id) != ADMIN_CHAT_ID:
        return
    wait_msg = await message.answer("вЏі Google Sheets statistikasi yuklanmoqda...")
    try:
        st = await crm_sheets.get_stats()
        report = (
            "📊 <b>MALINACHI_ — SOTUV VA CRM HISOBOTI</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 <b>Jami lidlar:</b> {st['total_leads']} ta\n"
            f"🆕 <b>Bugungi lidlar:</b> {st['today_leads']} ta\n"
            f"🔥 <b>VIP Ulgurji (3000+):</b> {st['vip_leads']} ta\n"
            f"рџ’° <b>Zaklad toвЂlaganlar:</b> {st['paid_leads']} ta\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"рџЊ± <b>Band qilingan koвЂchatlar:</b> {st['total_qty']:,} dona\n"
            f"рџ’µ <b>Tushgan zaklad:</b> {st['total_deposit']:,} soвЂm\n"
            f"вЏі <b>Kutilayotgan qoldiq:</b> {st['total_remaining']:,} soвЂm\n"
            f"рџ“€ <b>Jami shartnoma summasi:</b> {st['total_contract']:,} soвЂm"
        ).replace(",", " ")
        await wait_msg.edit_text(report)
    except Exception as exc:
        logger.error("Statistika xatosi: %s", exc)
        await wait_msg.edit_text(f"❌ Statistikani olishda xatolik: {exc}")


@router.callback_query(F.data.startswith("crm_status:"))
async def cb_update_status(callback: CallbackQuery) -> None:
    if not callback.from_user or str(callback.from_user.id) != ADMIN_CHAT_ID:
        await callback.answer("Ruxsat yo'q!", show_alert=True)
        return
    _, lead_id, new_status = callback.data.split(":", 2)
    await callback.answer("Yangilanmoqda...")
    ok = await crm_sheets.update_lead_status(lead_id, new_status)
    if ok and callback.message:
        old_text = callback.message.html_text or ""
        await callback.message.edit_text(
            f"{old_text}\n\n✅ <b>Status yangilandi:</b> <code>{new_status}</code>",
            reply_markup=build_admin_lead_keyboard(lead_id),
        )
    else:
        await callback.answer("Lid ID topilmadi!", show_alert=True)


@router.callback_query(F.data.startswith("crm_deposit:"))
async def cb_start_deposit(callback: CallbackQuery) -> None:
    if not callback.from_user or str(callback.from_user.id) != ADMIN_CHAT_ID:
        await callback.answer("Ruxsat yo'q!", show_alert=True)
        return
    _, lead_id = callback.data.split(":", 1)
    await set_admin_state(
        admin_id=str(callback.from_user.id),
        action="WAIT_DEPOSIT_INPUT",
        payload={"lead_id": lead_id},
    )
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            f"💰 <b>{lead_id}</b> uchun zaklad ma'lumotlarini quyidagi formatda yuboring:\n"
            "<code>ko'chat_soni zaklad_summasi [nav]</code>\n\n"
            "<i>Masalan:</i> <code>1000 5000000 Atlant</code>\n"
            "<i>(Bekor qilish uchun: /cancel)</i>"
        )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message) -> None:
    if message.from_user and str(message.from_user.id) == ADMIN_CHAT_ID:
        await clear_admin_state(str(message.from_user.id))
        await message.answer("Amal bekor qilindi ✅")


@router.message(F.contact)
async def handle_tg_contact(message: Message) -> None:
    if not message.from_user or not message.contact:
        return
    reply_text, kb, _ = await process_funnel_message(
        platform="TG_Bot",
        user_id=str(message.from_user.id),
        username=message.from_user.username or "",
        full_name=message.from_user.full_name,
        text=message.contact.phone_number,
    )
    await message.answer(reply_text, reply_markup=kb)


@router.message(F.text)
async def handle_tg_text(message: Message) -> None:
    if not message.from_user or not message.text:
        return
    user_id = str(message.from_user.id)

    if user_id == ADMIN_CHAT_ID:
        admin_st = await get_admin_state(user_id)
        if admin_st["action"] == "WAIT_DEPOSIT_INPUT":
            parts = message.text.strip().split()
            if len(parts) < 2 or not parts[0].isdigit() or not parts[1].isdigit():
                await message.answer(
                    "вљ пёЏ Format noto'g'ri. Iltimos, avval ko'chat soni, keyin zaklad summasini yozing.\n"
                    "Masalan: <code>1500 7000000 Maravilla</code>"
                )
                return

            qty = int(parts[0])
            deposit_sum = int(parts[1])
            variety = parts[2].capitalize() if len(parts) >= 3 else "Atlant"
            lead_id = admin_st["payload"].get("lead_id", "")

            wait_m = await message.answer("вЏі Zaklad va moliyaviy jadvalga yozilmoqda...")
            res = await crm_sheets.record_deposit(
                lead_id=lead_id,
                seedling_qty=qty,
                deposit_amount=deposit_sum,
                variety=variety,
            )
            await clear_admin_state(user_id)

            if not res:
                await wait_m.edit_text(f"❌ {lead_id} raqamli lid topilmadi.")
                return

            msg_ok = (
                f"✅ <b>ZAKLAD MUVAFFAQIYATLI RASMIYLASHTIRILDI!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🧾 <b>Buyurtma ID:</b> {res['order_id']} ({res['lead_id']})\n"
                f"👤 <b>Mijoz:</b> {res['client_name']} ({res['phone']})\n"
                f"📍 <b>Hudud:</b> {res['region']}\n"
                f"🌱 <b>Nav va Soni:</b> {variety} — {res['qty']:,} dona\n"
                f"рџ’µ <b>Jami summa:</b> {res['total_amount']:,} soвЂm\n"
                f"рџ’° <b>ToвЂlangan zaklad:</b> {res['deposit_amount']:,} soвЂm\n"
                f"вЏі <b>Qoldiq summa:</b> {res['remaining_amount']:,} soвЂm\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Ma'lumotlar ham Lidlar_CRM, ham Zaklad_Va_Buyurtmalar varagвЂiga yozildi.</i>"
            ).replace(",", " ")
            await wait_m.edit_text(msg_ok)
            return

    reply_text, kb, _ = await process_funnel_message(
        platform="TG_Bot",
        user_id=user_id,
        username=message.from_user.username or "",
        full_name=message.from_user.full_name,
        text=message.text,
    )
    await message.answer(reply_text, reply_markup=kb)


# =====================================================================
# FON WORKERLAR (23H FOLLOW-UP, 5S NAVBAT, 10M ESKI KOMMENTLAR)
# =====================================================================

async def followup_background_worker() -> None:
    while True:
        try:
            due_users = await get_due_followups()
            if due_users:
                followup_text = await crm_sheets.get_funnel_text("FOLLOWUP_23SOAT")
                for u in due_users:
                    platform = u["platform"]
                    uid = u["user_id"]
                    sent = False
                    if platform == "IG_DM":
                        sent = await ig_send_dm(uid, followup_text)
                    elif platform == "TG_Bot":
                        try:
                            await bot.send_message(chat_id=int(uid), text=followup_text)
                            sent = True
                        except Exception as e:
                            logger.error("TG follow-up xatosi (%s): %s", uid, e)

                    await mark_followup_sent(platform, uid)
                    logger.info("23h Follow-up yuborildi: %s | %s | sent=%s", platform, uid, sent)
        except Exception as exc:
            logger.error("Follow-up worker xatosi: %s", exc)

        await asyncio.sleep(300)


async def ig_comment_queue_worker() -> None:
    """Yangi kommentlarga navbatma-navbat, orasida 5 soniya kutib javob beradi."""
    while True:
        item = await ig_comment_queue.get()
        try:
            await send_initial_comment_dm(
                comment_id=item["comment_id"],
                commenter_id=item["commenter_id"],
                commenter_username=item["commenter_username"],
                comment_text=item["comment_text"],
                media_permalink=item.get("media_permalink", ""),
            )
        except Exception as exc:
            logger.error("IG Comment Queue worker xatosi: %s", exc)
        finally:
            ig_comment_queue.task_done()
            await asyncio.sleep(5)


async def old_comments_background_worker() -> None:
    """
    3.2: Har 10 daqiqada oxirgi 5 ta Reels/Post ostidagi kommentlarni tekshirib,
    hali ko'rilmagan FAQAT 1 TA eski kommentga javob yozadi va to'xtaydi.
    """
    while True:
        await asyncio.sleep(600)
        if not IG_ACCESS_TOKEN:
            continue

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                # 1. Oxirgi 5 ta post/Reels ID larini olamiz
                media_resp = await client.get(
                    f"{IG_GRAPH_URL}/me/media",
                    params={"fields": "id,permalink", "limit": 5, "access_token": IG_ACCESS_TOKEN},
                )
                if media_resp.status_code != 200:
                    logger.error("Media ro'yxatini olishda xato: %s", media_resp.text)
                    continue

                media_list = media_resp.json().get("data", [])
                answered_one = False

                for media in media_list:
                    if answered_one:
                        break
                    m_id = media.get("id")
                    m_permalink = media.get("permalink", "")
                    c_resp = await client.get(
                        f"{IG_GRAPH_URL}/{m_id}/comments",
                        params={"fields": "id,text,from", "limit": 50, "access_token": IG_ACCESS_TOKEN},
                    )
                    if c_resp.status_code != 200:
                        continue

                    for c in c_resp.json().get("data", []):
                        c_id = str(c.get("id", ""))
                        c_text = str(c.get("text", "")).strip()
                        sender = c.get("from", {})
                        u_id = str(sender.get("id", ""))
                        u_name = str(sender.get("username", ""))

                        if not c_id or not u_id or u_id == IG_BUSINESS_ACCOUNT_ID:
                            continue

                        if not await is_ig_comment_processed(c_id):
                            logger.info("Eski kommentga javob yuborilmoqda: @%s (%s)", u_name, c_id)
                            await send_initial_comment_dm(c_id, u_id, u_name, c_text)
                            answered_one = True
                            break
        except Exception as exc:
            logger.error("Old comments worker xatosi: %s", exc)


# =====================================================================
# FASTAPI LIFESPAN VA WEBHOOK ENDPOINTLAR
# =====================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await crm_sheets.initialize()

    if WEBHOOK_BASE_URL:
        tg_webhook_url = f"{WEBHOOK_BASE_URL}/webhook/telegram"
        try:
            await bot.set_webhook(url=tg_webhook_url, drop_pending_updates=True, request_timeout=15)
        except Exception as e:
            logging.warning(f"Telegram set_webhook timeout/xatolik (server ishlashda davom etadi): {e}")
        logger.info("Telegram Webhook o'rnatildi: %s", tg_webhook_url)

    worker_followup = asyncio.create_task(followup_background_worker())
    worker_ig_queue = asyncio.create_task(ig_comment_queue_worker())
    worker_old_comments = asyncio.create_task(old_comments_background_worker())
    yield
    worker_followup.cancel()
    worker_ig_queue.cancel()
    worker_old_comments.cancel()
    await bot.session.close()


app = FastAPI(title="Malinachi_ Omnichannel CRM", lifespan=lifespan)


@app.get("/health")
async def health_check() -> Dict[str, str]:
    return {"status": "ok", "service": "Malinachi_ CRM"}


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request) -> Dict[str, bool]:
    data = await request.json()
    update = Update.model_validate(data, context={"bot": bot})
    await dp.feed_update(bot, update)
    return {"ok": True}


@app.get("/webhook/instagram")
async def instagram_verify_webhook(
    hub_mode: str = Query(default="", alias="hub.mode"),
    hub_verify_token: str = Query(default="", alias="hub.verify_token"),
    hub_challenge: str = Query(default="", alias="hub.challenge"),
) -> PlainTextResponse:
    if hub_mode == "subscribe" and hub_verify_token == IG_VERIFY_TOKEN:
        logger.info("Instagram Webhook muvaffaqiyatli tasdiqlandi!")
        return PlainTextResponse(content=hub_challenge, status_code=200)
    raise HTTPException(status_code=403, detail="Verify token noto'g'ri")


@app.post("/webhook/instagram")
async def instagram_event_webhook(request: Request) -> Dict[str, str]:
    body: Dict[str, Any] = await request.json()
    if body.get("object") != "instagram":
        return {"status": "ignored"}

    for entry in body.get("entry", []):
        # 1. INSTAGRAM DIRECT MESSAGES
        for msg_event in entry.get("messaging", []):
            message_obj = msg_event.get("message", {})
            if message_obj.get("is_echo") or "text" not in message_obj:
                continue

            sender_id = str(msg_event.get("sender", {}).get("id", ""))
            text = str(message_obj.get("text", "")).strip()
            if not sender_id or not text:
                continue

            # 1.3: Unikal user_id orqali profil nomi/username ni tekshirish
            existing_session = await get_session("IG_DM", sender_id)
            if existing_session and existing_session.get("username"):
                uname = existing_session["username"]
                fname = existing_session["full_name"]
            else:
                uname, fname = await ig_get_user_profile(sender_id)

            reply_text, _, quick_replies = await process_funnel_message(
                platform="IG_DM",
                user_id=sender_id,
                username=uname,
                full_name=fname,
                text=text,
            )
            await ig_send_dm(sender_id, reply_text, quick_replies=quick_replies)

        # 2. INSTAGRAM COMMENTS (5 soniyalik navbatga tushadi)
        for change in entry.get("changes", []):
            if change.get("field") != "comments":
                continue
            val = change.get("value", {})
            comment_id = str(val.get("id", ""))
            comment_text = str(val.get("text", "")).strip()
            from_info = val.get("from", {})
            commenter_id = str(from_info.get("id", ""))
            commenter_username = str(from_info.get("username", ""))

            if not comment_id or not comment_text or commenter_id == IG_BUSINESS_ACCOUNT_ID:
                continue

            media_obj = val.get("media") or {}
            media_id = str(media_obj.get("id", ""))
            media_permalink = await get_ig_media_permalink(media_id) if media_id else ""
            await ig_comment_queue.put({
                "comment_id": comment_id,
                "commenter_id": commenter_id,
                "commenter_username": commenter_username,
                "comment_text": comment_text,
                "media_permalink": media_permalink,
            })

    return {"status": "received"}
