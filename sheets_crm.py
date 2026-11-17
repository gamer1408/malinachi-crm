import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_HEADERS = {
    "Lidlar_CRM": [
        "ID",
        "Sana_Vaqt",
        "Manba",
        "Username_yoki_ID",
        "Ism",
        "Telefon",
        "Viloyat_Tuman",
        "Kocat_Soni",
        "Segment",
        "Qaysi_Reels_KalitSoz",
        "Voronka_Bosqichi",
        "Umumiy_Summa",
        "Zaklad_Avans_Summa",
        "Qoldiq_Summa",
        "Izoh_Va_Eslatma",
    ],
    "Zaklad_Va_Buyurtmalar": [
        "Buyurtma_ID",
        "Sana",
        "Mijoz_Ismi",
        "Telefon",
        "Viloyat",
        "Nav",
        "Soni_Dona",
        "Narxi_1_Dona",
        "Jami_Summa",
        "Zaklad_Tolandi",
        "Qoldiq_Pul",
        "Tolov_Usuli",
        "Yuklash_Sana",
        "Yetkazish_Holati",
    ],
    "Reels_KalitSozlar": [
        "Kalit_Soz",
        "Izohga_Javob_Matni",
        "DM_1_Xabar_Va_Qollanma_Linki",
        "Faolmi",
    ],
    "DM_Voronka_Matnlar": [
        "Bosqich_Kodi",
        "Xabar_Matni",
    ],
    "Statistika_Dashboard": [
        "Korsatkich",
        "Qiymat",
    ],
}

DEFAULT_KEYWORDS = [
    [
        "MALINA",
        "Assalomu alaykum! Directingizga Atlant va Maravilla ko‘chatlari bo‘yicha to‘liq qo‘llanma va narxlarni yubordik 📩",
        "Assalomu alaykum! Angrendagi «Malinachi_» ko‘chatxonasiga xush kelibsiz 🌱\n\nBizda yuqori hosildor *Atlant* va *Maravilla* navli sertifikatlangan malina ko‘chatlari mavjud. O‘zbekistonning barcha viloyatlariga ishonchli yetkazib beramiz.\n\n📄 Agrotexnika qo‘llanmasi: https://t.me/malinalar_uz",
        "HA",
    ],
    [
        "ATLANT",
        "Zo‘r! Atlant navi bo‘yicha batafsil ma'lumot va hisob-kitobni Directingizga yubordik ✅",
        "Assalomu alaykum! *Atlant* remontant malina navi — O‘zbekiston iqlimiga eng chidamli va birinchi yildanoq yuqori hosil beruvchi nav.\n\n📄 Batafsil ma'lumot: https://t.me/malinalar_uz",
        "HA",
    ],
    [
        "NARX",
        "Narxlar va ulgurji chegirmalar jadvalini Directingizga tashlab qo‘ydik 📩",
        "Assalomu alaykum! «Malinachi_» ko‘chatxonasi narxlari hajmga qarab belgilanadi (tomorqa, biznes va ulgurji fermerlar uchun alohida chegirmalar bor).",
        "HA",
    ],
]

DEFAULT_FUNNEL_TEXTS = {
    "QADAM_1_HAJM": (
        "Sizga taxminan nechta ko‘chat kerak bo‘ladi? Raqamni yozing yoki tanlang:\n"
        "1️⃣ 100 – 300 ta (Tomorqa uchun)\n"
        "2️⃣ 500 – 2000 ta (Kichik biznes / issiqxona)\n"
        "3️⃣ 3000+ ta (Katta bog‘ / Ulgurji)"
    ),
    "QADAM_2_MANZIL": (
        "Ajoyib! Ko‘chatlarni qaysi viloyat va tumanga yetkazib berishimiz kerak? "
        "(Masalan: Samarqand, Urgut tumani)"
    ),
    "QADAM_3_TELEFON": (
        "Mutaxassisimiz siz bilan bog‘lanib, ekish sxemasi va aniq narxni hisoblab berishi uchun "
        "telefon raqamingizni yozib qoldiring (Masalan: +998901234567):"
    ),
    "YAKUNIY_JAVOB": (
        "Rahmat! ✅ Ma'lumotlaringiz «Malinachi_» Angren ko‘chatxonasi bazasiga qabul qilindi.\n"
        "Agromutaxassisimiz tez orada siz bilan bog‘lanadi. Kanalimiz: @malinalar_uz"
    ),
    "FOLLOWUP_23SOAT": (
        "Assalomu alaykum! 🌱 Kecha «Malinachi_» ko‘chatxonasidan Atlant va Maravilla ko‘chatlari "
        "bo‘yicha so‘ragan edingiz. Hozir mavsumiy band qilish (zaklad) ochiq — savollaringiz bo‘lsa "
        "shu yerga yozing yoki telefon raqamingizni qoldiring, mutaxassisimiz bepul maslahat beradi!"
    ),
}


def now_uz_str() -> str:
    offset = int(os.getenv("TZ_OFFSET_HOURS", "5"))
    tz = timezone(timedelta(hours=offset))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def today_uz_str() -> str:
    offset = int(os.getenv("TZ_OFFSET_HOURS", "5"))
    tz = timezone(timedelta(hours=offset))
    return datetime.now(tz).strftime("%Y-%m-%d")


class GoogleSheetsCRM:
    def __init__(self) -> None:
        self.sheet_name = os.getenv("GOOGLE_SHEET_NAME", "MalinachiBaza")
        self.creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "service_account.json")
        self.client: Optional[gspread.Client] = None
        self.spreadsheet: Optional[gspread.Spreadsheet] = None

        # 5 daqiqalik (300 soniya) kesh
        self._cache_ttl = 300
        self._last_cache_time = 0.0
        self._keywords_cache: Dict[str, Dict[str, str]] = {}
        self._texts_cache: Dict[str, str] = dict(DEFAULT_FUNNEL_TEXTS)

    def _connect_sync(self) -> None:
        if self.spreadsheet is not None:
            return

        # Render.com da JSON matn sifatida ENV ga qo'yilgan bo'lsa ham ishlaydi
        creds_json_env = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if creds_json_env:
            info = json.loads(creds_json_env)
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        else:
            creds = Credentials.from_service_account_file(self.creds_file, scopes=SCOPES)

        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open(self.sheet_name)

    def _setup_worksheets_sync(self) -> None:
        self._connect_sync()
        assert self.spreadsheet is not None

        existing_titles = {ws.title for ws in self.spreadsheet.worksheets()}

        for title, headers in SHEET_HEADERS.items():
            if title not in existing_titles:
                ws = self.spreadsheet.add_worksheet(title=title, rows=1000, cols=len(headers) + 2)
                ws.append_row(headers, value_input_option="USER_ENTERED")

                if title == "Reels_KalitSozlar":
                    for row in DEFAULT_KEYWORDS:
                        ws.append_row(row, value_input_option="USER_ENTERED")

                elif title == "DM_Voronka_Matnlar":
                    for code, text in DEFAULT_FUNNEL_TEXTS.items():
                        ws.append_row([code, text], value_input_option="USER_ENTERED")

                elif title == "Statistika_Dashboard":
                    dashboard_rows = [
                        ["Jami lidlar soni", "=COUNTA(Lidlar_CRM!A2:A)"],
                        ["Bugungi lidlar soni", '=COUNTIF(Lidlar_CRM!B2:B; TEXT(TODAY();"yyyy-mm-dd")&"*")'],
                        ["Zaklad to'laganlar soni", '=COUNTIF(Lidlar_CRM!K2:K; "Zaklad_tolandi")'],
                        ["Jami sotilgan ko'chatlar (zakladli)", "=SUM(Zaklad_Va_Buyurtmalar!G2:G)"],
                        ["Jami tushgan zaklad summasi (UZS)", "=SUM(Zaklad_Va_Buyurtmalar!J2:J)"],
                        ["Kutilayotgan qoldiq summa (UZS)", "=SUM(Zaklad_Va_Buyurtmalar!K2:K)"],
                        ["Jami shartnoma summasi (UZS)", "=SUM(Zaklad_Va_Buyurtmalar!I2:I)"],
                    ]
                    for d_row in dashboard_rows:
                        ws.append_row(d_row, value_input_option="USER_ENTERED")
            else:
                ws = self.spreadsheet.worksheet(title)
                first_row = ws.row_values(1)
                if not first_row:
                    ws.append_row(headers, value_input_option="USER_ENTERED")

        self._refresh_cache_sync(force=True)

    def _refresh_cache_sync(self, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self._last_cache_time) < self._cache_ttl and self._keywords_cache:
            return

        self._connect_sync()
        assert self.spreadsheet is not None

        # 1. Reels_KalitSozlar keshlash
        ws_kw = self.spreadsheet.worksheet("Reels_KalitSozlar")
        kw_records = ws_kw.get_all_records()
        new_kw: Dict[str, Dict[str, str]] = {}
        for rec in kw_records:
            kw = str(rec.get("Kalit_Soz", "")).strip().upper()
            active = str(rec.get("Faolmi", "")).strip().upper()
            if kw and active in ("HA", "YES", "1", "TRUE"):
                new_kw[kw] = {
                    "comment_reply": str(rec.get("Izohga_Javob_Matni", "")).strip(),
                    "dm_text": str(rec.get("DM_1_Xabar_Va_Qollanma_Linki", "")).strip(),
                }
        self._keywords_cache = new_kw

        # 2. DM_Voronka_Matnlar keshlash
        ws_txt = self.spreadsheet.worksheet("DM_Voronka_Matnlar")
        txt_records = ws_txt.get_all_records()
        new_txt = dict(DEFAULT_FUNNEL_TEXTS)
        for rec in txt_records:
            code = str(rec.get("Bosqich_Kodi", "")).strip().upper()
            msg = str(rec.get("Xabar_Matni", "")).strip()
            if code and msg:
                new_txt[code] = msg
        self._texts_cache = new_txt
        self._last_cache_time = now

    async def initialize(self) -> None:
        await asyncio.to_thread(self._setup_worksheets_sync)

    async def get_keyword_data(self, comment_text: str) -> Optional[Tuple[str, Dict[str, str]]]:
        await asyncio.to_thread(self._refresh_cache_sync, False)
        cleaned = comment_text.strip().upper()
        # Aniq moslik yoki izoh ichida kalit so'z qatnashganini tekshirish
        for kw, data in self._keywords_cache.items():
            if kw == cleaned or kw in cleaned.split():
                return kw, data
        for kw, data in self._keywords_cache.items():
            if kw in cleaned:
                return kw, data
        return None

    async def get_funnel_text(self, code: str) -> str:
        await asyncio.to_thread(self._refresh_cache_sync, False)
        return self._texts_cache.get(code.upper(), DEFAULT_FUNNEL_TEXTS.get(code.upper(), ""))

    def _add_lead_sync(
        self,
        source: str,
        username_or_id: str,
        full_name: str,
        phone: str,
        region: str,
        seedling_count: str,
        segment: str,
        keyword: str,
        note: str = "",
    ) -> str:
        self._connect_sync()
        assert self.spreadsheet is not None
        ws = self.spreadsheet.worksheet("Lidlar_CRM")

        col_a = ws.col_values(1)
        next_num = len(col_a)  # 1-qator sarlavha bo'lgani uchun len(col_a) yangi tartib raqami
        lead_id = f"MLN-{next_num:04d}"

        row = [
            lead_id,
            now_uz_str(),
            source,
            username_or_id,
            full_name or "Noma'lum",
            f"'{phone}",  # '+' belgisi formulaga aylanib ketmasligi uchun
            region,
            seedling_count,
            segment,
            keyword or "To'g'ridan-to'g'ri",
            "Yangi",
            0,
            0,
            0,
            note,
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return lead_id

    async def add_lead(
        self,
        source: str,
        username_or_id: str,
        full_name: str,
        phone: str,
        region: str,
        seedling_count: str,
        segment: str,
        keyword: str,
        note: str = "",
    ) -> str:
        return await asyncio.to_thread(
            self._add_lead_sync,
            source,
            username_or_id,
            full_name,
            phone,
            region,
            seedling_count,
            segment,
            keyword,
            note,
        )

    def _update_lead_status_sync(self, lead_id: str, new_status: str) -> bool:
        self._connect_sync()
        assert self.spreadsheet is not None
        ws = self.spreadsheet.worksheet("Lidlar_CRM")
        cell = ws.find(lead_id, in_column=1)
        if not cell:
            return False
        # 11-ustun: Voronka_Bosqichi
        ws.update_cell(cell.row, 11, new_status)
        return True

    async def update_lead_status(self, lead_id: str, new_status: str) -> bool:
        return await asyncio.to_thread(self._update_lead_status_sync, lead_id, new_status)

    def _record_deposit_sync(
        self,
        lead_id: str,
        seedling_qty: int,
        deposit_amount: int,
        variety: str = "Atlant",
        payment_method: str = "Click/Payme",
    ) -> Optional[Dict[str, Any]]:
        self._connect_sync()
        assert self.spreadsheet is not None

        ws_leads = self.spreadsheet.worksheet("Lidlar_CRM")
        cell = ws_leads.find(lead_id, in_column=1)
        if not cell:
            return None

        row_vals = ws_leads.row_values(cell.row)
        # Ustun indekslari (0-based): 4=Ism, 5=Telefon, 6=Viloyat_Tuman
        client_name = row_vals[4] if len(row_vals) > 4 else "Mijoz"
        phone = row_vals[5].lstrip("'") if len(row_vals) > 5 else ""
        region = row_vals[6] if len(row_vals) > 6 else ""

        unit_price = int(os.getenv("DEFAULT_SEEDLING_PRICE", "23000"))
        total_amount = seedling_qty * unit_price
        remaining_amount = max(0, total_amount - deposit_amount)

        # Lidlar_CRM yangilash: Kocat_Soni(8), Bosqich(11), Umumiy(12), Zaklad(13), Qoldiq(14)
        ws_leads.update_cell(cell.row, 8, seedling_qty)
        ws_leads.update_cell(cell.row, 11, "Zaklad_tolandi")
        ws_leads.update_cell(cell.row, 12, total_amount)
        ws_leads.update_cell(cell.row, 13, deposit_amount)
        ws_leads.update_cell(cell.row, 14, remaining_amount)

        # Zaklad_Va_Buyurtmalar varag'iga qo'shish
        ws_orders = self.spreadsheet.worksheet("Zaklad_Va_Buyurtmalar")
        order_num = len(ws_orders.col_values(1))
        order_id = f"ORD-{order_num:04d}"

        order_row = [
            order_id,
            today_uz_str(),
            client_name,
            f"'{phone}",
            region,
            variety,
            seedling_qty,
            unit_price,
            total_amount,
            deposit_amount,
            remaining_amount,
            payment_method,
            "Kelishiladi",
            "Tayyorlanmoqda",
        ]
        ws_orders.append_row(order_row, value_input_option="USER_ENTERED")

        return {
            "order_id": order_id,
            "lead_id": lead_id,
            "client_name": client_name,
            "phone": phone,
            "region": region,
            "qty": seedling_qty,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "deposit_amount": deposit_amount,
            "remaining_amount": remaining_amount,
        }

    async def record_deposit(
        self,
        lead_id: str,
        seedling_qty: int,
        deposit_amount: int,
        variety: str = "Atlant",
        payment_method: str = "Click/Payme",
    ) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._record_deposit_sync,
            lead_id,
            seedling_qty,
            deposit_amount,
            variety,
            payment_method,
        )

    def _get_stats_sync(self) -> Dict[str, Any]:
        self._connect_sync()
        assert self.spreadsheet is not None

        ws_leads = self.spreadsheet.worksheet("Lidlar_CRM")
        leads = ws_leads.get_all_records()

        ws_orders = self.spreadsheet.worksheet("Zaklad_Va_Buyurtmalar")
        orders = ws_orders.get_all_records()

        today = today_uz_str()
        total_leads = len(leads)
        today_leads = sum(1 for r in leads if str(r.get("Sana_Vaqt", "")).startswith(today))
        vip_leads = sum(1 for r in leads if str(r.get("Segment", "")) == "VIP_Ulgurji")
        paid_leads = sum(1 for r in leads if str(r.get("Voronka_Bosqichi", "")) == "Zaklad_tolandi")

        total_qty = 0
        total_deposit = 0
        total_remaining = 0
        total_contract = 0

        for o in orders:
            try:
                total_qty += int(float(str(o.get("Soni_Dona", 0) or 0).replace(" ", "")))
                total_deposit += int(float(str(o.get("Zaklad_Tolandi", 0) or 0).replace(" ", "")))
                total_remaining += int(float(str(o.get("Qoldiq_Pul", 0) or 0).replace(" ", "")))
                total_contract += int(float(str(o.get("Jami_Summa", 0) or 0).replace(" ", "")))
            except ValueError:
                continue

        return {
            "total_leads": total_leads,
            "today_leads": today_leads,
            "vip_leads": vip_leads,
            "paid_leads": paid_leads,
            "total_qty": total_qty,
            "total_deposit": total_deposit,
            "total_remaining": total_remaining,
            "total_contract": total_contract,
        }

    async def get_stats(self) -> Dict[str, Any]:
        return await asyncio.to_thread(self._get_stats_sync)


crm_sheets = GoogleSheetsCRM()