import json
import time
from typing import Any, Dict, List, Optional
import aiosqlite

DB_PATH = "crm_state.db"


async def init_db() -> None:
    """SQLite jadvallarini yaratish (FSM holatlar, follow-up taymerlar va admin state)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS user_sessions (
                platform TEXT NOT NULL,
                user_id TEXT NOT NULL,
                username TEXT DEFAULT '',
                full_name TEXT DEFAULT '',
                step TEXT DEFAULT 'IDLE',
                keyword TEXT DEFAULT '',
                volume_text TEXT DEFAULT '',
                segment TEXT DEFAULT '',
                region TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                last_interaction REAL DEFAULT 0,
                followup_due REAL DEFAULT 0,
                followup_sent INTEGER DEFAULT 0,
                PRIMARY KEY (platform, user_id)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_states (
                admin_id TEXT PRIMARY KEY,
                action TEXT DEFAULT '',
                payload TEXT DEFAULT '{}'
            )
            """
        )
        await db.commit()
        await db.execute("""
        CREATE TABLE IF NOT EXISTS processed_ig_comments (
            comment_id TEXT PRIMARY KEY,
            user_id TEXT,
            username TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


async def get_session(platform: str, user_id: str) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM user_sessions WHERE platform = ? AND user_id = ?",
            (platform, str(user_id)),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def upsert_session(
    platform: str,
    user_id: str,
    username: Optional[str] = None,
    full_name: Optional[str] = None,
    step: Optional[str] = None,
    keyword: Optional[str] = None,
    volume_text: Optional[str] = None,
    segment: Optional[str] = None,
    region: Optional[str] = None,
    phone: Optional[str] = None,
    schedule_followup_hours: Optional[float] = None,
    cancel_followup: bool = False,
) -> Dict[str, Any]:
    now = time.time()
    existing = await get_session(platform, str(user_id))

    if not existing:
        followup_due = (now + schedule_followup_hours * 3600) if schedule_followup_hours else 0
        followup_sent = 0 if schedule_followup_hours else 1
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO user_sessions (
                    platform, user_id, username, full_name, step, keyword,
                    volume_text, segment, region, phone, last_interaction,
                    followup_due, followup_sent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    platform,
                    str(user_id),
                    username or "",
                    full_name or "",
                    step or "IDLE",
                    keyword or "",
                    volume_text or "",
                    segment or "",
                    region or "",
                    phone or "",
                    now,
                    followup_due,
                    followup_sent,
                ),
            )
            await db.commit()
    else:
        new_username = username if username is not None else existing["username"]
        new_full_name = full_name if full_name is not None else existing["full_name"]
        new_step = step if step is not None else existing["step"]
        new_keyword = keyword if keyword is not None else existing["keyword"]
        new_volume = volume_text if volume_text is not None else existing["volume_text"]
        new_segment = segment if segment is not None else existing["segment"]
        new_region = region if region is not None else existing["region"]
        new_phone = phone if phone is not None else existing["phone"]

        followup_due = existing["followup_due"]
        followup_sent = existing["followup_sent"]

        if cancel_followup:
            followup_due = 0
            followup_sent = 1
        elif schedule_followup_hours is not None:
            followup_due = now + schedule_followup_hours * 3600
            followup_sent = 0

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                UPDATE user_sessions SET
                    username = ?, full_name = ?, step = ?, keyword = ?,
                    volume_text = ?, segment = ?, region = ?, phone = ?,
                    last_interaction = ?, followup_due = ?, followup_sent = ?
                WHERE platform = ? AND user_id = ?
                """,
                (
                    new_username,
                    new_full_name,
                    new_step,
                    new_keyword,
                    new_volume,
                    new_segment,
                    new_region,
                    new_phone,
                    now,
                    followup_due,
                    followup_sent,
                    platform,
                    str(user_id),
                ),
            )
            await db.commit()

    updated = await get_session(platform, str(user_id))
    assert updated is not None
    return updated


async def get_due_followups() -> List[Dict[str, Any]]:
    """23 soatlik taymeri yetib kelgan va hali raqam qoldirmagan mijozlarni olish."""
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT * FROM user_sessions
            WHERE followup_sent = 0
              AND followup_due > 0
              AND followup_due <= ?
              AND step IN ('WAIT_VOLUME', 'WAIT_REGION', 'WAIT_PHONE')
            """,
            (now,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def mark_followup_sent(platform: str, user_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE user_sessions SET followup_sent = 1, followup_due = 0 WHERE platform = ? AND user_id = ?",
            (platform, str(user_id)),
        )
        await db.commit()


async def set_admin_state(admin_id: str, action: str, payload: Optional[Dict[str, Any]] = None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO admin_states (admin_id, action, payload)
            VALUES (?, ?, ?)
            ON CONFLICT(admin_id) DO UPDATE SET action = excluded.action, payload = excluded.payload
            """,
            (str(admin_id), action, json.dumps(payload or {}, ensure_ascii=False)),
        )
        await db.commit()


async def get_admin_state(admin_id: str) -> Dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM admin_states WHERE admin_id = ?", (str(admin_id),)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return {"action": "", "payload": {}}
            return {
                "action": row["action"],
                "payload": json.loads(row["payload"] or "{}"),
            }


async def is_ig_comment_processed(comment_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM processed_ig_comments WHERE comment_id = ?", (comment_id,)
        ) as cursor:
            return await cursor.fetchone() is not None

async def is_ig_user_recently_messaged(user_id: str) -> bool:
    """Bitta foydalanuvchiga 24 soat ichida qayta-qayta DM yubormaslik uchun tekshiruv."""
    if not user_id:
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT 1 FROM processed_ig_comments 
            WHERE user_id = ? AND created_at >= datetime('now', '-24 hours')
            """,
            (user_id,),
        ) as cursor:
            return await cursor.fetchone() is not None

async def mark_ig_comment_processed(comment_id: str, user_id: str, username: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO processed_ig_comments (comment_id, user_id, username) VALUES (?, ?, ?)",
            (comment_id, user_id, username),
        )
        await db.commit()

async def clear_admin_state(admin_id: str) -> None:
    await set_admin_state(str(admin_id), "", {})