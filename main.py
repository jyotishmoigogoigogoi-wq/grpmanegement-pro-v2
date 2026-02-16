#!/usr/bin/env python3
"""
Group Master Elite - Production-Grade Telegram Bot
Single-file architecture with internal service layers
"""

# =============================================================================
# SECTION 1: IMPORTS & CONFIGURATION
# =============================================================================
import os
import re
import sys
import asyncio
import logging
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any, Callable
from functools import wraps

import pytz
import asyncpg
from groq import Groq

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ChatType, ParseMode

# =============================================================================
# SECTION 2: CONFIGURATION (ENVIRONMENT-BASED)
# =============================================================================
DATABASE_URL = os.getenv("DATABASE_URL")
BOT_TOKEN = os.getenv("BOT_TOKEN") 
GROQ_API_KEY = os.getenv("GROQ_API_KEY") 
GLOBAL_OWNER_ID = int(os.getenv("GLOBAL_OWNER_ID", "7728424218"))

# Webhook configuration (for Render)
WEBHOOK_URL = os.getenv("WEBHOOK_URL")          # e.g., https://your-app.onrender.com
PORT = int(os.getenv("PORT", 8443))
WEBHOOK_LISTEN = os.getenv("WEBHOOK_LISTEN", "0.0.0.0")

IST = pytz.timezone("Asia/Kolkata")

# =============================================================================
# SECTION 3: CONSTANTS & GAME BALANCE
# =============================================================================
XP_PER_LEVEL = 700
MAX_LEVEL = 70
SHIELD_COST = 1100
SHIELD_DURATION_HOURS = 11
DAILY_COINS = 100
DAILY_COOLDOWN_HOURS = 11
SCRATCH_COOLDOWN_HOURS = 1
REVIVE_SELF_COST = 700
REVIVE_OTHER_COST = 800
XP_PER_MESSAGE = 10

GIFT_TYPES = {
    "teddy": {"emoji": "🧸", "price": 50},
    "rose": {"emoji": "🌹", "price": 30},
    "heart": {"emoji": "❤️", "price": 20},
    "slap": {"emoji": "🤚", "price": 10},
    "cake": {"emoji": "🍰", "price": 100},
    "ring": {"emoji": "💍", "price": 500},
    "kiss": {"emoji": "💋", "price": 40},
    "hug": {"emoji": "🤗", "price": 25},
}

LEVEL_SYMBOLS = [
    (0, 9, "⛧"),
    (10, 19, "⛦"),
    (20, 29, "✞"),
    (30, 39, "✠"),
    (40, 49, "♱"),
    (50, 59, "☾"),
    (60, 69, "☽"),
    (70, 70, "☬"),
]

RICHES_TITLES = [
    "🥇 ⟡𝐓𝐎𝐏 𝟏⟡",
    "🥈 ⟡𝐓𝐎𝐏 𝟐⟡",
    "🥉 ⟡𝐓𝐎𝐏 𝟑⟡",
    "♛ 𝐄𝐌𝐏𝐄𝐑𝐎𝐑 ♛",
    "𓆩𝐑𝐎𝐘𝐀𝐋𓆪",
    "✦ 𝐌𝐈𝐋𝐋𝐈𝐎𝐍𝐀𝐈𝐑 ✦",
    "💎 𝐁𝐈𝐋𝐋𝐈𝐎𝐍𝐀𝐈𝐑 💎",
    "⚜ 𝐂𝐑𝐎𝐖𝐍𝐄𝐃 ⚜",
    "⛧ 𝐃𝐎𝐌𝐈𝐍𝐀𝐓𝐎𝐑 ⛧",
    "👑 𝐋𝐄𝐆𝐀𝐂𝐘 𝐊𝐈𝐍𝐆 👑",
]

MEDALS = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

# =============================================================================
# SECTION 4: LOGGING CONFIGURATION
# =============================================================================
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("GME")

# =============================================================================
# SECTION 5: RATE LIMITING SYSTEM
# =============================================================================
@dataclass
class RateLimitEntry:
    count: int
    window_start: datetime

class RateLimiter:
    def __init__(self, max_requests: int = 5, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._storage: Dict[Tuple[int, str], RateLimitEntry] = {}
        self._lock = asyncio.Lock()
    
    async def check(self, user_id: int, command: str) -> Tuple[bool, int]:
        key = (user_id, command)
        now = datetime.now(IST)
        async with self._lock:
            entry = self._storage.get(key)
            if entry is None or (now - entry.window_start).total_seconds() > self.window_seconds:
                self._storage[key] = RateLimitEntry(1, now)
                return True, self.max_requests - 1
            if entry.count >= self.max_requests:
                remaining = self.window_seconds - int((now - entry.window_start).total_seconds())
                return False, remaining
            entry.count += 1
            return True, self.max_requests - entry.count

rate_limiter = RateLimiter(max_requests=10, window_seconds=60)

# =============================================================================
# SECTION 6: PENDING STATE MANAGER (THREAD-SAFE)
# =============================================================================
class PendingStateManager:
    """Thread-safe pending action manager with automatic cleanup."""
    
    def __init__(self, expiry_seconds: int = 300):
        self._storage: Dict[int, Dict[int, Dict[str, Any]]] = {}
        self._expiry: Dict[int, Dict[int, datetime]] = {}
        self._lock = asyncio.Lock()
        self._expiry_seconds = expiry_seconds
    
    async def set(self, chat_id: int, user_id: int, data: Dict[str, Any]) -> None:
        async with self._lock:
            if chat_id not in self._storage:
                self._storage[chat_id] = {}
                self._expiry[chat_id] = {}
            self._storage[chat_id][user_id] = data
            self._expiry[chat_id][user_id] = datetime.now(IST) + timedelta(seconds=self._expiry_seconds)
    
    async def get(self, chat_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if chat_id not in self._storage:
                return None
            expiry = self._expiry.get(chat_id, {}).get(user_id)
            if expiry and datetime.now(IST) > expiry:
                self._storage[chat_id].pop(user_id, None)
                self._expiry[chat_id].pop(user_id, None)
                return None
            return self._storage[chat_id].get(user_id)
    
    async def delete(self, chat_id: int, user_id: int) -> None:
        async with self._lock:
            if chat_id in self._storage:
                self._storage[chat_id].pop(user_id, None)
                self._expiry[chat_id].pop(user_id, None)

pending_manager = PendingStateManager(expiry_seconds=300)

# =============================================================================
# SECTION 7: DATABASE SERVICE LAYER
# =============================================================================
class DatabaseService:
    """Singleton database service with connection pooling."""
    
    _instance: Optional['DatabaseService'] = None
    _pool: Optional[asyncpg.Pool] = None
    _lock = asyncio.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def initialize(self) -> None:
        async with self._lock:
            if self._pool is None:
                self._pool = await asyncpg.create_pool(
                    DATABASE_URL,
                    min_size=5,
                    max_size=20,
                    command_timeout=60,
                )
                logger.info("Database pool initialized")
    
    async def close(self) -> None:
        async with self._lock:
            if self._pool:
                await self._pool.close()
                self._pool = None
                logger.info("Database pool closed")
    
    @asynccontextmanager
    async def acquire(self):
        if self._pool is None:
            await self.initialize()
        async with self._pool.acquire() as conn:
            yield conn
    
    @asynccontextmanager
    async def transaction(self):
        async with self.acquire() as conn:
            async with conn.transaction():
                yield conn
    
    # ==================== SCHEMA INITIALIZATION ====================
    async def init_schema(self) -> None:
        async with self.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users_global (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT NOT NULL DEFAULT 'Unknown',
                    total_xp BIGINT NOT NULL DEFAULT 0,
                    total_coins BIGINT NOT NULL DEFAULT 0,
                    level INT NOT NULL DEFAULT 0,
                    last_updated TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS groups (
                    chat_id BIGINT PRIMARY KEY,
                    title TEXT,
                    username TEXT,
                    invite_link TEXT,
                    added_on TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users_per_group (
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    username TEXT NOT NULL DEFAULT 'Unknown',
                    msg_count INT NOT NULL DEFAULT 0,
                    xp INT NOT NULL DEFAULT 0,
                    coins INT NOT NULL DEFAULT 0,
                    last_daily TIMESTAMPTZ,
                    last_scratch TIMESTAMPTZ,
                    shield_expiry TIMESTAMPTZ,
                    is_dead BOOLEAN NOT NULL DEFAULT FALSE,
                    is_verified_owner INT NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, chat_id)
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS gifts (
                    id SERIAL PRIMARY KEY,
                    from_user BIGINT NOT NULL,
                    to_user BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    gift_type TEXT NOT NULL,
                    amount INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            # Indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_global_xp ON users_global(total_xp DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_global_coins ON users_global(total_coins DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_per_group_chat ON users_per_group(chat_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_per_group_xp ON users_per_group(chat_id, xp DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_per_group_coins ON users_per_group(chat_id, coins DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_per_group_username ON users_per_group(chat_id, username)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_gifts_to_user ON gifts(to_user)")
            logger.info("Database schema initialized")
    
    # ==================== USER GLOBAL OPERATIONS ====================
    async def get_user_global(self, user_id: int) -> Optional[Dict[str, Any]]:
        async with self.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users_global WHERE user_id = $1",
                user_id
            )
            return dict(row) if row else None
    
    async def update_user_global(self, user_id: int, username: str, xp_delta: int = 0, coins_delta: int = 0) -> None:
        async with self.acquire() as conn:
            await conn.execute("""
                INSERT INTO users_global (user_id, username, total_xp, total_coins, last_updated)
                VALUES ($1, $2, GREATEST($3, 0), GREATEST($4, 0), NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET username = EXCLUDED.username,
                    total_xp = GREATEST(users_global.total_xp + EXCLUDED.total_xp, 0),
                    total_coins = GREATEST(users_global.total_coins + EXCLUDED.total_coins, 0),
                    last_updated = NOW()
            """, user_id, username, xp_delta, coins_delta)
    
    async def get_global_leaderboard(self, limit: int = 10) -> List[Dict[str, Any]]:
        async with self.acquire() as conn:
            rows = await conn.fetch("""
                SELECT username, total_xp, total_coins, (total_xp / $1)::int as level
                FROM users_global
                ORDER BY total_xp DESC
                LIMIT $2
            """, XP_PER_LEVEL, limit)
            return [dict(r) for r in rows]
    
    async def get_riches_leaderboard(self, limit: int = 10) -> List[Dict[str, Any]]:
        async with self.acquire() as conn:
            rows = await conn.fetch("""
                SELECT username, total_coins
                FROM users_global
                ORDER BY total_coins DESC
                LIMIT $1
            """, limit)
            return [dict(r) for r in rows]
    
    async def get_user_total_messages(self, user_id: int) -> int:
        async with self.acquire() as conn:
            result = await conn.fetchval(
                "SELECT COALESCE(SUM(msg_count), 0) FROM users_per_group WHERE user_id = $1",
                user_id
            )
            return result or 0
    
    # ==================== USER PER GROUP OPERATIONS ====================
    async def get_user_per_group(self, user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
        async with self.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users_per_group WHERE user_id = $1 AND chat_id = $2",
                user_id, chat_id
            )
            return dict(row) if row else None
    
    async def update_user_per_group(
        self,
        user_id: int,
        chat_id: int,
        username: str,
        xp_delta: int = 0,
        coins_delta: int = 0,
        msg_inc: bool = False,
        shield_expiry: Optional[datetime] = None,
        set_dead: Optional[bool] = None,
        last_daily: bool = False,
        last_scratch: bool = False,
        is_verified_owner: Optional[int] = None,
    ) -> None:
        async with self.acquire() as conn:
            # Build dynamic query safely
            set_clauses = []
            params = [user_id, chat_id, username]
            param_idx = 4
            
            if xp_delta != 0:
                set_clauses.append(f"xp = GREATEST(users_per_group.xp + ${param_idx}, 0)")
                params.append(xp_delta)
                param_idx += 1
            
            if coins_delta != 0:
                set_clauses.append(f"coins = GREATEST(users_per_group.coins + ${param_idx}, 0)")
                params.append(coins_delta)
                param_idx += 1
            
            if msg_inc:
                set_clauses.append("msg_count = users_per_group.msg_count + 1")
            
            if shield_expiry is not None:
                set_clauses.append(f"shield_expiry = ${param_idx}")
                params.append(shield_expiry)
                param_idx += 1
            
            if set_dead is not None:
                set_clauses.append(f"is_dead = ${param_idx}")
                params.append(set_dead)
                param_idx += 1
            
            if last_daily:
                set_clauses.append("last_daily = NOW()")
            
            if last_scratch:
                set_clauses.append("last_scratch = NOW()")
            
            if is_verified_owner is not None:
                set_clauses.append(f"is_verified_owner = ${param_idx}")
                params.append(is_verified_owner)
                param_idx += 1
            
            set_clause = ", ".join(set_clauses) if set_clauses else ""
            
            if set_clause:
                query = f"""
                    INSERT INTO users_per_group (user_id, chat_id, username)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (user_id, chat_id) DO UPDATE
                    SET username = EXCLUDED.username,
                        {set_clause}
                """
            else:
                query = """
                    INSERT INTO users_per_group (user_id, chat_id, username)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (user_id, chat_id) DO UPDATE
                    SET username = EXCLUDED.username
                """
            
            await conn.execute(query, *params)
    
    async def get_group_leaderboard(self, chat_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        async with self.acquire() as conn:
            rows = await conn.fetch("""
                SELECT username, xp, (xp / $1)::int as level
                FROM users_per_group
                WHERE chat_id = $2
                ORDER BY xp DESC
                LIMIT $3
            """, XP_PER_LEVEL, chat_id, limit)
            return [dict(r) for r in rows]
    
    async def get_group_riches(self, chat_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        async with self.acquire() as conn:
            rows = await conn.fetch("""
                SELECT username, coins
                FROM users_per_group
                WHERE chat_id = $1
                ORDER BY coins DESC
                LIMIT $2
            """, chat_id, limit)
            return [dict(r) for r in rows]
    
    async def find_user_by_username(self, chat_id: int, username: str) -> Optional[int]:
        async with self.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT user_id FROM users_per_group WHERE chat_id = $1 AND username = $2",
                chat_id, username.lstrip('@')
            )
            return row['user_id'] if row else None
    
    async def get_user_chat_ids(self, user_id: int) -> List[int]:
        async with self.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT chat_id FROM users_per_group WHERE user_id = $1",
                user_id
            )
            return [r['chat_id'] for r in rows]
    
    # ==================== ATOMIC COIN OPERATIONS ====================
    async def transfer_coins(
        self,
        from_user_id: int,
        to_user_id: int,
        chat_id: int,
        amount: int,
        from_username: str,
        to_username: str,
    ) -> Tuple[bool, str]:
        """Atomic coin transfer with validation. Returns (success, message)."""
        async with self.transaction() as conn:
            # Check sender's balance
            sender = await conn.fetchrow(
                "SELECT coins FROM users_per_group WHERE user_id = $1 AND chat_id = $2 FOR UPDATE",
                from_user_id, chat_id
            )
            if not sender:
                return False, "Sender not found in this group."
            if sender['coins'] < amount:
                return False, f"Insufficient coins. You have {sender['coins']}."
            
            # Deduct from sender
            await conn.execute("""
                UPDATE users_per_group
                SET coins = coins - $1
                WHERE user_id = $2 AND chat_id = $3
            """, amount, from_user_id, chat_id)
            
            # Add to receiver
            await conn.execute("""
                INSERT INTO users_per_group (user_id, chat_id, username, coins)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id, chat_id) DO UPDATE
                SET coins = users_per_group.coins + EXCLUDED.coins,
                    username = EXCLUDED.username
            """, to_user_id, chat_id, to_username, amount)
            
            # Update global balances
            await conn.execute("""
                INSERT INTO users_global (user_id, username, total_coins)
                VALUES ($1, $2, -$3)
                ON CONFLICT (user_id) DO UPDATE
                SET total_coins = GREATEST(users_global.total_coins - $3, 0),
                    username = EXCLUDED.username
            """, from_user_id, from_username, amount)
            
            await conn.execute("""
                INSERT INTO users_global (user_id, username, total_coins)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE
                SET total_coins = users_global.total_coins + EXCLUDED.total_coins,
                    username = EXCLUDED.username
            """, to_user_id, to_username, amount)
            
            return True, "Transfer successful"
    
    # ==================== GIFT OPERATIONS ====================
    async def add_gift(self, from_user: int, to_user: int, chat_id: int, gift_type: str, amount: int) -> None:
        async with self.acquire() as conn:
            await conn.execute("""
                INSERT INTO gifts (from_user, to_user, chat_id, gift_type, amount, created_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
            """, from_user, to_user, chat_id, gift_type, amount)
    
    async def get_gifts(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        async with self.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM gifts WHERE to_user = $1
                ORDER BY created_at DESC LIMIT $2
            """, user_id, limit)
            return [dict(r) for r in rows]
    
    # ==================== GROUP OPERATIONS ====================
    async def add_group(self, chat_id: int, title: str, username: Optional[str], invite_link: Optional[str]) -> None:
        async with self.acquire() as conn:
            await conn.execute("""
                INSERT INTO groups (chat_id, title, username, invite_link, added_on)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (chat_id) DO UPDATE
                SET title = EXCLUDED.title,
                    username = EXCLUDED.username,
                    invite_link = EXCLUDED.invite_link
            """, chat_id, title, username, invite_link)
    
    async def remove_group(self, chat_id: int) -> None:
        async with self.acquire() as conn:
            await conn.execute("DELETE FROM groups WHERE chat_id = $1", chat_id)
    
    async def get_all_groups(self) -> List[Dict[str, Any]]:
        async with self.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM groups ORDER BY added_on DESC")
            return [dict(r) for r in rows]
    
    async def get_group_stats(self, chat_id: int) -> Dict[str, Any]:
        async with self.acquire() as conn:
            top_user = await conn.fetchrow("""
                SELECT username, xp FROM users_per_group
                WHERE chat_id = $1 ORDER BY xp DESC LIMIT 1
            """, chat_id)
            user_count = await conn.fetchval(
                "SELECT COUNT(*) FROM users_per_group WHERE chat_id = $1",
                chat_id
            )
            return {
                "top_user": dict(top_user) if top_user else None,
                "user_count": user_count or 0,
            }

# Global database service instance
db = DatabaseService()

# =============================================================================
# SECTION 8: AI SERVICE
# =============================================================================
class AIService:
    def __init__(self):
        self._client: Optional[Groq] = None
        if GROQ_API_KEY:
            self._client = Groq(api_key=GROQ_API_KEY)
    
    async def roast(self, target_name: str, context: str = "") -> str:
        if not self._client:
            return "AI is not configured."
        
        prompt = f"Roast {target_name}. Keep it under 20 words, savage, witty, use modern slang and emojis."
        if context:
            prompt += f" Context: {context}"
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self._client.chat.completions.create(
                    model="llama-3.1-8b-instant",
                    messages=[
                        {"role": "system", "content": "You are a savage, witty roaster."},
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=100,
                )
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"AI error: {e}")
            return "AI is sleeping. Try later!"
    
    async def ask(self, question: str) -> str:
        if not self._client:
            return "AI is not configured."
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self._client.chat.completions.create(
                    model="llama-3.1-8b-instant",
                    messages=[
                        {"role": "system", "content": "You are a helpful AI assistant."},
                        {"role": "user", "content": question},
                    ],
                    max_tokens=500,
                )
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"AI error: {e}")
            return "AI is sleeping. Try later!"

ai_service = AIService()

# =============================================================================
# SECTION 9: UI FORMATTING UTILITIES
# =============================================================================
def get_level_symbol(level: int) -> str:
    for low, high, sym in LEVEL_SYMBOLS:
        if low <= level <= high:
            return sym
    return "⛧"

def format_number(num: int) -> str:
    return f"{num:,}"

def create_progress_bar(current: int, total: int, length: int = 10) -> str:
    filled = int((current / total) * length) if total > 0 else 0
    bar = "█" * filled + "░" * (length - filled)
    percentage = int((current / total) * 100) if total > 0 else 0
    return f"[{bar}] {percentage}%"

def border_text(title: str, content: str) -> str:
    separator = "─" * 20
    return f"┏━━━ {title} ━━━┓\n{content}\n┗{separator}┛"

def styled_box(title: str, lines: List[str], emoji: str = "✨") -> str:
    content = "\n".join(f"{emoji} {line}" for line in lines)
    return border_text(title, content)

# =============================================================================
# SECTION 10: DECORATORS & MIDDLEWARE
# =============================================================================
def require_group(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.effective_chat:
            return
        if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            await update.message.reply_text("❌ This command only works in groups.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

def rate_limit_command(command_name: str):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            allowed, remaining = await rate_limiter.check(user_id, command_name)
            if not allowed:
                await update.message.reply_text(
                    f"⏳ Rate limit hit! Try again in {remaining}s."
                )
                return
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

def safe_reply(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        try:
            return await func(update, context, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Error in {func.__name__}")
            try:
                if update and update.effective_message:
                    await update.effective_message.reply_text(
                        "❌ An error occurred. Please try again later."
                    )
            except:
                pass
    return wrapper

# =============================================================================
# SECTION 11: OWNER VERIFICATION
# =============================================================================
async def is_owner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is global owner or verified group owner."""
    user_id = update.effective_user.id
    if user_id == GLOBAL_OWNER_ID:
        return True
    
    chat_id = update.effective_chat.id
    try:
        perms = await context.bot.get_chat_member(chat_id, user_id)
        if perms.status == "creator":
            user_data = await db.get_user_per_group(user_id, chat_id)
            if user_data and user_data.get("is_verified_owner") == 1:
                return True
    except Exception as e:
        logger.error(f"Owner check error: {e}")
    
    return False

async def require_owner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not await is_owner(update, context):
        await update.message.reply_text("❌ Only the group owner can use this command.")
        return False
    return True

# =============================================================================
# SECTION 12: LEVEL & XP SYSTEM
# =============================================================================
async def process_message_xp(
    user_id: int,
    username: str,
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Process XP gain and handle level ups with broadcasting."""
    # Get current state
    old_global = await db.get_user_global(user_id)
    old_level = old_global['total_xp'] // XP_PER_LEVEL if old_global else 0
    
    # Update per-group and global
    await db.update_user_per_group(user_id, chat_id, username, xp_delta=XP_PER_MESSAGE, msg_inc=True)
    await db.update_user_global(user_id, username, xp_delta=XP_PER_MESSAGE)
    
    # Check level up
    new_global = await db.get_user_global(user_id)
    new_level = new_global['total_xp'] // XP_PER_LEVEL
    
    if new_level > old_level:
        await handle_level_up(user_id, username, old_level, new_level, chat_id, context)

async def handle_level_up(
    user_id: int,
    username: str,
    old_level: int,
    new_level: int,
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Broadcast level up messages."""
    symbol = get_level_symbol(new_level)
    msg = border_text(
        "𝐋𝐄𝐕𝐄𝐋 𝐔𝐏 !",
        f"🎉 @{username} just advanced to 𝐋𝐞𝐯𝐞𝐥 {new_level} ({symbol})!\nKeep grinding! 🔥"
    )
    try:
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Level up broadcast failed: {e}")
    
    # Elite achievement
    if new_level >= MAX_LEVEL and old_level < MAX_LEVEL:
        await broadcast_elite(user_id, username, symbol, context)

async def broadcast_elite(
    user_id: int,
    username: str,
    symbol: str,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Broadcast elite achievement to all user's groups."""
    chat_ids = await db.get_user_chat_ids(user_id)
    elite_msg = border_text(
        "★ 𝐄𝐋𝐈𝐓𝐄 𝐀𝐂𝐇𝐈𝐄𝐕𝐄𝐃 ★",
        f"👑 @{username} has reached the ultimate rank: 𝐄𝐋𝐈𝐓𝐄 {symbol}!\nThis is a historic moment! Everyone cheer! 🎊"
    )
    for grp_id in chat_ids:
        try:
            await context.bot.send_message(chat_id=grp_id, text=elite_msg, parse_mode=ParseMode.HTML)
        except Exception:
            pass  # Bot might not be in group anymore

# =============================================================================
# SECTION 13: SHIELD UTILITIES
# =============================================================================
def has_active_shield(user_data: Optional[Dict[str, Any]]) -> bool:
    if not user_data:
        return False
    shield_expiry = user_data.get('shield_expiry')
    if not shield_expiry:
        return False
    now = datetime.now(IST)
    if shield_expiry.tzinfo is None:
        shield_expiry = IST.localize(shield_expiry)
    return shield_expiry > now

# =============================================================================
# SECTION 14: COMMAND HANDLERS
# =============================================================================

@safe_reply
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user.first_name
    content = (
        f"👑 Welcome, {user}!\n\n"
        "I track your activity across all groups and reward you with XP & coins.\n"
        "Your progress is global – every message counts towards your rank.\n\n"
        "✦ 𝐂𝐎𝐑𝐄 𝐂𝐎𝐌𝐌𝐀𝐍𝐃𝐒 ✦\n"
        "/rank          – Your global stats\n"
        "/leaderboard   – Top 10 global XP holders\n"
        "/riches        – Top 10 global coin hoarders\n"
        "/grpleaderboard – Top 10 in this group (XP)\n"
        "/grpriches     – Top 10 in this group (coins)\n\n"
        "🎁 /daily       – 100 coins daily\n"
        "🎰 /scratch     – Luck game (hourly)\n"
        "🛒 /shop        – View items\n"
        "🤖 /ai          – Ask me anything\n"
        "🔥 /roast       – Roast someone\n"
        "💀 /kill        – Kill someone (if no shield)\n"
        "💰 /rob         – Steal coins (reply)\n"
        "🎁 /gift        – Send a gift (reply)\n"
        "💎 /mygifts     – View your gifts\n\n"
        "👑 Owner tools: /analytics, /members, /top, /addcoins, /removecoins, /stats\n\n"
        "💡 Use /help for full list."
    )
    await update.message.reply_text(border_text("𝐆𝐑𝐎𝐔𝐏 𝐌𝐀𝐒𝐓𝐄𝐑 𝐄𝐋𝐈𝐓𝐄", content), parse_mode=ParseMode.HTML)

@safe_reply
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    content = (
        "👤 𝐌𝐄𝐌𝐁𝐄𝐑\n"
        "/rank – Your global stats\n"
        "/leaderboard – Top 10 global XP\n"
        "/riches – Top 10 global coins\n"
        "/grpleaderboard – Top 10 group XP\n"
        "/grpriches – Top 10 group coins\n"
        "/daily – Claim daily 100 coins\n"
        "/scratch – Try your luck\n"
        "/shop – View shop items\n"
        "/buy [item] – Buy item (shield, xp, lottery, reviveself, reviveother)\n"
        "/roast [@user/reply] – Roast someone\n"
        "/kill (reply) – Kill someone\n"
        "/rob [amount] (reply) – Steal coins\n"
        "/revive [@user] – Revive self (700) or other (800)\n"
        "/gift (reply) – Send a gift\n"
        "/mygifts – View gifts received\n"
        "/ai [question] – Ask AI\n\n"
        "👑 𝐎𝐖𝐍𝐄𝐑 (only who added bot)\n"
        "/analytics – Group insights\n"
        "/members – Member stats\n"
        "/top – Group's top 10 XP\n"
        "/addcoins (reply) – Add coins to user\n"
        "/removecoins (reply) – Remove coins from user\n"
        "/stats – List all groups bot is in\n\n"
        f"✨ Level up every {XP_PER_LEVEL} XP | Max Level {MAX_LEVEL} (Elite ☬)\n"
        f"🛡️ Shield ({SHIELD_COST} coins) protects from roast, rob, kill for {SHIELD_DURATION_HOURS}h."
    )
    await update.message.reply_text(border_text("𝐂𝐎𝐌𝐌𝐀𝐍𝐃 𝐋𝐈𝐒𝐓", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
async def rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name
    
    global_data = await db.get_user_global(user_id)
    if not global_data:
        await update.message.reply_text("❌ No stats yet. Start chatting!")
        return
    
    total_xp = global_data['total_xp']
    total_coins = global_data['total_coins']
    level = min(total_xp // XP_PER_LEVEL, MAX_LEVEL)
    symbol = get_level_symbol(level)
    xp_to_next = XP_PER_LEVEL - (total_xp % XP_PER_LEVEL) if level < MAX_LEVEL else 0
    total_msgs = await db.get_user_total_messages(user_id)
    
    progress = create_progress_bar(XP_PER_LEVEL - xp_to_next, XP_PER_LEVEL)
    
    content = (
        f"👤 @{username}\n"
        f"{'─' * 20}\n"
        f"✨ 𝐓𝐨𝐭𝐚𝐥 𝐗𝐏    : {format_number(total_xp)}\n"
        f"🆙 𝐋𝐞𝐯𝐞𝐥       : {level} {symbol}\n"
        f"📊 𝐏𝐫𝐨𝐠𝐫𝐞𝐬𝐬    : {progress}\n"
        f"💰 𝐓𝐨𝐭𝐚𝐥 𝐂𝐨𝐢𝐧𝐬 : {format_number(total_coins)}\n"
        f"📨 𝐌𝐞𝐬𝐬𝐚𝐠𝐞𝐬    : {format_number(total_msgs)}\n"
        f"{'─' * 20}\n"
        f"🕒 Last updated: just now"
    )
    await update.message.reply_text(border_text("𝐘𝐎𝐔𝐑 𝐆𝐋𝐎𝐁𝐀𝐋 𝐏𝐑𝐎𝐅𝐈𝐋𝐄", content), parse_mode=ParseMode.HTML)

@safe_reply
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await db.get_global_leaderboard(limit=10)
    if not rows:
        await update.message.reply_text("❌ No data yet.")
        return
    
    lines = []
    for i, row in enumerate(rows):
        name = row['username'] or "Unknown"
        level = min(row['level'], MAX_LEVEL)
        symbol = get_level_symbol(level)
        xp = format_number(row['total_xp'])
        lines.append(f"{MEDALS[i]} @{name} • L{level}{symbol} • {xp} XP")
    
    content = "\n".join(lines) + "\n\n🔥 Keep climbing the ranks!"
    await update.message.reply_text(border_text("𝐆𝐋𝐎𝐁𝐀𝐋 𝐗𝐏 𝐋𝐄𝐀𝐃𝐄𝐑𝐁𝐎𝐀𝐑𝐃", content), parse_mode=ParseMode.HTML)

@safe_reply
async def riches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await db.get_riches_leaderboard(limit=10)
    if not rows:
        await update.message.reply_text("❌ No data yet.")
        return
    
    lines = []
    for i, row in enumerate(rows):
        title = RICHES_TITLES[i] if i < len(RICHES_TITLES) else f"{i+1}."
        name = row['username'] or "Unknown"
        coins = format_number(row['total_coins'])
        lines.append(f"{title} @{name} • {coins} 💰")
    
    content = "\n".join(lines) + "\n\n💎 The richer, the better!"
    await update.message.reply_text(border_text("𝐆𝐋𝐎𝐁𝐀𝐋 𝐂𝐎𝐈𝐍 𝐇𝐎𝐀𝐑𝐃𝐄𝐑𝐒", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
async def grpleaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    rows = await db.get_group_leaderboard(chat_id, limit=10)
    if not rows:
        await update.message.reply_text("❌ No group stats yet.")
        return
    
    lines = []
    for i, row in enumerate(rows):
        name = row['username'] or "Unknown"
        level = min(row['level'], MAX_LEVEL)
        symbol = get_level_symbol(level)
        xp = format_number(row['xp'])
        lines.append(f"{MEDALS[i]} @{name} • L{level}{symbol} • {xp} XP")
    
    content = "\n".join(lines) + "\n\n🚀 Most active in this group!"
    await update.message.reply_text(border_text("𝐓𝐇𝐈𝐒 𝐆𝐑𝐎𝐔𝐏'𝐒 𝐗𝐏 𝐄𝐋𝐈𝐓𝐄", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
async def grpriches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    rows = await db.get_group_riches(chat_id, limit=10)
    if not rows:
        await update.message.reply_text("❌ No group stats yet.")
        return
    
    lines = []
    for i, row in enumerate(rows):
        name = row['username'] or "Unknown"
        coins = format_number(row['coins'])
        lines.append(f"{MEDALS[i]} @{name} • {coins} 💰")
    
    content = "\n".join(lines) + "\n\n💸 Spend wisely!"
    await update.message.reply_text(border_text("𝐓𝐇𝐈𝐒 𝐆𝐑𝐎𝐔𝐏'𝐒 𝐂𝐎𝐈𝐍 𝐊𝐈𝐍𝐆𝐒", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
@rate_limit_command("daily")
async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    username = update.effective_user.username or update.effective_user.first_name
    now = datetime.now(IST)
    
    user_data = await db.get_user_per_group(user_id, chat_id)
    if user_data and user_data.get('last_daily'):
        last = user_data['last_daily']
        if last.tzinfo is None:
            last = IST.localize(last)
        if (now - last) < timedelta(hours=DAILY_COOLDOWN_HOURS):
            remaining = timedelta(hours=DAILY_COOLDOWN_HOURS) - (now - last)
            hours, rem = divmod(int(remaining.total_seconds()), 3600)
            minutes = rem // 60
            content = f"⏳ Already claimed!\n{'─' * 20}\n⏰ Next in: {hours}h {minutes}m\n💎 Check back soon!"
            await update.message.reply_text(border_text("𝐂𝐎𝐎𝐋𝐃𝐎𝐖𝐍", content), parse_mode=ParseMode.HTML)
            return
    
    await db.update_user_global(user_id, username, coins_delta=DAILY_COINS)
    await db.update_user_per_group(user_id, chat_id, username, coins_delta=DAILY_COINS, last_daily=True)
    
    global_data = await db.get_user_global(user_id)
    new_balance = global_data['total_coins'] if global_data else DAILY_COINS
    
    content = (
        f"✨ +{DAILY_COINS} coins added!\n"
        f"{'─' * 20}\n"
        f"💰 Balance: {format_number(new_balance)}\n"
        f"📅 Next claim: in {DAILY_COOLDOWN_HOURS}h\n"
        f"💡 Tip: Try /scratch!"
    )
    await update.message.reply_text(border_text("𝐃𝐀𝐈𝐋𝐘 𝐑𝐄𝐖𝐀𝐑𝐃", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
@rate_limit_command("scratch")
async def scratch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    username = update.effective_user.username or update.effective_user.first_name
    now = datetime.now(IST)
    
    user_data = await db.get_user_per_group(user_id, chat_id)
    if user_data and user_data.get('last_scratch'):
        last = user_data['last_scratch']
        if last.tzinfo is None:
            last = IST.localize(last)
        if (now - last) < timedelta(hours=SCRATCH_COOLDOWN_HOURS):
            remaining = timedelta(hours=SCRATCH_COOLDOWN_HOURS) - (now - last)
            minutes = int(remaining.total_seconds()) // 60
            content = f"⏳ Already scratched!\n{'─' * 20}\n⏰ Next in: {minutes}m\n💡 Try /daily!"
            await update.message.reply_text(border_text("𝐂𝐎𝐎𝐋𝐃𝐎𝐖𝐍", content), parse_mode=ParseMode.HTML)
            return
    
    win = random.randint(1, 1000)
    await db.update_user_global(user_id, username, coins_delta=win)
    await db.update_user_per_group(user_id, chat_id, username, coins_delta=win, last_scratch=True)
    
    global_data = await db.get_user_global(user_id)
    new_balance = global_data['total_coins'] if global_data else win
    
    if win > 0:
        content = (
            f"🎉 You won {format_number(win)} coins!\n"
            f"{'─' * 20}\n"
            f"💰 Balance: {format_number(new_balance)}\n"
            f"✨ Lucky streak!"
        )
    else:
        content = (
            f"😬 No win this time!\n"
            f"{'─' * 20}\n"
            f"💰 Balance: {format_number(new_balance)}\n"
            f"🔄 Try again in 1h!"
        )
    await update.message.reply_text(border_text("𝐒𝐂𝐑𝐀𝐓𝐂𝐇 𝐂𝐀𝐑𝐃", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
async def shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton(f"🛡️ Shield ({SHIELD_COST} coins)", callback_data="buy_shield")],
        [InlineKeyboardButton("✨ Random XP (100 coins)", callback_data="buy_xp")],
        [InlineKeyboardButton("🎟️ Lottery Ticket (50 coins)", callback_data="buy_lottery")],
        [InlineKeyboardButton(f"💪 Revive Self ({REVIVE_SELF_COST} coins)", callback_data="buy_revive_self")],
        [InlineKeyboardButton(f"👥 Revive Other ({REVIVE_OTHER_COST} coins)", callback_data="buy_revive_other")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    content = "🛒 Select an item:"
    await update.message.reply_text(border_text("𝐒𝐇𝐎𝐏", content), reply_markup=reply_markup, parse_mode=ParseMode.HTML)

@safe_reply
async def buy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    username = query.from_user.username or query.from_user.first_name
    data = query.data
    
    user_per_group = await db.get_user_per_group(user_id, chat_id)
    if not user_per_group:
        await query.edit_message_text("❌ You need to chat first!")
        return
    
    coins = user_per_group['coins']
    
    if data == "buy_shield":
        if coins >= SHIELD_COST:
            expiry = datetime.now(IST) + timedelta(hours=SHIELD_DURATION_HOURS)
            await db.update_user_per_group(user_id, chat_id, username, coins_delta=-SHIELD_COST, shield_expiry=expiry)
            await db.update_user_global(user_id, username, coins_delta=-SHIELD_COST)
            content = f"🛡️ Shield active!\nProtected for {SHIELD_DURATION_HOURS}h.\n{'─' * 20}\n💰 Balance: {format_number(coins - SHIELD_COST)}"
            await query.edit_message_text(border_text("𝐏𝐔𝐑𝐂𝐇𝐀𝐒𝐄𝐃", content), parse_mode=ParseMode.HTML)
        else:
            content = f"❌ Need {SHIELD_COST} coins!\nYou have {format_number(coins)}.\n{'─' * 20}\n💰 Use /daily or /scratch!"
            await query.edit_message_text(border_text("𝐈𝐍𝐒𝐔𝐅𝐅𝐈𝐂𝐈𝐄𝐍𝐓", content), parse_mode=ParseMode.HTML)
    
    elif data == "buy_xp":
        price = 100
        if coins >= price:
            gain = random.randint(1, 50)
            await db.update_user_per_group(user_id, chat_id, username, coins_delta=-price, xp_delta=gain)
            await db.update_user_global(user_id, username, coins_delta=-price, xp_delta=gain)
            content = f"✨ +{gain} XP gained!\n{'─' * 20}\n💰 Balance: {format_number(coins - price)}"
            await query.edit_message_text(border_text("𝐏𝐔𝐑𝐂𝐇𝐀𝐒𝐄𝐃", content), parse_mode=ParseMode.HTML)
        else:
            content = f"❌ Need {price} coins!\nYou have {format_number(coins)}."
            await query.edit_message_text(border_text("𝐈𝐍𝐒𝐔𝐅𝐅𝐈𝐂𝐈𝐄𝐍𝐓", content), parse_mode=ParseMode.HTML)
    
    elif data == "buy_lottery":
        price = 50
        if coins >= price:
            win = random.choice([0, 100, 200, 500])
            await db.update_user_per_group(user_id, chat_id, username, coins_delta=-price + win)
            await db.update_user_global(user_id, username, coins_delta=-price + win)
            if win > 0:
                content = f"🎟️ Won {win} coins!\n{'─' * 20}\n💰 Balance: {format_number(coins - price + win)}"
            else:
                content = f"🎟️ No win!\n{'─' * 20}\n💰 Balance: {format_number(coins - price)}"
            await query.edit_message_text(border_text("𝐋𝐎𝐓𝐓𝐄𝐑𝐘", content), parse_mode=ParseMode.HTML)
        else:
            content = f"❌ Need {price} coins!\nYou have {format_number(coins)}."
            await query.edit_message_text(border_text("𝐈𝐍𝐒𝐔𝐅𝐅𝐈𝐂𝐈𝐄𝐍𝐓", content), parse_mode=ParseMode.HTML)
    
    elif data == "buy_revive_self":
        if not user_per_group.get('is_dead'):
            await query.edit_message_text("❌ You're already alive!")
            return
        if coins >= REVIVE_SELF_COST:
            await db.update_user_per_group(user_id, chat_id, username, coins_delta=-REVIVE_SELF_COST, set_dead=False)
            await db.update_user_global(user_id, username, coins_delta=-REVIVE_SELF_COST)
            content = f"💪 Revived!\n{'─' * 20}\n💰 Balance: {format_number(coins - REVIVE_SELF_COST)}"
            await query.edit_message_text(border_text("𝐑𝐄𝐕𝐈𝐕𝐄𝐃", content), parse_mode=ParseMode.HTML)
        else:
            content = f"❌ Need {REVIVE_SELF_COST} coins!\nYou have {format_number(coins)}."
            await query.edit_message_text(border_text("𝐈𝐍𝐒𝐔𝐅𝐅𝐈𝐂𝐈𝐄𝐍𝐓", content), parse_mode=ParseMode.HTML)
    
    elif data == "buy_revive_other":
        await query.edit_message_text("📝 Use /revive @username to revive someone else!")

@safe_reply
@require_group
@rate_limit_command("roast")
async def roast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    attacker = update.effective_user
    
    # Determine target
    target_user = None
    target_name = None
    
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        target_name = target_user.username or target_user.first_name
    elif context.args:
        target_name = context.args[0].lstrip('@')
        # Try to resolve username to user_id
        target_id = await db.find_user_by_username(chat_id, target_name)
        if target_id:
            try:
                member = await context.bot.get_chat_member(chat_id, target_id)
                target_user = member.user
            except:
                pass
    else:
        await update.message.reply_text("❌ Reply to someone or use /roast @username")
        return
    
    # Self-roast check
    if target_user and target_user.id == attacker.id:
        await update.message.reply_text("🤡 Why roast yourself? Get help.")
        return
    
    # Bot roast
    if target_user and target_user.id == context.bot.id:
        roast_text = await ai_service.roast(context.bot.first_name, "roasting myself")
        content = f"🔥 {roast_text}\n{'─' * 20}\n💀 Self-aware!"
        await update.message.reply_text(border_text("𝐑𝐎𝐀𝐒𝐓", content), parse_mode=ParseMode.HTML)
        return
    
    # Check if target is owner
    if target_user:
        target_data = await db.get_user_per_group(target_user.id, chat_id)
        if target_data and target_data.get('is_verified_owner') == 1:
            roast_text = await ai_service.roast(attacker.first_name, "tried to roast owner")
            content = f"🔥 @{attacker.username or attacker.first_name}, {roast_text}\n{'─' * 20}\n💀 Backfire!"
            await update.message.reply_text(border_text("𝐑𝐎𝐀𝐒𝐓", content), parse_mode=ParseMode.HTML)
            return
        
        # Check shield
        if has_active_shield(target_data):
            content = f"🛡️ @{target_user.username or target_user.first_name} is shielded!"
            await update.message.reply_text(border_text("𝐒𝐇𝐈𝐄𝐋𝐃", content), parse_mode=ParseMode.HTML)
            return
    
    # Perform roast
    roast_text = await ai_service.roast(target_name)
    content = f"🔥 @{target_name}, {roast_text}\n{'─' * 20}\n💀 Destroyed!"
    await update.message.reply_text(border_text("𝐑𝐎𝐀𝐒𝐓", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
@rate_limit_command("kill")
async def kill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to someone to kill them!")
        return
    
    target = update.message.reply_to_message.from_user
    attacker = update.effective_user
    chat_id = update.effective_chat.id
    
    if target.id == context.bot.id:
        await update.message.reply_text("😵 I'm immortal!")
        return
    if target.id == attacker.id:
        await update.message.reply_text("🤡 Can't kill yourself.")
        return
    
    target_data = await db.get_user_per_group(target.id, chat_id)
    
    # Owner protection
    if target_data and target_data.get('is_verified_owner') == 1:
        await db.update_user_per_group(attacker.id, chat_id, attacker.username or attacker.first_name, set_dead=True)
        await update.message.reply_text(f"⚰️ @{attacker.username or attacker.first_name} tried to kill the owner and died!")
        return
    
    # Shield check
    if has_active_shield(target_data):
        content = f"🛡️ @{target.username or target.first_name} is shielded!"
        await update.message.reply_text(border_text("𝐒𝐇𝐈𝐄𝐋𝐃", content), parse_mode=ParseMode.HTML)
        return
    
    # Already dead check
    if target_data and target_data.get('is_dead'):
        await update.message.reply_text(f"💀 @{target.username or target.first_name} is already dead!")
        return
    
    # Kill
    await db.update_user_per_group(target.id, chat_id, target.username or target.first_name, set_dead=True)
    content = f"🔪 @{attacker.username or attacker.first_name} killed @{target.username or target.first_name}!\n💀 RIP"
    await update.message.reply_text(border_text("𝐊𝐈𝐋𝐋", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
@rate_limit_command("rob")
async def rob(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to someone to rob them!")
        return
    if not context.args:
        await update.message.reply_text("❌ Usage: /rob <amount> (reply to user)")
        return
    
    try:
        amount = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Amount must be a number.")
        return
    
    if amount <= 0:
        await update.message.reply_text("❌ Amount must be positive.")
        return
    if amount > 10000:
        await update.message.reply_text("❌ Max rob amount is 10,000 coins.")
        return
    
    target = update.message.reply_to_message.from_user
    thief = update.effective_user
    chat_id = update.effective_chat.id
    
    if target.id == thief.id:
        await update.message.reply_text("🤡 Can't rob yourself.")
        return
    if target.id == context.bot.id:
        await update.message.reply_text("😤 I have no coins.")
        return
    
    target_data = await db.get_user_per_group(target.id, chat_id)
    thief_data = await db.get_user_per_group(thief.id, chat_id)
    
    if not thief_data:
        await update.message.reply_text("❌ Chat first to earn coins.")
        return
    
    # Owner protection
    if target_data and target_data.get('is_verified_owner') == 1:
        fine = min(amount * 2, thief_data['coins'])
        await db.update_user_per_group(thief.id, chat_id, thief.username or thief.first_name, coins_delta=-fine)
        await db.update_user_global(thief.id, thief.username or thief.first_name, coins_delta=-fine)
        content = f"👑 Owner protection! Fined {format_number(fine)} coins.\n💰 New balance: {format_number(thief_data['coins'] - fine)}"
        await update.message.reply_text(border_text("𝐑𝐎𝐁 𝐅𝐀𝐈𝐋𝐄𝐃", content), parse_mode=ParseMode.HTML)
        return
    
    # Shield check
    if has_active_shield(target_data):
        content = f"🛡️ @{target.username or target.first_name} is shielded!"
        await update.message.reply_text(border_text("𝐒𝐇𝐈𝐄𝐋𝐃", content), parse_mode=ParseMode.HTML)
        return
    
    # Dead check
    if target_data and target_data.get('is_dead'):
        await update.message.reply_text(f"💀 @{target.username or target.first_name} is dead. No coins to steal.")
        return
    
    # Check target balance
    if not target_data or target_data['coins'] < amount:
        await update.message.reply_text(f"❌ @{target.username or target.first_name} doesn't have that many coins.")
        return
    
    # Atomic transfer
    success, msg = await db.transfer_coins(
        target.id, thief.id, chat_id, amount,
        target.username or target.first_name,
        thief.username or thief.first_name
    )
    
    if success:
        thief_new = thief_data['coins'] + amount
        content = f"💰 Robbed {format_number(amount)} coins from @{target.username or target.first_name}!\n💰 Your balance: {format_number(thief_new)}"
        await update.message.reply_text(border_text("𝐑𝐎𝐁", content), parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ {msg}")

@safe_reply
@require_group
async def revive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    username = update.effective_user.username or update.effective_user.first_name
    
    if context.args:
        # Revive other
        target_name = context.args[0].lstrip('@')
        target_id = await db.find_user_by_username(chat_id, target_name)
        
        if not target_id:
            await update.message.reply_text("❌ User not found in this group.")
            return
        
        target_data = await db.get_user_per_group(target_id, chat_id)
        if not target_data or not target_data.get('is_dead'):
            await update.message.reply_text(f"❌ @{target_name} is already alive.")
            return
        
        self_data = await db.get_user_per_group(user_id, chat_id)
        if not self_data or self_data['coins'] < REVIVE_OTHER_COST:
            await update.message.reply_text(f"❌ Need {REVIVE_OTHER_COST} coins to revive someone else.")
            return
        
        await db.update_user_per_group(target_id, chat_id, target_name, set_dead=False)
        await db.update_user_per_group(user_id, chat_id, username, coins_delta=-REVIVE_OTHER_COST)
        await db.update_user_global(user_id, username, coins_delta=-REVIVE_OTHER_COST)
        
        content = f"💪 @{username} revived @{target_name}!\n💰 Spent {REVIVE_OTHER_COST} coins."
        await update.message.reply_text(border_text("𝐑𝐄𝐕𝐈𝐕𝐄", content), parse_mode=ParseMode.HTML)
    else:
        # Revive self
        user_data = await db.get_user_per_group(user_id, chat_id)
        if not user_data:
            await update.message.reply_text("❌ Chat first to earn coins.")
            return
        if not user_data.get('is_dead'):
            await update.message.reply_text("❌ You're already alive.")
            return
        if user_data['coins'] < REVIVE_SELF_COST:
            await update.message.reply_text(f"❌ Need {REVIVE_SELF_COST} coins to revive yourself.")
            return
        
        await db.update_user_per_group(user_id, chat_id, username, coins_delta=-REVIVE_SELF_COST, set_dead=False)
        await db.update_user_global(user_id, username, coins_delta=-REVIVE_SELF_COST)
        
        content = f"💪 Revived!\n💰 Balance: {format_number(user_data['coins'] - REVIVE_SELF_COST)}"
        await update.message.reply_text(border_text("𝐑𝐄𝐕𝐈𝐕𝐄", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
async def gift(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to someone to send a gift!")
        return
    
    target = update.message.reply_to_message.from_user
    sender = update.effective_user
    chat_id = update.effective_chat.id
    
    if target.id == sender.id:
        await update.message.reply_text("❌ Can't gift yourself.")
        return
    
    await pending_manager.set(chat_id, sender.id, {
        "action": "gift",
        "target_id": target.id,
        "target_name": target.username or target.first_name
    })
    
    keyboard = [
        [InlineKeyboardButton(f"{info['emoji']} {gtype.capitalize()} ({info['price']})", callback_data=f"gift_{gtype}")]
        for gtype, info in GIFT_TYPES.items()
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("🎁 Choose a gift:", reply_markup=reply_markup)

@safe_reply
async def gift_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    data = query.data
    
    if not data.startswith("gift_"):
        return
    
    gift_type = data[5:]
    if gift_type not in GIFT_TYPES:
        return
    
    pending = await pending_manager.get(chat_id, user_id)
    if not pending or pending.get("action") != "gift":
        await query.edit_message_text("❌ No pending gift. Use /gift again.")
        return
    
    target_id = pending["target_id"]
    target_name = pending["target_name"]
    price = GIFT_TYPES[gift_type]["price"]
    emoji = GIFT_TYPES[gift_type]["emoji"]
    
    sender_data = await db.get_user_per_group(user_id, chat_id)
    if not sender_data or sender_data['coins'] < price:
        await query.edit_message_text(f"❌ Need {price} coins for this gift.")
        await pending_manager.delete(chat_id, user_id)
        return
    
    # Deduct and record
    await db.update_user_per_group(user_id, chat_id, query.from_user.username or query.from_user.first_name, coins_delta=-price)
    await db.update_user_global(user_id, query.from_user.username or query.from_user.first_name, coins_delta=-price)
    await db.add_gift(user_id, target_id, chat_id, gift_type, price)
    
    content = f"🎁 Sent {emoji} {gift_type} to @{target_name}!"
    await query.edit_message_text(border_text("𝐆𝐈𝐅𝐓 𝐒𝐄𝐍𝐓", content), parse_mode=ParseMode.HTML)
    await context.bot.send_message(chat_id, f"@{target_name} check your gifts! 🎁")
    
    await pending_manager.delete(chat_id, user_id)

@safe_reply
async def mygifts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    gifts = await db.get_gifts(user_id, limit=10)
    
    if not gifts:
        await update.message.reply_text("❌ No gifts received yet.")
        return
    
    lines = []
    for g in gifts:
        from_data = await db.get_user_global(g['from_user'])
        from_un = from_data['username'] if from_data else str(g['from_user'])
        emoji = GIFT_TYPES.get(g['gift_type'], {}).get('emoji', '🎁')
        date_str = g['created_at'].strftime('%Y-%m-%d') if g['created_at'] else 'Unknown'
        lines.append(f"{emoji} From @{from_un} • {g['gift_type'].capitalize()} • {format_number(g['amount'])} • {date_str}")
    
    content = "\n".join(lines)
    await update.message.reply_text(border_text("𝐘𝐎𝐔𝐑 𝐆𝐈𝐅𝐓𝐒", content), parse_mode=ParseMode.HTML)

@safe_reply
@rate_limit_command("ai")
async def ai_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prompt = " ".join(context.args)
    if not prompt:
        await update.message.reply_text("❌ Usage: /ai <question>")
        return
    
    await update.message.reply_chat_action("typing")
    response = await ai_service.ask(prompt)
    await update.message.reply_text(border_text("𝐀𝐈 𝐀𝐒𝐒𝐈𝐒𝐓𝐀𝐍𝐓", response), parse_mode=ParseMode.HTML)

# =============================================================================
# SECTION 15: OWNER COMMANDS
# =============================================================================

@safe_reply
@require_group
async def analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update, context):
        return
    
    chat_id = update.effective_chat.id
    stats = await db.get_group_stats(chat_id)
    
    top_name = stats['top_user']['username'] if stats['top_user'] else "N/A"
    top_xp = stats['top_user']['xp'] if stats['top_user'] else 0
    
    content = (
        f"📊 Group Insights\n"
        f"{'─' * 20}\n"
        f"👥 Tracked users: {stats['user_count']}\n"
        f"💬 Top chatter: @{top_name} ({format_number(top_xp)} XP)\n"
        f"🏥 Status: Active\n"
        f"🕒 {datetime.now(IST).strftime('%I:%M %p IST')}"
    )
    await update.message.reply_text(border_text("𝐀𝐍𝐀𝐋𝐘𝐓𝐈𝐂𝐒", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
async def members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update, context):
        return
    
    chat_id = update.effective_chat.id
    try:
        total = await context.bot.get_chat_member_count(chat_id)
        admins = await context.bot.get_chat_administrators(chat_id)
        creator = next((a.user.first_name for a in admins if a.status == "creator"), "Unknown")
    except Exception as e:
        logger.error(f"Members error: {e}")
        await update.message.reply_text("❌ Could not fetch members.")
        return
    
    content = (
        f"👥 Total: {total}\n"
        f"🛠️ Admins: {len(admins)}\n"
        f"👑 Founder: {creator}\n"
        f"{'─' * 20}\n"
        f"📌 {update.effective_chat.title}\n"
        f"🆔 {chat_id}"
    )
    await update.message.reply_text(border_text("𝐌𝐄𝐌𝐁𝐄𝐑𝐒", content), parse_mode=ParseMode.HTML)

@safe_reply
@require_group
async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update, context):
        return
    await grpleaderboard(update, context)

@safe_reply
@require_group
async def addcoins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update, context):
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to a user with /addcoins")
        return
    
    target = update.message.reply_to_message.from_user
    chat_id = update.effective_chat.id
    owner_id = update.effective_user.id
    
    await pending_manager.set(chat_id, owner_id, {
        "action": "addcoins",
        "target_id": target.id,
        "target_name": target.username or target.first_name
    })
    await update.message.reply_text("💰 How many coins to add?")

@safe_reply
@require_group
async def removecoins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner(update, context):
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to a user with /removecoins")
        return
    
    target = update.message.reply_to_message.from_user
    chat_id = update.effective_chat.id
    owner_id = update.effective_user.id
    
    await pending_manager.set(chat_id, owner_id, {
        "action": "removecoins",
        "target_id": target.id,
        "target_name": target.username or target.first_name
    })
    await update.message.reply_text("💰 How many coins to remove?")

@safe_reply
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != GLOBAL_OWNER_ID:
        await update.message.reply_text("❌ Only global owner can use this.")
        return
    
    groups = await db.get_all_groups()
    if not groups:
        await update.message.reply_text("❌ No groups found.")
        return
    
    lines = [f"📌 Total Groups: {len(groups)}", ""]
    for g in groups:
        title = g['title'] or "Unknown"
        chat_id = g['chat_id']
        username = g.get('username') or "private"
        added = g['added_on'].strftime('%Y-%m-%d') if g['added_on'] else "unknown"
        try:
            members = await context.bot.get_chat_member_count(chat_id)
        except:
            members = "?"
        lines.append(f"{title} (ID: {chat_id})")
        lines.append(f"   👥 {members} members | 📅 {added} | 🔗 @{username}")
    
    content = "\n".join(lines)
    await update.message.reply_text(border_text("𝐁𝐎𝐓 𝐆𝐑𝐎𝐔𝐏𝐒", content), parse_mode=ParseMode.HTML)

# =============================================================================
# SECTION 16: MESSAGE HANDLERS
# =============================================================================

async def log_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle message XP and pending actions."""
    if not update.message or not update.effective_chat:
        return
    
    # Skip commands
    if update.message.text and update.message.text.startswith('/'):
        return
    
    # Only process in groups
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    username = update.effective_user.username or update.effective_user.first_name
    text = update.message.text or ""
    
    # Check for pending action first
    pending = await pending_manager.get(chat_id, user_id)
    if pending:
        await handle_pending(update, pending)
        return
    
    # Process XP for normal messages
    if text:
        await process_message_xp(user_id, username, chat_id, context)

async def handle_pending(update: Update, pending: Dict[str, Any]):
    """Handle pending actions from numeric/text replies."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()
    action = pending.get("action")
    target_id = pending.get("target_id")
    target_name = pending.get("target_name", "Unknown")
    
    if action in ("addcoins", "removecoins"):
        if not text.isdigit():
            await update.message.reply_text("❌ Enter a valid number.")
            return
        
        amount = int(text)
        if amount <= 0:
            await update.message.reply_text("❌ Amount must be positive.")
            return
        if amount > 1000000:
            await update.message.reply_text("❌ Max amount is 1,000,000.")
            return
        
        if action == "addcoins":
            await db.update_user_per_group(target_id, chat_id, target_name, coins_delta=amount)
            await db.update_user_global(target_id, target_name, coins_delta=amount)
            verb = "added to"
        else:
            target_per = await db.get_user_per_group(target_id, chat_id)
            if target_per and target_per['coins'] < amount:
                await update.message.reply_text(f"❌ @{target_name} only has {target_per['coins']} coins.")
                await pending_manager.delete(chat_id, user_id)
                return
            await db.update_user_per_group(target_id, chat_id, target_name, coins_delta=-amount)
            await db.update_user_global(target_id, target_name, coins_delta=-amount)
            verb = "removed from"
        
        new_global = await db.get_user_global(target_id)
        new_per = await db.get_user_per_group(target_id, chat_id)
        content = (
            f"✅ {format_number(amount)} coins {verb} @{target_name}.\n"
            f"{'─' * 20}\n"
            f"💎 Global: {format_number(new_global['total_coins'])}\n"
            f"📊 Group: {format_number(new_per['coins'])}"
        )
        await update.message.reply_text(border_text("𝐂𝐎𝐈𝐍𝐒 𝐔𝐏𝐃𝐀𝐓𝐄𝐃", content), parse_mode=ParseMode.HTML)
        await pending_manager.delete(chat_id, user_id)

# =============================================================================
# SECTION 17: CHAT MEMBER HANDLERS
# =============================================================================

async def track_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Track bot addition/removal from groups."""
    result = update.my_chat_member
    if not result:
        return
    
    chat = result.chat
    user = result.from_user
    new_status = result.new_chat_member.status if result.new_chat_member else None
    old_status = result.old_chat_member.status if result.old_chat_member else None
    
    if new_status == "member" and old_status == "left":
        # Bot added
        await db.add_group(chat.id, chat.title, chat.username, chat.invite_link)
        await db.update_user_per_group(user.id, chat.id, user.username or user.first_name, is_verified_owner=1)
        try:
            await context.bot.send_message(chat.id, f"✅ Bot added! {user.first_name} is now the group owner.")
        except:
            pass
        logger.info(f"Bot added to group {chat.id} ({chat.title}) by {user.id}")
    
    elif new_status == "left" and old_status == "member":
        # Bot removed
        await db.remove_group(chat.id)
        logger.info(f"Bot removed from group {chat.id}")

# =============================================================================
# SECTION 18: BOT INITIALIZATION
# =============================================================================

async def post_init(application):
    """Initialize database on startup."""
    await db.initialize()
    await db.init_schema()
    logger.info("Bot initialized and ready!")

async def post_shutdown(application):
    """Cleanup on shutdown."""
    await db.close()
    logger.info("Bot shutdown complete.")

# =============================================================================
# SECTION 19: MAIN ENTRY POINT (WEBHOOK MODE)
# =============================================================================

def main():
    # Validate config
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not set!")
        sys.exit(1)
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set!")
        sys.exit(1)
    if not WEBHOOK_URL:
        logger.error("WEBHOOK_URL not set! Required for webhook mode.")
        sys.exit(1)
    
    # Build application
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    
    # Command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("rank", rank))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("riches", riches))
    application.add_handler(CommandHandler("grpleaderboard", grpleaderboard))
    application.add_handler(CommandHandler("grpriches", grpriches))
    application.add_handler(CommandHandler("daily", daily))
    application.add_handler(CommandHandler("scratch", scratch))
    application.add_handler(CommandHandler("shop", shop))
    application.add_handler(CommandHandler("roast", roast))
    application.add_handler(CommandHandler("kill", kill))
    application.add_handler(CommandHandler("rob", rob))
    application.add_handler(CommandHandler("revive", revive))
    application.add_handler(CommandHandler("gift", gift))
    application.add_handler(CommandHandler("mygifts", mygifts))
    application.add_handler(CommandHandler("ai", ai_cmd))
    
    # Owner commands
    application.add_handler(CommandHandler("analytics", analytics))
    application.add_handler(CommandHandler("members", members))
    application.add_handler(CommandHandler("top", top))
    application.add_handler(CommandHandler("addcoins", addcoins))
    application.add_handler(CommandHandler("removecoins", removecoins))
    application.add_handler(CommandHandler("stats", stats))
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(buy_callback, pattern="^buy_"))
    application.add_handler(CallbackQueryHandler(gift_callback, pattern="^gift_"))
    
    # Message handlers (single handler for both XP and pending)
    application.add_handler(
        MessageHandler(
            filters.TEXT & filters.ChatType.GROUPS & ~filters.COMMAND,
            log_messages
        )
    )
    
    # Chat member handler
    application.add_handler(ChatMemberHandler(track_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    
    # Start webhook
    logger.info(f"Starting webhook on {WEBHOOK_LISTEN}:{PORT} with URL {WEBHOOK_URL}/{BOT_TOKEN}")
    application.run_webhook(
        listen=WEBHOOK_LISTEN,
        port=PORT,
        url_path=BOT_TOKEN,               # secret path using bot token
        webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        secret_token=None,                 # optional: add a secret for extra security
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )

if __name__ == "__main__":
    main()
