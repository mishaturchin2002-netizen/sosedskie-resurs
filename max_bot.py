"""
«Соседский ресурс» — чат-бот для мессенджера MAX.
==================================================

Один файл: модели SQLAlchemy 2.0 (async), бизнес-логика, клавиатуры,
Long Polling на aiohttp и фоновые напоминания.

УСТАНОВКА
---------
    pip install "SQLAlchemy[asyncio]>=2.0,<2.1" "aiosqlite>=0.20" "aiohttp>=3.9" "python-dotenv>=1.0"

ЗАПУСК
------
    1. Создайте бота на платформе MAX (business.max.ru или «MAX для бизнеса»).
    2. Получите access_token бота.
    3. Скопируйте .env.example в .env.
    4. Впишите MAX_TOKEN и MAX_ADMIN_ID.
    5. python max_bot.py

ВАЖНО ПРО MAX API
-----------------
* Используется актуальный домен https://platform-api2.max.ru.
* Токен передаётся в заголовке Authorization, а не в query-параметрах.
* Для работы Long Polling бот не должен быть подписан на Webhook.
* Long Polling в MAX предназначен для разработки и тестирования. Для
  production документация MAX рекомендует Webhook.
* На некоторых машинах для platform-api2.max.ru может потребоваться
  сертификат Минцифры в доверенных корневых сертификатах.
* Время в боте — локальное время сервера (naive datetime), как и в VK-версии.
* SQLite-файл создаётся рядом с max_bot.py. При изменении схемы удалите
  старую базу: для MVP миграции не подключены.
* В групповых чатах MAX бот должен быть администратором с правом
  read_all_messages, иначе он не получит обычные сообщения участников.
* Схема БД и бизнес-логика совпадают с VK-версией; отличается только
  транспорт MAX (aiohttp + Long Polling) и формат клавиатур.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text as SAText,
    UniqueConstraint,
    desc,
    event,
    func,
    select,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ============================================================================
# НАСТРОЙКИ — ЧИТАЮТСЯ ИЗ .env / ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
# ============================================================================

load_dotenv(Path(__file__).resolve().with_name(".env"))

MAX_TOKEN = os.getenv("MAX_TOKEN", "").strip()
try:
    ADMIN_ID = int(os.getenv("MAX_ADMIN_ID", "0"))
except ValueError:
    ADMIN_ID = 0


# Актуальный Bot API MAX. При необходимости можно переопределить.
MAX_API_BASE = os.getenv("MAX_API_BASE", "https://platform-api2.max.ru")

# ============================================================================
# КОНСТАНТЫ И ЛОГИРОВАНИЕ
# ============================================================================

DB_PATH = Path(__file__).resolve().with_name("sosedskiy_resurs_max.db")
DATABASE_URL = os.getenv(
    "MAX_DATABASE_URL",
    f"sqlite+aiosqlite:///{DB_PATH.as_posix()}",
)

MAX_TEXT_LIMIT = 4000
MAX_CALLBACK_PAYLOAD_LIMIT = 1024
UPDATE_TYPES = "message_created,message_callback,bot_started"
LONG_POLL_TIMEOUT = 30
LONG_POLL_LIMIT = 100
API_TIMEOUT_SECONDS = 30

MIN_BOOKING_MINUTES = 15
MAX_BOOKING_HOURS = 24
RETURN_GRACE_MINUTES = 0
REMINDER_MINUTES = 15
REMINDER_INTERVAL_SECONDS = 60

CANCEL_PENALTY = -5
LATE_CANCEL_PENALTY = -3
LATE_RETURN_PENALTY = -2
ON_TIME_BONUS = 1

SAVED_RUB_PER_BOOKING = 500
MAX_IDEAS_PER_DAY = 3
IDEA_VOTES_THRESHOLD = 10
VIOLATION_WINDOW_DAYS = 30
VIOLATION_LIMIT = 2
BLOCK_HOURS = 24
SUGGESTION_STEP_MINUTES = 30
SUGGESTION_DAYS_BEFORE = 3
SUGGESTION_DAYS_AFTER = 14

LEVELS: tuple[tuple[int, str, str], ...] = (
    (30, "Староста", "👑"),
    (10, "Сосед", "🤝"),
    (0, "Новичок", "🌱"),
)

WEEKDAYS = ("пн", "вт", "ср", "чт", "пт", "сб", "вс")

# Формат из задания: ДД.ММ.ГГГГ ЧЧ:ММ. Дополнительно разрешаем «/» и «-»
# в качестве разделителей даты, чтобы пользователю было проще.
DATETIME_RE = re.compile(
    r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})\s+(\d{1,2}):(\d{1,2})"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("sosedskiy-resurs-max")

# In-memory состояния диалогов. После рестарта незавершённый ввод начнётся заново.
USER_STATES: dict[int, dict[str, Any]] = {}

# Сериализует проверку пересечений и создание броней внутри процесса.
BOOKING_LOCK = asyncio.Lock()

# Защищает от двойного нажатия «Вернуть»/«Отменить».
BOOKING_ACTION_LOCK = asyncio.Lock()

# Глобальный API-клиент. Инициализируется в main() внутри работающего event loop.
API: "MaxAPI | None" = None


# ============================================================================
# БАЗА ДАННЫХ: МОДЕЛИ (структура сохранена как в VK-версии)
# ============================================================================


def now_naive() -> datetime:
    """Локальное «настенное» время без tzinfo — удобно для SQLite."""
    return datetime.now().replace(microsecond=0)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    first_name: Mapped[str] = mapped_column(String(64), default="")
    last_name: Mapped[str] = mapped_column(String(64), default="")
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    points: Mapped[int] = mapped_column(Integer, default=0)
    total_saved_rub: Mapped[int] = mapped_column(Integer, default=0)
    blocked_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now_naive)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=now_naive, onupdate=now_naive
    )


class Resource(Base):
    __tablename__ = "resources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(80), unique=True)
    description: Mapped[str] = mapped_column(SAText, default="")
    emoji: Mapped[str] = mapped_column(String(8), default="📦")
    default_duration_minutes: Mapped[int] = mapped_column(Integer, default=120)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now_naive)


class Booking(Base):
    __tablename__ = "bookings"
    __table_args__ = (
        Index("ix_bookings_resource_time", "resource_id", "start_time", "end_time"),
        Index("ix_bookings_user_status", "user_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource_id: Mapped[int] = mapped_column(
        ForeignKey("resources.id", ondelete="RESTRICT"), index=True
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    start_time: Mapped[datetime] = mapped_column(DateTime, index=True)
    end_time: Mapped[datetime] = mapped_column(DateTime, index=True)
    # active | completed | cancelled
    status: Mapped[str] = mapped_column(String(16), default="active", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now_naive)
    returned_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    notified_15m: Mapped[bool] = mapped_column(Boolean, default=False)


class ReputationLog(Base):
    __tablename__ = "reputation_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    delta: Mapped[int] = mapped_column(Integer)
    reason: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now_naive, index=True)


class Poll(Base):
    __tablename__ = "polls"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    question: Mapped[str] = mapped_column(String(200))
    description: Mapped[str] = mapped_column(SAText, default="")
    budget_rub: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    is_user_idea: Mapped[bool] = mapped_column(Boolean, default=False)
    idea_notified: Mapped[bool] = mapped_column(Boolean, default=False)
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now_naive, index=True)


class PollVote(Base):
    __tablename__ = "poll_votes"
    __table_args__ = (UniqueConstraint("poll_id", "user_id", name="uq_poll_vote_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    poll_id: Mapped[int] = mapped_column(
        ForeignKey("polls.id", ondelete="CASCADE"), index=True
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    # +1 — «За», -1 — «Против».
    value: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now_naive)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=now_naive, onupdate=now_naive
    )


class DamageReport(Base):
    __tablename__ = "damage_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    description: Mapped[str] = mapped_column(SAText)
    # В MAX здесь храним token/photo_id изображения; полный URL — в photo_url.
    photo_attachment: Mapped[str | None] = mapped_column(String(255), nullable=True)
    photo_url: Mapped[str | None] = mapped_column(SAText, nullable=True)
    # new | in_progress | resolved
    status: Mapped[str] = mapped_column(String(16), default="new", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now_naive, index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


# ============================================================================
# БАЗА ДАННЫХ: ПОДКЛЮЧЕНИЕ
# ============================================================================

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    connect_args={"timeout": 30},
)
SessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


@event.listens_for(engine.sync_engine, "connect")
def _sqlite_pragmas(dbapi_connection: Any, _connection_record: Any) -> None:
    """Включаем внешние ключи и разумный busy-timeout для SQLite."""
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=5000")
    finally:
        cursor.close()


DEFAULT_RESOURCES: tuple[dict[str, Any], ...] = (
    {
        "name": "Мангал",
        "emoji": "🔥",
        "description": (
            "Большой угольный мангал с решёткой, шампурами и щипцами. "
            "Стоит у беседки №2. Уголь и розжиг — свои."
        ),
        "default_duration_minutes": 120,
    },
    {
        "name": "Коворкинг",
        "emoji": "💻",
        "description": (
            "Тихая комната в клубе: стол на 4 человека, монитор, проектор, "
            "Wi-Fi и удлинители. Можно проводить созвоны и мини-встречи."
        ),
        "default_duration_minutes": 180,
    },
    {
        "name": "Инструменты",
        "emoji": "🧰",
        "description": (
            "Набор соседского инструмента: шуруповёрт, дрель, уровень, "
            "рулетка, молоток и набор бит. Выдаём в ящике у старшего по подъезду."
        ),
        "default_duration_minutes": 240,
    },
)

DEFAULT_POLLS: tuple[tuple[str, str, int], ...] = (
    (
        "Купить новый шредер для коворкинга?",
        "Старый шредер забивается и рвёт бумагу. Ориентир по бюджету — 18 000 ₽. "
        "Голосуем, запускать ли сбор.",
        18000,
    ),
    (
        "Поставить второй мангал у пруда?",
        "Летом одного мангала не хватает. Предлагаем поставить ещё один "
        "у большой беседки. Ориентир — 12 000 ₽.",
        12000,
    ),
)


async def _table_columns(connection: Any, table: str) -> set[str]:
    result = await connection.exec_driver_sql(f"PRAGMA table_info({table})")
    return {str(row[1]) for row in result.fetchall()}


async def _ensure_sqlite_column(
    connection: Any,
    table: str,
    column: str,
    ddl: str,
) -> None:
    columns = await _table_columns(connection, table)
    if column not in columns:
        await connection.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        logger.info("Миграция БД: добавлена колонка %s.%s", table, column)


async def migrate_sqlite_schema(connection: Any) -> None:
    """Мягкая миграция уже работающей SQLite-базы MAX-бота."""
    await _ensure_sqlite_column(
        connection,
        "users",
        "total_saved_rub",
        "total_saved_rub INTEGER NOT NULL DEFAULT 0",
    )
    await _ensure_sqlite_column(
        connection,
        "users",
        "blocked_until",
        "blocked_until DATETIME",
    )
    await _ensure_sqlite_column(
        connection,
        "polls",
        "is_user_idea",
        "is_user_idea BOOLEAN NOT NULL DEFAULT 0",
    )
    await _ensure_sqlite_column(
        connection,
        "polls",
        "idea_notified",
        "idea_notified BOOLEAN NOT NULL DEFAULT 0",
    )
    await connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_polls_is_user_idea ON polls (is_user_idea)"
    )


async def init_db() -> None:
    """Создаёт таблицы, выполняет мягкие миграции и наполняет стартовые данные."""
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await migrate_sqlite_schema(connection)

    async with SessionLocal() as session:
        resources_count = (
            await session.execute(select(func.count()).select_from(Resource))
        ).scalar_one()
        if resources_count == 0:
            for item in DEFAULT_RESOURCES:
                session.add(Resource(**item))
            logger.info("Добавлены стартовые ресурсы: %s", len(DEFAULT_RESOURCES))

        polls_count = (
            await session.execute(select(func.count()).select_from(Poll))
        ).scalar_one()
        if polls_count == 0:
            for question, description, budget in DEFAULT_POLLS:
                session.add(
                    Poll(
                        question=question,
                        description=description,
                        budget_rub=budget,
                        is_active=True,
                    )
                )
            logger.info("Добавлены стартовые голосования: %s", len(DEFAULT_POLLS))

        await session.commit()


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================


def shorten(text: str, limit: int = 40) -> str:
    """Обрезает строку под ограничение длины."""
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def clip_text(text: str, limit: int = MAX_TEXT_LIMIT) -> str:
    """MAX принимает не более 4000 символов в одном сообщении."""
    text = text or ""
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def points_word(number: int) -> str:
    """Правильное склонение слова «балл»."""
    n = abs(int(number)) % 100
    if 11 <= n <= 14:
        return "баллов"
    n %= 10
    if n == 1:
        return "балл"
    if 2 <= n <= 4:
        return "балла"
    return "баллов"


def fmt_dt(value: datetime) -> str:
    return value.strftime("%d.%m.%Y %H:%M")


def fmt_dt_short(value: datetime) -> str:
    return f"{WEEKDAYS[value.weekday()]}, {value.strftime('%d.%m %H:%M')}"


def fmt_interval(start: datetime, end: datetime) -> str:
    return f"{fmt_dt(start)} — {fmt_dt(end)}"


def fmt_money(amount: int) -> str:
    return f"{int(amount or 0):,}".replace(",", " ")


def parse_iso_datetime(value: Any) -> datetime | None:
    """ISO-дата из callback payload; timezone-aware значения отклоняем."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is not None:
        return None
    return parsed


def ceil_to_step(
    value: datetime,
    step_minutes: int = SUGGESTION_STEP_MINUTES,
) -> datetime:
    base = value.replace(second=0, microsecond=0)
    if base < value:
        base += timedelta(minutes=1)
    remainder = base.minute % step_minutes
    if remainder:
        base += timedelta(minutes=step_minutes - remainder)
    return base


def level_info(points: int) -> tuple[str, str, int, int | None]:
    """Возвращает (название, иконка, нижняя граница, следующая граница)."""
    for threshold, name, icon in LEVELS:
        if points >= threshold:
            next_threshold: int | None = None
            for candidate, _name, _icon in reversed(LEVELS):
                if candidate > threshold:
                    next_threshold = candidate
                    break
            return name, icon, threshold, next_threshold
    return "Новичок", "🌱", 0, 10


def progress_bar(value: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "░" * width
    ratio = max(0.0, min(1.0, value / total))
    filled = int(round(ratio * width))
    return "█" * filled + "░" * (width - filled)


def as_int(value: Any, default: int | None = None) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_datetimes(text: str) -> list[datetime]:
    """
    Ищет в тексте одну или несколько дат формата ДД.ММ.ГГГГ ЧЧ:ММ.

    Примеры:
        «25.10.2026 18:00»
        «25.10.2026 18:00 - 25.10.2026 20:00»
    """
    values: list[datetime] = []
    for day, month, year, hour, minute in DATETIME_RE.findall(text or ""):
        values.append(
            datetime(
                year=int(year),
                month=int(month),
                day=int(day),
                hour=int(hour),
                minute=int(minute),
            )
        )
    return values


def display_name(user: User | None, user_id: int | None = None) -> str:
    """Имя для сообщений. Всегда есть понятный fallback по ID."""
    if user is None:
        return f"Сосед #{user_id}" if user_id else "Сосед"
    first = (user.first_name or "").strip()
    last = (user.last_name or "").strip()
    if first and first != "Сосед":
        return f"{first} {last}".strip()
    return f"Сосед #{user.id}"


def booking_status_label(booking: Booking) -> str:
    if booking.status == "active":
        if now_naive() > booking.end_time:
            return "⚠️ просрочена"
        return "🟢 активна"
    if booking.status == "completed":
        return "✅ завершена"
    if booking.status == "cancelled":
        return "❌ отменена"
    return booking.status


def extract_photo_info(
    attachments: list[dict[str, Any]] | None,
) -> tuple[str | None, str | None]:
    """
    Достаёт из attachments входящего сообщения MAX:
      * token (или photo_id) изображения — кладём в photo_attachment;
      * URL изображения — кладём в photo_url и используем для отправки админу.

    В MAX изображение имеет вид:
        {"type": "image", "payload": {"photo_id": ..., "token": "...", "url": "..."}}
    """
    for attachment in attachments or []:
        if not isinstance(attachment, dict):
            continue
        if attachment.get("type") not in ("image", "photo"):
            continue
        payload = attachment.get("payload") or {}
        if not isinstance(payload, dict):
            continue

        url = payload.get("url")
        token = payload.get("token")
        photo_id = payload.get("photo_id")
        token_value = str(token) if token else (str(photo_id) if photo_id else None)
        if url or token_value:
            return token_value, str(url) if url else None

    return None, None


# ============================================================================
# MAX BOT API: ТОЛЬКО aiohttp + LONG POLLING
# ============================================================================


class MaxAPIError(Exception):
    """Ошибка MAX Bot API с HTTP-статусом и телом ответа."""

    def __init__(self, status: int, payload: Any):
        self.status = status
        self.payload = payload
        if isinstance(payload, dict):
            detail = payload.get("message") or payload.get("error") or payload.get("code")
        else:
            detail = str(payload)[:300]
        super().__init__(f"MAX API {status}: {detail}")


def build_image_attachment(
    photo_url: str | None = None,
    photo_token: str | None = None,
) -> dict[str, Any] | None:
    """Вложение-картинка для MAX: можно передать URL или token существующего файла."""
    if photo_url:
        return {"type": "image", "payload": {"url": photo_url}}
    if photo_token:
        return {"type": "image", "payload": {"token": photo_token}}
    return None


def build_attachments(
    keyboard: dict[str, Any] | None = None,
    photo_url: str | None = None,
    photo_token: str | None = None,
) -> list[dict[str, Any]]:
    """Собирает массив attachments для POST /messages или POST /answers."""
    attachments: list[dict[str, Any]] = []
    image = build_image_attachment(photo_url=photo_url, photo_token=photo_token)
    if image is not None:
        attachments.append(image)
    if keyboard is not None:
        attachments.append(keyboard)
    return attachments


class MaxAPI:
    """
    Минимальный асинхронный клиент MAX Bot API.

    Авторизация: заголовок `Authorization: <access_token>`.
    Документация: https://dev.max.ru/docs-api
    """

    def __init__(self, token: str, base_url: str = MAX_API_BASE):
        self.token = (token or "").strip()
        self.base_url = base_url.rstrip("/")
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "MaxAPI":
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS),
                trust_env=True,
                connector=aiohttp.TCPConnector(ssl=False)
            )
        return self
    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> dict[str, Any]:
        """Единая точка HTTP-запросов к MAX Bot API."""
        if not self.token:
            raise MaxAPIError(401, {"message": "Пустой MAX_TOKEN"})
        if self.session is None or self.session.closed:
            raise RuntimeError("MAX API client is not started")

        url = f"{self.base_url}{path}"
        headers = {
            "Authorization": self.token,
            "Accept": "application/json",
        }
        if json_body is not None:
            headers["Content-Type"] = "application/json"

        request_timeout = timeout or aiohttp.ClientTimeout(
            total=API_TIMEOUT_SECONDS,
            connect=10,
        )

        try:
            async with self.session.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=request_timeout,
            ) as response:
                raw = await response.text()
                data: Any = {}
                if raw:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        data = {"raw": raw[:500]}

                if not 200 <= response.status < 300:
                    raise MaxAPIError(response.status, data)
                if not isinstance(data, dict):
                    return {"data": data}
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            raise MaxAPIError(0, {"message": str(error)}) from error

    async def get_me(self) -> dict[str, Any]:
        return await self._request("GET", "/me")

    async def get_updates(
        self,
        marker: int | None = None,
        *,
        limit: int = LONG_POLL_LIMIT,
        timeout: int = LONG_POLL_TIMEOUT,
        types: str = UPDATE_TYPES,
    ) -> dict[str, Any]:
        """GET /updates — Long Polling с маркером следующей страницы."""
        params: dict[str, Any] = {
            "limit": limit,
            "timeout": timeout,
            "types": types,
        }
        if marker is not None:
            params["marker"] = marker

        return await self._request(
            "GET",
            "/updates",
            params=params,
            timeout=aiohttp.ClientTimeout(
                total=timeout + 20,
                connect=15,
                sock_read=timeout + 15,
            ),
        )

    async def send_message(
        self,
        *,
        user_id: int | None = None,
        chat_id: int | None = None,
        text: str,
        keyboard: dict[str, Any] | None = None,
        photo_url: str | None = None,
        photo_token: str | None = None,
    ) -> dict[str, Any] | None:
        """
        POST /messages.
        В MAX получатель задаётся query-параметром user_id или chat_id.
        """
        if user_id is None and chat_id is None:
            raise ValueError("Нужен user_id или chat_id")
        if user_id is not None and chat_id is not None:
            raise ValueError("Укажите только один получатель: user_id или chat_id")

        params: dict[str, Any] = {}
        if user_id is not None:
            params["user_id"] = int(user_id)
        if chat_id is not None:
            params["chat_id"] = int(chat_id)

        body: dict[str, Any] = {"text": clip_text(text)}
        attachments = build_attachments(
            keyboard=keyboard,
            photo_url=photo_url,
            photo_token=photo_token,
        )
        if attachments:
            body["attachments"] = attachments

        data = await self._request("POST", "/messages", params=params, json_body=body)
        message = data.get("message")
        return message if isinstance(message, dict) else None

    async def answer_callback(
        self,
        callback_id: str,
        *,
        text: str | None = None,
        keyboard: dict[str, Any] | None = None,
        photo_url: str | None = None,
        notification: str | None = None,
    ) -> bool:
        """
        POST /answers — ответ на нажатие callback-кнопки.
        Может обновить текущее сообщение и/или показать одноразовое уведомление.
        """
        body: dict[str, Any] = {}
        if text is not None:
            message: dict[str, Any] = {"text": clip_text(text)}
            attachments = build_attachments(keyboard=keyboard, photo_url=photo_url)
            if attachments:
                message["attachments"] = attachments
            body["message"] = message
        if notification:
            body["notification"] = clip_text(notification, 1000)
        if not body:
            body["notification"] = "Готово"

        data = await self._request(
            "POST",
            "/answers",
            params={"callback_id": str(callback_id)},
            json_body=body,
        )
        return bool(data.get("success", True))


# ============================================================================
# КЛАВИАТУРЫ MAX (inline_keyboard + callback-кнопки)
# ============================================================================


def encode_payload(payload: dict[str, Any]) -> str:
    """MAX требует payload кнопки строкой, а не вложенным объектом."""
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(encoded) > MAX_CALLBACK_PAYLOAD_LIMIT:
        raise ValueError(
            f"Callback payload слишком длинный: {len(encoded)} > "
            f"{MAX_CALLBACK_PAYLOAD_LIMIT}"
        )
    return encoded


def callback_button(label: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Одна inline callback-кнопка MAX."""
    return {
        "type": "callback",
        "text": shorten(label, 128),
        "payload": encode_payload(payload),
    }


class KeyboardBuilder:
    """
    Мини-билдер inline-клавиатуры MAX.

    Итоговый объект:
        {
          "type": "inline_keyboard",
          "payload": {"buttons": [[button, ...], ...]}
        }
    """

    def __init__(self) -> None:
        self._rows: list[list[dict[str, Any]]] = []

    def add(self, label: str, payload: dict[str, Any]) -> "KeyboardBuilder":
        if not self._rows:
            self._rows.append([])
        if len(self._rows[-1]) >= 7:
            self._rows.append([])
        self._rows[-1].append(callback_button(label, payload))
        return self

    def row(self) -> "KeyboardBuilder":
        self._rows.append([])
        return self

    def build(self) -> dict[str, Any] | None:
        rows = [row for row in self._rows if row]
        if not rows:
            return None
        total_buttons = sum(len(row) for row in rows)
        if total_buttons > 210:
            raise ValueError("В keyboard MAX нельзя больше 210 кнопок")
        return {
            "type": "inline_keyboard",
            "payload": {"buttons": rows},
        }


def kb_main(is_admin: bool) -> dict[str, Any] | None:
    """Главное меню. Только callback-кнопки."""
    keyboard = KeyboardBuilder()
    keyboard.add("📦 Забронировать", {"cmd": "book"})
    keyboard.add("🗓 Мои брони", {"cmd": "my_bookings", "page": 0})
    keyboard.row()
    keyboard.add("⭐ Моя репутация", {"cmd": "profile"})
    keyboard.add("🗳 Голосования", {"cmd": "polls", "page": 0})
    keyboard.row()
    keyboard.add("🛠 Сообщить о поломке", {"cmd": "damage"})
    keyboard.add("ℹ️ Помощь", {"cmd": "help"})
    if is_admin:
        keyboard.row()
        keyboard.add("🛡 Админ-панель", {"cmd": "admin"})
    return keyboard.build()


def kb_resources_page(
    page: int,
    total_pages: int,
    resources: list[Resource],
) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    for index, resource in enumerate(resources):
        if index and index % 2 == 0:
            keyboard.row()
        keyboard.add(
            f"{resource.emoji} {shorten(resource.name, 28)}",
            {"cmd": "resource", "id": resource.id},
        )

    if resources:
        keyboard.row()
    if total_pages > 1:
        if page > 0:
            keyboard.add("◀️ Назад", {"cmd": "resources", "page": page - 1})
        if page + 1 < total_pages:
            keyboard.add("Вперёд ▶️", {"cmd": "resources", "page": page + 1})
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_resource_card(resource_id: int) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("✅ Забронировать", {"cmd": "book_resource", "id": resource_id})
    keyboard.add("🕒 Занятость", {"cmd": "resource_slots", "id": resource_id})
    keyboard.row()
    keyboard.add("🔙 К списку", {"cmd": "resources", "page": 0})
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_resource_slots(resource_id: int) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("✅ Забронировать", {"cmd": "book_resource", "id": resource_id})
    keyboard.add("🔙 К карточке", {"cmd": "resource", "id": resource_id})
    keyboard.row()
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_duration(resource_id: int) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add(
        "1 час",
        {"cmd": "duration", "rid": resource_id, "hours": 1},
    )
    keyboard.add(
        "2 часа",
        {"cmd": "duration", "rid": resource_id, "hours": 2},
    )
    keyboard.add(
        "3 часа",
        {"cmd": "duration", "rid": resource_id, "hours": 3},
    )
    keyboard.row()
    keyboard.add(
        "4 часа",
        {"cmd": "duration", "rid": resource_id, "hours": 4},
    )
    keyboard.add(
        "✍️ Своё окончание",
        {"cmd": "custom_end", "rid": resource_id},
    )
    keyboard.row()
    keyboard.add("❌ Отмена", {"cmd": "menu"})
    return keyboard.build()


def kb_booking_retry(resource_id: int) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add(
        "✍️ Другое время",
        {"cmd": "book_resource", "id": resource_id},
    )
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_booking_conflict(attempt: "BookingAttempt", fallback_resource_id: int) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    if (
        attempt.suggested_resource_id is not None
        and attempt.suggested_start is not None
        and attempt.suggested_end is not None
    ):
        keyboard.add(
            f"✅ Занять {fmt_dt_short(attempt.suggested_start)}",
            {
                "cmd": "suggest_booking",
                "rid": attempt.suggested_resource_id,
                "start": attempt.suggested_start.isoformat(timespec="minutes"),
                "end": attempt.suggested_end.isoformat(timespec="minutes"),
            },
        )
        keyboard.row()
    keyboard.add(
        "✍️ Другое время",
        {"cmd": "book_resource", "id": fallback_resource_id},
    )
    keyboard.add("📦 Другие ресурсы", {"cmd": "book"})
    keyboard.row()
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_idea_done(poll_id: int) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("💡 Открыть идею", {"cmd": "poll", "id": poll_id})
    keyboard.add(
        "💡 К народным идеям",
        {"cmd": "polls", "page": 0, "ideas": 1},
    )
    keyboard.row()
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_admin_idea_open(poll_id: int) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("💡 Открыть инициативу", {"cmd": "poll", "id": poll_id})
    keyboard.add("🛡 В админку", {"cmd": "admin"})
    return keyboard.build()


def kb_booking_done() -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("🗓 Мои брони", {"cmd": "my_bookings", "page": 0})
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_my_bookings(
    bookings: list[Booking],
    page: int,
    total_pages: int,
) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    for booking in bookings:
        keyboard.add(
            f"✅ Вернуть #{booking.id}",
            {"cmd": "return", "id": booking.id},
        )
        keyboard.add(
            f"❌ Отмена #{booking.id}",
            {"cmd": "cancel_booking", "id": booking.id},
        )
        keyboard.row()

    if total_pages > 1:
        if page > 0:
            keyboard.add("◀️ Назад", {"cmd": "my_bookings", "page": page - 1})
        if page + 1 < total_pages:
            keyboard.add("Вперёд ▶️", {"cmd": "my_bookings", "page": page + 1})
        keyboard.row()

    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_profile() -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("🗓 Мои брони", {"cmd": "my_bookings", "page": 0})
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_polls_list(
    polls: list[Poll],
    page: int = 0,
    total_pages: int = 1,
    ideas_only: bool = False,
) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    for poll in polls:
        prefix = "💡" if poll.is_user_idea else "🗳"
        keyboard.add(
            f"{prefix} {shorten(poll.question, 40)}",
            {"cmd": "poll", "id": poll.id},
        )
        keyboard.row()

    if total_pages > 1:
        if page > 0:
            keyboard.add(
                "◀️ Назад",
                {"cmd": "polls", "page": page - 1, "ideas": int(ideas_only)},
            )
        if page + 1 < total_pages:
            keyboard.add(
                "Вперёд ▶️",
                {"cmd": "polls", "page": page + 1, "ideas": int(ideas_only)},
            )
        keyboard.row()

    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_poll_card(
    poll_id: int,
    is_active: bool,
    is_user_idea: bool = False,
) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    if is_active:
        keyboard.add(
            "✅ За",
            {"cmd": "vote", "poll_id": poll_id, "value": 1},
        )
        keyboard.add(
            "❌ Против",
            {"cmd": "vote", "poll_id": poll_id, "value": -1},
        )
        keyboard.row()
    back_label = "📋 К народным идеям" if is_user_idea else "📋 К голосованиям"
    keyboard.add(
        back_label,
        {"cmd": "polls", "page": 0, "ideas": int(is_user_idea)},
    )
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_damage_cancel() -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("❌ Отмена", {"cmd": "menu"})
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_admin_panel() -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("📊 Статистика", {"cmd": "admin_stats"})
    keyboard.add("🛠 Репорты", {"cmd": "admin_reports"})
    keyboard.row()
    keyboard.add("📦 Инвентарь", {"cmd": "admin_inventory"})
    keyboard.add("🗳 Опросы", {"cmd": "admin_polls"})
    keyboard.row()
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_admin_back() -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("🛡 В админку", {"cmd": "admin"})
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_admin_reports(reports: list[DamageReport]) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    for index, report in enumerate(reports):
        if index and index % 2 == 0:
            keyboard.row()
        keyboard.add(
            f"✅ Репорт #{report.id}",
            {"cmd": "admin_resolve", "id": report.id},
        )
    if reports:
        keyboard.row()
    keyboard.add("🛡 В админку", {"cmd": "admin"})
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_admin_polls(polls: list[Poll]) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    active = [poll for poll in polls if poll.is_active]
    for index, poll in enumerate(active):
        if index and index % 2 == 0:
            keyboard.row()
        keyboard.add(
            f"🔒 Закрыть #{poll.id}",
            {"cmd": "admin_close_poll", "id": poll.id},
        )
    if active:
        keyboard.row()
    keyboard.add("🛡 В админку", {"cmd": "admin"})
    keyboard.add("🏠 Меню", {"cmd": "menu"})
    return keyboard.build()


def kb_admin_report_notify(report_id: int) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add(
        "✅ Отметить решённым",
        {"cmd": "admin_resolve", "id": report_id},
    )
    keyboard.add("🛡 В админку", {"cmd": "admin"})
    return keyboard.build()


def kb_return_notify(booking_id: int) -> dict[str, Any] | None:
    keyboard = KeyboardBuilder()
    keyboard.add("✅ Вернуть", {"cmd": "return", "id": booking_id})
    keyboard.add("🗓 Мои брони", {"cmd": "my_bookings", "page": 0})
    return keyboard.build()


# ============================================================================
# ПОЛЬЗОВАТЕЛИ, БАЛЛЫ И БРОНИРОВАНИЕ
# ============================================================================


async def ensure_user(
    user_id: int,
    first_name: str = "",
    last_name: str = "",
    username: str | None = None,
) -> User:
    """Создаёт/обновляет пользователя MAX в БД."""
    async with SessionLocal() as session:
        user = await session.get(User, user_id)
        changed = False

        if user is None:
            user = User(
                id=user_id,
                first_name=(first_name or "Сосед").strip() or "Сосед",
                last_name=(last_name or "").strip(),
                username=username,
                points=0,
            )
            session.add(user)
            changed = True
        else:
            if first_name and first_name != user.first_name:
                user.first_name = first_name
                changed = True
            if last_name and last_name != user.last_name:
                user.last_name = last_name
                changed = True
            if username is not None and username != user.username:
                user.username = username
                changed = True

        if changed:
            user.updated_at = now_naive()
            await session.commit()
            await session.refresh(user)

        return user


def add_points(session: AsyncSession, user: User, delta: int, reason: str) -> None:
    """
    Изменяет баллы и пишет запись в журнал репутации.
    Commit делает вызывающая сторона — изменение брони и баллов
    попадает в одну транзакцию.
    """
    if delta == 0:
        return
    user.points = int(user.points or 0) + int(delta)
    user.updated_at = now_naive()
    session.add(
        ReputationLog(
            user_id=user.id,
            delta=int(delta),
            reason=reason[:255],
        )
    )


async def maybe_block_user(
    session: AsyncSession,
    user: User,
    now: datetime,
) -> bool:
    """2 поздних нарушения за 30 дней — блокировка бронирования на 24 часа."""
    if user.blocked_until is not None and user.blocked_until > now:
        return False

    await session.flush()
    violations = (
        await session.execute(
            select(func.count())
            .select_from(ReputationLog)
            .where(
                ReputationLog.user_id == user.id,
                ReputationLog.created_at
                >= now - timedelta(days=VIOLATION_WINDOW_DAYS),
                ReputationLog.reason.like("Поздн%"),
            )
        )
    ).scalar_one()

    if int(violations) >= VIOLATION_LIMIT:
        user.blocked_until = now + timedelta(hours=BLOCK_HOURS)
        user.updated_at = now
        return True
    return False


@dataclass(slots=True)
class BookingAttempt:
    ok: bool
    text: str
    booking_id: int | None = None
    start: datetime | None = None
    end: datetime | None = None
    resource_name: str = ""
    suggested_resource_id: int | None = None
    suggested_resource_name: str = ""
    suggested_start: datetime | None = None
    suggested_end: datetime | None = None
    suggested_other_resource: bool = False
    blocked: bool = False


async def find_nearest_free_slot(
    session: AsyncSession,
    resource_id: int,
    desired_start: datetime,
    duration: timedelta,
    user_id: int,
    now: datetime | None = None,
) -> tuple[datetime, datetime] | None:
    """Ищет ближайшее свободное окно в диапазоне −3/+14 дней."""
    if duration <= timedelta(0):
        return None

    now = now or now_naive()
    search_floor = max(now, desired_start - timedelta(days=SUGGESTION_DAYS_BEFORE))
    search_ceiling = desired_start + timedelta(days=SUGGESTION_DAYS_AFTER)

    rows = (
        await session.execute(
            select(Booking.start_time, Booking.end_time).where(
                Booking.resource_id == resource_id,
                Booking.status == "active",
                Booking.end_time > search_floor,
                Booking.start_time < search_ceiling + duration,
            )
        )
    ).all()
    own_rows = (
        await session.execute(
            select(Booking.start_time, Booking.end_time).where(
                Booking.user_id == user_id,
                Booking.status == "active",
                Booking.end_time > search_floor,
                Booking.start_time < search_ceiling + duration,
            )
        )
    ).all()

    intervals = [(start, end) for start, end in rows]
    intervals.extend((start, end) for start, end in own_rows)

    candidate = ceil_to_step(search_floor)
    if candidate < now:
        candidate = ceil_to_step(now)
    if candidate < now + timedelta(minutes=1):
        candidate = ceil_to_step(now + timedelta(minutes=1))

    best: tuple[datetime, datetime] | None = None
    best_key: tuple[float, int, datetime] | None = None

    while candidate + duration <= search_ceiling:
        candidate_end = candidate + duration
        conflict = any(
            booking_start < candidate_end and booking_end > candidate
            for booking_start, booking_end in intervals
        )
        if not conflict:
            distance = abs((candidate - desired_start).total_seconds())
            key = (distance, 0 if candidate >= desired_start else 1, candidate)
            if best_key is None or key < best_key:
                best_key = key
                best = (candidate, candidate_end)
        candidate += timedelta(minutes=SUGGESTION_STEP_MINUTES)

    return best


async def find_alternative_slot(
    session: AsyncSession,
    exclude_resource_id: int,
    desired_start: datetime,
    duration: timedelta,
    user_id: int,
    now: datetime,
) -> tuple[Resource, datetime, datetime] | None:
    resources = (
        (
            await session.execute(
                select(Resource)
                .where(
                    Resource.is_active.is_(True),
                    Resource.id != exclude_resource_id,
                )
                .order_by(Resource.id)
            )
        )
        .scalars()
        .all()
    )
    best: tuple[Resource, datetime, datetime] | None = None
    best_distance: float | None = None
    for resource in resources:
        slot = await find_nearest_free_slot(
            session,
            resource.id,
            desired_start,
            duration,
            user_id,
            now=now,
        )
        if slot is None:
            continue
        candidate_start, candidate_end = slot
        distance = abs((candidate_start - desired_start).total_seconds())
        if best_distance is None or distance < best_distance:
            best_distance = distance
            best = (resource, candidate_start, candidate_end)
    return best


async def try_book(
    user_id: int,
    resource_id: int,
    start: datetime,
    end: datetime,
) -> BookingAttempt:
    """
    Проверяет интервал и создаёт бронь.

    Главная SQL-проверка пересечения:
        bookings.start_time < new_end
        AND bookings.end_time > new_start

    Почему так: интервалы [start, end) пересекаются, если существующая
    бронь началась раньше нашего окончания И закончилась позже нашего начала.
    Соседние брони (end == start) конфликтом не считаются.
    """
    now = now_naive()

    if start < now - timedelta(minutes=1):
        return BookingAttempt(False, "❌ Нельзя бронировать время в прошлом.")
    if start > now + timedelta(days=366):
        return BookingAttempt(
            False,
            "❌ Слишком далёкая дата. Выберите время в пределах года.",
        )
    if end <= start:
        return BookingAttempt(False, "❌ Окончание должно быть позже начала.")
    duration = end - start
    if duration < timedelta(minutes=MIN_BOOKING_MINUTES):
        return BookingAttempt(
            False,
            f"❌ Минимальная длительность брони — {MIN_BOOKING_MINUTES} минут.",
        )
    if duration > timedelta(hours=MAX_BOOKING_HOURS):
        return BookingAttempt(
            False,
            f"❌ Максимальная длительность брони — {MAX_BOOKING_HOURS} часов.",
        )

    async with BOOKING_LOCK:
        async with SessionLocal() as session:
            user = await session.get(User, user_id)
            if user is None:
                return BookingAttempt(False, "❌ Пользователь не найден.")

            if user.blocked_until is not None and user.blocked_until > now:
                return BookingAttempt(
                    False,
                    "🚫 Бронирование временно недоступно.\n\n"
                    f"Вы заблокированы до {fmt_dt(user.blocked_until)} "
                    "за систематические нарушения.\n"
                    "Если это ошибка — напишите администратору.",
                    blocked=True,
                )

            resource = await session.get(Resource, resource_id)
            if resource is None or not resource.is_active:
                return BookingAttempt(False, "❌ Такого ресурса больше нет в инвентаре.")

            # Один человек не должен занимать два ресурса в одно и то же время.
            own_booking_row = (
                await session.execute(
                    select(Booking, Resource)
                    .join(Resource, Resource.id == Booking.resource_id)
                    .where(
                        Booking.user_id == user_id,
                        Booking.status == "active",
                        Booking.start_time < end,
                        Booking.end_time > start,
                    )
                    .order_by(Booking.start_time)
                    .limit(1)
                )
            ).first()
            if own_booking_row is not None:
                own_booking, own_resource = own_booking_row
                return BookingAttempt(
                    False,
                    "❌ У вас уже есть бронь на это время:\n"
                    f"{own_resource.emoji} «{own_resource.name}»\n"
                    f"🕒 {fmt_interval(own_booking.start_time, own_booking.end_time)}\n\n"
                    "Сначала завершите или отмените её.",
                )

            # Проверка пересечения интервалов в БД:
            # start_time < new_end AND end_time > new_start.
            conflict_row = (
                await session.execute(
                    select(Booking, User)
                    .join(User, User.id == Booking.user_id)
                    .where(
                        Booking.resource_id == resource_id,
                        Booking.status == "active",
                        Booking.start_time < end,
                        Booking.end_time > start,
                    )
                    .order_by(Booking.start_time)
                    .limit(1)
                )
            ).first()

            if conflict_row is not None:
                conflict_booking, conflict_user = conflict_row

                suggested = await find_nearest_free_slot(
                    session,
                    resource_id,
                    start,
                    duration,
                    user_id,
                    now=now,
                )
                alternative: tuple[Resource, datetime, datetime] | None = None
                if suggested is None:
                    alternative = await find_alternative_slot(
                        session,
                        resource_id,
                        start,
                        duration,
                        user_id,
                        now,
                    )

                intro = (
                    "😔 Извините, на это время уже забронировано.\n\n"
                    f"📦 «{resource.name}»\n"
                    f"👤 Сосед: {display_name(conflict_user)}\n"
                    f"🕒 {fmt_interval(conflict_booking.start_time, conflict_booking.end_time)}\n\n"
                )
                attempt = BookingAttempt(
                    False,
                    intro,
                    suggested_resource_id=resource.id,
                    suggested_resource_name=resource.name,
                )

                if suggested is not None:
                    suggested_start, suggested_end = suggested
                    attempt.suggested_start = suggested_start
                    attempt.suggested_end = suggested_end
                    attempt.text = (
                        intro
                        + "Но не расстраивайтесь! Я могу предложить вам "
                        "ближайшее свободное окно:\n"
                        f"🕒 {fmt_interval(suggested_start, suggested_end)}\n\n"
                        "Нажмите «Занять это окно» — я сразу оформлю бронь."
                    )
                elif alternative is not None:
                    alt_resource, alt_start, alt_end = alternative
                    attempt.suggested_resource_id = alt_resource.id
                    attempt.suggested_resource_name = alt_resource.name
                    attempt.suggested_start = alt_start
                    attempt.suggested_end = alt_end
                    attempt.suggested_other_resource = True
                    attempt.text = (
                        intro
                        + "Но не расстраивайтесь! На этот ресурс свободных "
                        "окон в ближайшие дни нет.\n\n"
                        f"Зато есть {alt_resource.emoji} «{alt_resource.name}»:\n"
                        f"🕒 {fmt_interval(alt_start, alt_end)}\n\n"
                        "Можно занять его — нажмите кнопку ниже."
                    )
                else:
                    attempt.text = (
                        intro
                        + "Но не расстраивайтесь! Я не нашёл свободного окна "
                        "в ближайшие 14 дней. Попробуйте выбрать другое время "
                        "или загляните позже — соседи часто меняют планы."
                    )
                return attempt

            booking = Booking(
                resource_id=resource_id,
                user_id=user_id,
                start_time=start,
                end_time=end,
                status="active",
                notified_15m=False,
            )
            session.add(booking)
            await session.commit()
            await session.refresh(booking)

            return BookingAttempt(
                True,
                "✅ Бронь создана!\n\n"
                f"📦 {resource.emoji} «{resource.name}»\n"
                f"🕒 {fmt_interval(start, end)}\n"
                f"🆔 Бронь №{booking.id}\n\n"
                "Верните вещь вовремя — за это начислим +1 балл. "
                "За 15 минут до конца я напомню в личные сообщения.",
                booking_id=booking.id,
                start=start,
                end=end,
                resource_name=resource.name,
            )


async def cancel_booking(
    booking_id: int,
    actor_id: int,
    is_admin: bool = False,
) -> tuple[bool, str]:
    """Публичная обёртка: не даём двум нажатиям обработать одну бронь."""
    async with BOOKING_ACTION_LOCK:
        return await _cancel_booking(booking_id, actor_id, is_admin)


async def _cancel_booking(
    booking_id: int,
    actor_id: int,
    is_admin: bool = False,
) -> tuple[bool, str]:
    """Отмена активной брони с начислением штрафа по правилам."""
    async with SessionLocal() as session:
        row = (
            await session.execute(
                select(Booking, Resource)
                .join(Resource, Resource.id == Booking.resource_id)
                .where(Booking.id == booking_id)
                .limit(1)
            )
        ).first()
        if row is None:
            return False, "❌ Бронь не найдена."

        booking, resource = row
        if booking.user_id != actor_id and not is_admin:
            return False, "❌ Это не ваша бронь."
        if booking.status != "active":
            return False, "❌ Эта бронь уже не активна."

        now = now_naive()
        if now >= booking.start_time:
            return (
                False,
                "❌ Бронь уже началась. Отменить её нельзя — "
                "пользуйтесь вещью и нажмите «Вернуть», когда закончите.",
            )

        user = await session.get(User, booking.user_id)
        if user is None:
            return False, "❌ Пользователь не найден."

        admin_cancel_other = is_admin and booking.user_id != actor_id
        if admin_cancel_other:
            delta = 0
            reason = f"Бронь №{booking.id} отменена администратором"
        elif booking.start_time - now >= timedelta(hours=1):
            delta = CANCEL_PENALTY
            reason = f"Отмена брони №{booking.id} более чем за час"
        else:
            delta = LATE_CANCEL_PENALTY
            reason = f"Поздняя отмена брони №{booking.id}"

        booking.status = "cancelled"
        booking.cancelled_at = now
        blocked_now = False
        if delta:
            add_points(session, user, delta, reason)
            if delta == LATE_CANCEL_PENALTY:
                blocked_now = await maybe_block_user(session, user, now)
        await session.commit()
        await session.refresh(user)

        if admin_cancel_other:
            penalty_text = "Штраф не начислен — отменил администратор."
        elif delta == CANCEL_PENALTY:
            penalty_text = f"Штраф: {delta} баллов (отмена больше чем за час)."
        else:
            penalty_text = f"Штраф: {delta} балла (отмена меньше чем за час)."

        level_name, icon, _floor, _next = level_info(user.points)
        block_text = ""
        if blocked_now and user.blocked_until is not None:
            block_text = (
                "\n\n🚫 Это уже второе позднее нарушение за 30 дней.\n"
                f"Бронирование заблокировано до {fmt_dt(user.blocked_until)}."
            )

        return (
            True,
            "❌ Бронь отменена.\n\n"
            f"📦 {resource.emoji} «{resource.name}»\n"
            f"🕒 {fmt_interval(booking.start_time, booking.end_time)}\n"
            f"{penalty_text}\n\n"
            f"⭐ Баланс: {user.points} {points_word(user.points)} "
            f"{icon} {level_name}"
            f"{block_text}",
        )


async def return_booking(booking_id: int, actor_id: int) -> tuple[bool, str]:
    """Публичная обёртка: не даём двум нажатиям обработать одну бронь."""
    async with BOOKING_ACTION_LOCK:
        return await _return_booking(booking_id, actor_id)


async def _return_booking(booking_id: int, actor_id: int) -> tuple[bool, str]:
    """Завершает бронь: +1 за возврат вовремя, -2 за опоздание."""
    async with SessionLocal() as session:
        row = (
            await session.execute(
                select(Booking, Resource)
                .join(Resource, Resource.id == Booking.resource_id)
                .where(Booking.id == booking_id)
                .limit(1)
            )
        ).first()
        if row is None:
            return False, "❌ Бронь не найдена."

        booking, resource = row
        if booking.user_id != actor_id:
            return False, "❌ Это не ваша бронь."
        if booking.status != "active":
            return False, "❌ Эта бронь уже не активна."

        now = now_naive()
        if now < booking.start_time - timedelta(minutes=5):
            return (
                False,
                "⏳ Бронь ещё не началась. Если планы изменились, "
                "отмените её в разделе «Мои брони».",
            )

        user = await session.get(User, booking.user_id)
        if user is None:
            return False, "❌ Пользователь не найден."

        on_time = now <= booking.end_time + timedelta(minutes=RETURN_GRACE_MINUTES)
        if on_time:
            delta = ON_TIME_BONUS
            reason = f"Возврат брони №{booking.id} вовремя"
        else:
            delta = LATE_RETURN_PENALTY
            late_minutes = max(1, int((now - booking.end_time).total_seconds() // 60))
            reason = f"Поздний возврат брони №{booking.id} на {late_minutes} мин."

        booking.status = "completed"
        booking.returned_at = now
        add_points(session, user, delta, reason)
        user.total_saved_rub = int(user.total_saved_rub or 0) + SAVED_RUB_PER_BOOKING

        blocked_now = False
        if not on_time:
            blocked_now = await maybe_block_user(session, user, now)

        await session.commit()
        await session.refresh(user)

        level_name, icon, _floor, _next = level_info(user.points)
        if on_time:
            head = (
                "✅ Возврат принят вовремя!\n\n"
                f"📦 {resource.emoji} «{resource.name}»\n"
                f"🕒 {fmt_interval(booking.start_time, booking.end_time)}\n"
                f"🎁 Начислено: +{ON_TIME_BONUS} балл\n\n"
            )
        else:
            late_minutes = max(1, int((now - booking.end_time).total_seconds() // 60))
            head = (
                "⚠️ Возврат с опозданием.\n\n"
                f"📦 {resource.emoji} «{resource.name}»\n"
                f"🕒 {fmt_interval(booking.start_time, booking.end_time)}\n"
                f"⌛ Опоздание: {late_minutes} мин.\n"
                f"📉 Списано: {LATE_RETURN_PENALTY} балла\n\n"
            )

        block_text = ""
        if blocked_now and user.blocked_until is not None:
            block_text = (
                "\n\n🚫 Это уже второе позднее нарушение за 30 дней.\n"
                f"Бронирование заблокировано до {fmt_dt(user.blocked_until)}."
            )

        return (
            True,
            head
            + f"⭐ Баланс: {user.points} {points_word(user.points)} "
            f"{icon} {level_name}\n"
            f"💰 Сэкономлено за эту бронь: {fmt_money(SAVED_RUB_PER_BOOKING)} ₽\n"
            f"💵 Всего сэкономлено: {fmt_money(user.total_saved_rub)} ₽"
            f"{block_text}",
        )


# ============================================================================
# ГОЛОСОВАНИЯ
# ============================================================================


async def cast_vote(user_id: int, poll_id: int, value: int) -> tuple[bool, str]:
    if value not in (1, -1):
        return False, "❌ Некорректный голос."

    async with SessionLocal() as session:
        poll = await session.get(Poll, poll_id)
        if poll is None:
            return False, "❌ Голосование не найдено."
        if not poll.is_active:
            return False, "🔒 Это голосование уже завершено."

        now = now_naive()
        # SQLite upsert: один пользователь — один голос. Повторное нажатие
        # меняет мнение, а не создаёт дубликат.
        statement = (
            sqlite_insert(PollVote)
            .values(
                poll_id=poll_id,
                user_id=user_id,
                value=value,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_update(
                index_elements=[PollVote.poll_id, PollVote.user_id],
                set_={"value": value, "updated_at": now},
            )
        )
        await session.execute(statement)
        await session.commit()
        return True, "Голос учтён."


# ============================================================================
# РЕПОРТЫ О ПОЛОМКАХ
# ============================================================================


async def create_damage_report(
    user_id: int,
    description: str,
    photo_attachment: str | None,
    photo_url: str | None,
) -> tuple[DamageReport, str]:
    """Сохраняет репорт и возвращает готовый текст для администратора."""
    async with SessionLocal() as session:
        user = await session.get(User, user_id)
        report = DamageReport(
            user_id=user_id,
            description=description,
            photo_attachment=photo_attachment[:255] if photo_attachment else None,
            photo_url=photo_url,
            status="new",
        )
        session.add(report)
        await session.commit()
        await session.refresh(report)

        username = f"@{user.username}" if user and user.username else "без username"
        admin_text = (
            f"🚨 Новый репорт о поломке #{report.id}\n\n"
            f"👤 {display_name(user, user_id)} ({username}, id {user_id})\n"
            f"🕒 {fmt_dt(report.created_at)}\n\n"
            f"📝 {description}\n\n"
            "Отметьте репорт решённым, когда почините или замените вещь."
        )
        if photo_url:
            admin_text += f"\n\n🔗 Фото: {photo_url}"
        return report, admin_text


async def resolve_damage_report(report_id: int) -> tuple[bool, str, int | None]:
    async with SessionLocal() as session:
        report = await session.get(DamageReport, report_id)
        if report is None:
            return False, "❌ Репорт не найден.", None
        if report.status == "resolved":
            return False, "ℹ️ Этот репорт уже закрыт.", report.user_id

        report.status = "resolved"
        report.resolved_at = now_naive()
        await session.commit()
        return True, f"✅ Репорт #{report_id} закрыт.", report.user_id


# ============================================================================
# АДМИНСКИЕ ОПЕРАЦИИ
# ============================================================================


async def admin_add_resource(
    name: str,
    description: str,
    duration_minutes: int = 120,
) -> tuple[bool, str, int | None]:
    name = " ".join(name.split())
    description = description.strip()

    if not (2 <= len(name) <= 80):
        return False, "❌ Название должно быть от 2 до 80 символов.", None
    if not (2 <= len(description) <= 1000):
        return False, "❌ Описание должно быть от 2 до 1000 символов.", None
    if not (15 <= duration_minutes <= MAX_BOOKING_HOURS * 60):
        return (
            False,
            f"❌ Длительность должна быть от 15 минут до {MAX_BOOKING_HOURS} часов.",
            None,
        )

    async with SessionLocal() as session:
        exists = (
            await session.execute(
                select(Resource)
                .where(func.lower(Resource.name) == name.lower())
                .limit(1)
            )
        ).scalar_one_or_none()
        if exists is not None:
            return False, f"❌ Ресурс «{exists.name}» уже есть (ID {exists.id}).", None

        resource = Resource(
            name=name,
            description=description,
            emoji="📦",
            default_duration_minutes=duration_minutes,
            is_active=True,
        )
        session.add(resource)
        await session.commit()
        await session.refresh(resource)
        return (
            True,
            "✅ Ресурс добавлен!\n\n"
            f"📦 «{resource.name}» (ID {resource.id})\n"
            f"🕒 Стандартная бронь: {duration_minutes} мин.\n\n"
            "Он сразу появился в списке бронирования.",
            resource.id,
        )


async def admin_add_poll(
    question: str,
    description: str,
    budget_rub: int = 0,
) -> tuple[bool, str, int | None]:
    question = " ".join(question.split())
    description = description.strip()

    if not (5 <= len(question) <= 200):
        return False, "❌ Вопрос должен быть от 5 до 200 символов.", None
    if len(description) > 1000:
        return False, "❌ Описание слишком длинное (максимум 1000).", None
    if not (0 <= budget_rub <= 10_000_000):
        return False, "❌ Некорректный бюджет.", None

    async with SessionLocal() as session:
        poll = Poll(
            question=question,
            description=description,
            budget_rub=budget_rub,
            is_active=True,
            created_by=ADMIN_ID,
        )
        session.add(poll)
        await session.commit()
        await session.refresh(poll)
        return True, f"✅ Голосование #{poll.id} создано и уже активно.", poll.id


async def admin_close_poll(poll_id: int) -> tuple[bool, str]:
    async with SessionLocal() as session:
        poll = await session.get(Poll, poll_id)
        if poll is None:
            return False, "❌ Голосование не найдено."
        if not poll.is_active:
            return False, "ℹ️ Голосование уже закрыто."
        poll.is_active = False
        await session.commit()
        return True, f"🔒 Голосование #{poll_id} закрыто."


async def admin_unblock_user(user_id: int) -> tuple[bool, str]:
    async with SessionLocal() as session:
        user = await session.get(User, user_id)
        if user is None:
            return False, "❌ Пользователь не найден."
        if user.blocked_until is None:
            return False, "ℹ️ У пользователя нет активной блокировки."
        user.blocked_until = None
        user.updated_at = now_naive()
        await session.commit()
        return True, f"✅ Блокировка с пользователя {display_name(user)} снята."


async def create_user_idea(
    user_id: int,
    question: str,
    description: str,
) -> tuple[bool, str, int | None, str]:
    """Публикует народную инициативу сразу, с антиспам-лимитом 3 в сутки."""
    question = " ".join(question.split()).strip()
    description = description.strip()

    if not (5 <= len(question) <= 200):
        return False, "❌ Вопрос должен быть от 5 до 200 символов.", None, question
    if not (5 <= len(description) <= 1000):
        return False, "❌ Описание должно быть от 5 до 1000 символов.", None, question

    now = now_naive()
    async with SessionLocal() as session:
        recent = (
            await session.execute(
                select(func.count())
                .select_from(Poll)
                .where(
                    Poll.created_by == user_id,
                    Poll.is_user_idea.is_(True),
                    Poll.created_at >= now - timedelta(days=1),
                )
            )
        ).scalar_one()
        if int(recent) >= MAX_IDEAS_PER_DAY:
            return (
                False,
                f"❌ Вы уже предложили {MAX_IDEAS_PER_DAY} идеи за сутки. "
                "Попробуйте завтра.",
                None,
                question,
            )

        poll = Poll(
            question=question,
            description=description,
            budget_rub=0,
            is_active=True,
            is_user_idea=True,
            idea_notified=False,
            created_by=user_id,
        )
        session.add(poll)
        await session.commit()
        await session.refresh(poll)
        return True, "✅ Идея опубликована!", poll.id, question


async def idea_quota_left(user_id: int) -> int:
    async with SessionLocal() as session:
        recent = (
            await session.execute(
                select(func.count())
                .select_from(Poll)
                .where(
                    Poll.created_by == user_id,
                    Poll.is_user_idea.is_(True),
                    Poll.created_at >= now_naive() - timedelta(days=1),
                )
            )
        ).scalar_one()
    return max(0, MAX_IDEAS_PER_DAY - int(recent))


async def check_idea_threshold(
    poll_id: int,
) -> tuple[str, int, int | None, int] | None:
    async with SessionLocal() as session:
        poll = await session.get(Poll, poll_id)
        if (
            poll is None
            or not poll.is_active
            or not poll.is_user_idea
            or poll.idea_notified
        ):
            return None

        votes_for = (
            await session.execute(
                select(func.count())
                .select_from(PollVote)
                .where(PollVote.poll_id == poll_id, PollVote.value == 1)
            )
        ).scalar_one()
        if int(votes_for) < IDEA_VOTES_THRESHOLD:
            return None

        poll.idea_notified = True
        await session.commit()
        return poll.question, int(votes_for), poll.created_by, poll.id


# ============================================================================
# СБОРЩИКИ ЭКРАНОВ
# ============================================================================


async def build_main_menu(user_id: int) -> tuple[str, dict[str, Any] | None]:
    async with SessionLocal() as session:
        user = await session.get(User, user_id)
        resources_count = (
            await session.execute(
                select(func.count())
                .select_from(Resource)
                .where(Resource.is_active.is_(True))
            )
        ).scalar_one()
        active_bookings = (
            await session.execute(
                select(func.count())
                .select_from(Booking)
                .where(Booking.status == "active")
            )
        ).scalar_one()
        active_polls = (
            await session.execute(
                select(func.count())
                .select_from(Poll)
                .where(Poll.is_active.is_(True))
            )
        ).scalar_one()

    if user is None:
        points = 0
        saved_rub = 0
        name = f"Сосед #{user_id}"
    else:
        points = int(user.points or 0)
        saved_rub = int(user.total_saved_rub or 0)
        name = display_name(user)

    level_name, icon, _floor, _next = level_info(points)

    text = (
        "🏡 «Соседский ресурс»\n"
        f"Привет, {name}!\n\n"
        "Это общий склад посёлка: бронируйте вещи и пространства, "
        "следите за репутацией и решайте, на что тратить общий бюджет.\n\n"
        f"⭐ {points} {points_word(points)} · {icon} {level_name}\n"
        f"💰 Сэкономлено: {fmt_money(saved_rub)} ₽\n"
        f"📦 Ресурсов: {resources_count} · "
        f"🟢 Активных броней: {active_bookings} · "
        f"🗳 Голосований: {active_polls}\n\n"
        "Что делаем?"
    )
    return text, kb_main(user_id == ADMIN_ID)


def build_help(is_admin: bool = False) -> tuple[str, dict[str, Any] | None]:
    text = (
        "ℹ️ Как всё устроено\n\n"
        "📦 Бронирование\n"
        "Выберите ресурс, введите начало в формате:\n"
        "25.10.2026 18:00\n\n"
        "Можно сразу указать интервал:\n"
        "25.10.2026 18:00 - 25.10.2026 20:00\n\n"
        "Бот проверит пересечение с другими бронями и назовёт того, "
        "кто уже занял слот.\n\n"
        "⭐ Репутация\n"
        "• +1 балл — вернули вещь вовремя;\n"
        "• −5 баллов — отмена больше чем за час до начала;\n"
        "• −3 балла — отмена меньше чем за час;\n"
        "• −2 балла — возврат с опозданием.\n\n"
        "Уровни: 🌱 Новичок (0–9), 🤝 Сосед (10–29), 👑 Староста (30+).\n\n"
        "💰 Экономия\n"
        "Каждый завершённый возврат = 500 ₽ условной экономии: "
        "вещь не пришлось покупать самому. Сумма копится в профиле.\n\n"
        "🛠 Поломка\n"
        "Нажмите кнопку, отправьте фото и описание. Репорт сразу уйдёт "
        "администратору и сохранится в БД.\n\n"
        "🗳 Голосования\n"
        "Голосуйте «За» или «Против». Один человек — один голос, "
        "но мнение можно поменять до закрытия опроса.\n\n"
        "🚫 Блокировка\n"
        "Два поздних возврата или отмены за 30 дней — бронирование "
        "недоступно 24 часа.\n\n"
        "⏰ Напоминание\n"
        "За 15 минут до конца брони бот напишет в личные сообщения."
    )
    return text, kb_main(is_admin)


async def build_resources_page(page: int = 0) -> tuple[str, dict[str, Any] | None]:
    page_size = 6
    async with SessionLocal() as session:
        total = (
            await session.execute(
                select(func.count())
                .select_from(Resource)
                .where(Resource.is_active.is_(True))
            )
        ).scalar_one()
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = max(0, min(page, total_pages - 1))
        resources = (
            (
                await session.execute(
                    select(Resource)
                    .where(Resource.is_active.is_(True))
                    .order_by(Resource.id)
                    .offset(page * page_size)
                    .limit(page_size)
                )
            )
            .scalars()
            .all()
        )

    if not resources:
        return (
            "📦 Пока нет доступных ресурсов.\n"
            "Администратор может добавить их командой /add_resource.",
            kb_main(False),
        )

    lines = [f"📦 Общий инвентарь · страница {page + 1}/{total_pages}", ""]
    for index, resource in enumerate(resources, start=1 + page * page_size):
        lines.append(
            f"{index}. {resource.emoji} {resource.name} — "
            f"стандарт {resource.default_duration_minutes} мин."
        )
    lines.extend(
        [
            "",
            "Нажмите на ресурс, чтобы посмотреть описание, занятость и выбрать время.",
        ]
    )
    return "\n".join(lines), kb_resources_page(page, total_pages, resources)


async def build_resource_card(resource_id: int) -> tuple[str, dict[str, Any] | None]:
    async with SessionLocal() as session:
        resource = await session.get(Resource, resource_id)

    if resource is None or not resource.is_active:
        return "❌ Ресурс не найден или снят с бронирования.", kb_main(False)

    text = (
        f"{resource.emoji} «{resource.name}»\n\n"
        f"{resource.description or 'Описание пока не добавлено.'}\n\n"
        f"🕒 Стандартная бронь: {resource.default_duration_minutes} мин.\n"
        f"🆔 ID ресурса: {resource.id}\n\n"
        "Проверьте занятость или сразу выбирайте время."
    )
    return text, kb_resource_card(resource.id)


async def build_resource_slots(resource_id: int) -> tuple[str, dict[str, Any] | None]:
    now = now_naive()
    async with SessionLocal() as session:
        resource = await session.get(Resource, resource_id)
        if resource is None:
            return "❌ Ресурс не найден.", kb_main(False)

        rows = (
            await session.execute(
                select(Booking, User)
                .join(User, User.id == Booking.user_id)
                .where(
                    Booking.resource_id == resource_id,
                    Booking.status == "active",
                    Booking.end_time > now,
                )
                .order_by(Booking.start_time)
                .limit(8)
            )
        ).all()

    if not rows:
        body = "На ближайшее время броней нет — можно занимать."
    else:
        lines = []
        for booking, user in rows:
            if booking.start_time <= now <= booking.end_time:
                prefix = "🔴 сейчас"
            else:
                prefix = "🟡 " + fmt_dt_short(booking.start_time)
            lines.append(
                f"{prefix} — {user.first_name or 'Сосед'} "
                f"({fmt_dt_short(booking.start_time)}–"
                f"{booking.end_time.strftime('%H:%M')})"
            )
        body = "\n".join(lines)

    text = (
        f"🕒 Занятость «{resource.name}»\n\n"
        f"{body}\n\n"
        "Хотите забронировать свободный интервал?"
    )
    return text, kb_resource_slots(resource_id)


async def build_profile(user_id: int) -> tuple[str, dict[str, Any] | None]:
    async with SessionLocal() as session:
        user = await session.get(User, user_id)
        if user is None:
            return "❌ Профиль не найден. Нажмите /start.", kb_main(user_id == ADMIN_ID)

        active_count = (
            await session.execute(
                select(func.count())
                .select_from(Booking)
                .where(Booking.user_id == user_id, Booking.status == "active")
            )
        ).scalar_one()
        completed = (
            (
                await session.execute(
                    select(Booking)
                    .where(Booking.user_id == user_id, Booking.status == "completed")
                    .order_by(desc(Booking.end_time))
                )
            )
            .scalars()
            .all()
        )
        cancelled_count = (
            await session.execute(
                select(func.count())
                .select_from(Booking)
                .where(Booking.user_id == user_id, Booking.status == "cancelled")
            )
        ).scalar_one()
        logs = (
            (
                await session.execute(
                    select(ReputationLog)
                    .where(ReputationLog.user_id == user_id)
                    .order_by(desc(ReputationLog.created_at))
                    .limit(5)
                )
            )
            .scalars()
            .all()
        )

    on_time = sum(
        1
        for booking in completed
        if booking.returned_at is not None
        and booking.returned_at
        <= booking.end_time + timedelta(minutes=RETURN_GRACE_MINUTES)
    )
    late = max(0, len(completed) - on_time)

    points = int(user.points or 0)
    saved_rub = int(user.total_saved_rub or 0)
    now = now_naive()
    block_line = ""
    if user.blocked_until is not None and user.blocked_until > now:
        block_line = (
            f"\n🚫 Бронирование заблокировано до {fmt_dt(user.blocked_until)}\n"
            "Причина: два поздних нарушения за 30 дней.\n"
        )

    level_name, icon, floor, next_threshold = level_info(points)
    if next_threshold is not None:
        current = max(points, floor)
        progress = progress_bar(current - floor, next_threshold - floor)
        until_next = max(0, next_threshold - points)
        next_line = (
            f"До уровня выше: {until_next} {points_word(until_next)}\n" f"{progress}"
        )
    else:
        next_line = "Максимальный уровень — вы легенда посёлка!"

    logs_text = "Пока пусто."
    if logs:
        logs_text = "\n".join(
            f"• {item.created_at.strftime('%d.%m %H:%M')} "
            f"{item.delta:+d} — {item.reason}"
            for item in logs
        )

    text = (
        "⭐ Моя репутация\n\n"
        f"👤 {display_name(user)}\n"
        f"Баллы: {points} {points_word(points)}\n"
        f"Уровень: {icon} {level_name}\n"
        f"💰 Сэкономлено денег: {fmt_money(saved_rub)} ₽\n"
        f"{block_line}"
        f"{next_line}\n\n"
        "📊 Статистика\n"
        f"• Активных броней: {active_count}\n"
        f"• Завершено: {len(completed)}\n"
        f"• Возвратов вовремя: {on_time}\n"
        f"• С опозданием: {late}\n"
        f"• Отменено: {cancelled_count}\n\n"
        f"🕘 Последние изменения:\n{logs_text}"
    )
    return text, kb_profile()


async def build_my_bookings(
    user_id: int,
    page: int = 0,
) -> tuple[str, dict[str, Any] | None]:
    page_size = 3
    async with SessionLocal() as session:
        active = (
            (
                await session.execute(
                    select(Booking, Resource)
                    .join(Resource, Resource.id == Booking.resource_id)
                    .where(Booking.user_id == user_id, Booking.status == "active")
                    .order_by(Booking.start_time)
                )
            )
            .all()
        )
        completed_count = (
            await session.execute(
                select(func.count())
                .select_from(Booking)
                .where(Booking.user_id == user_id, Booking.status == "completed")
            )
        ).scalar_one()

    total_pages = max(1, (len(active) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    page_items = active[page * page_size : page * page_size + page_size]

    if not active:
        return (
            "🗓 Мои брони\n\n"
            "Сейчас активных броней нет.\n"
            f"Завершённых броней: {completed_count}.\n\n"
            "Самое время что-нибудь забронировать.",
            kb_main(user_id == ADMIN_ID),
        )

    lines = [f"🗓 Мои активные брони · {page + 1}/{total_pages}", ""]
    for booking, resource in page_items:
        lines.extend(
            [
                f"📦 «{resource.name}»",
                f"🕒 {fmt_interval(booking.start_time, booking.end_time)}",
                f"Статус: {booking_status_label(booking)} · №{booking.id}",
                "",
            ]
        )
    lines.append(f"Всего активных: {len(active)} · завершено: {completed_count}")

    return "\n".join(lines), kb_my_bookings(
        [booking for booking, _resource in page_items],
        page,
        total_pages,
    )


async def build_polls(
    user_id: int,
    page: int = 0,
    ideas_only: bool = False,
) -> tuple[str, dict[str, Any] | None]:
    page_size = 4
    async with SessionLocal() as session:
        condition = [
            Poll.is_active.is_(True),
            Poll.is_user_idea.is_(ideas_only),
        ]
        total = (
            await session.execute(
                select(func.count()).select_from(Poll).where(*condition)
            )
        ).scalar_one()
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = max(0, min(page, total_pages - 1))

        polls = (
            (
                await session.execute(
                    select(Poll)
                    .where(*condition)
                    .order_by(desc(Poll.created_at))
                    .offset(page * page_size)
                    .limit(page_size)
                )
            )
            .scalars()
            .all()
        )

        if not polls:
            if ideas_only:
                return (
                    "💡 Народных инициатив пока нет.\n\n"
                    "Нажмите «💡 Предложить идею» и расскажите, что нужно изменить.",
                    kb_polls_list([], page, total_pages, ideas_only=True),
                )
            return (
                "🗳 Активных голосований пока нет.\n"
                "Администратор может создать опрос командой /add_poll.",
                kb_polls_list([], page, total_pages, ideas_only=False),
            )

        title = "💡 Народные инициативы" if ideas_only else "🗳 Активные голосования"
        lines = [f"{title} · {page + 1}/{total_pages}", ""]
        for index, poll in enumerate(polls, start=1 + page * page_size):
            votes_count = (
                await session.execute(
                    select(func.count())
                    .select_from(PollVote)
                    .where(PollVote.poll_id == poll.id)
                )
            ).scalar_one()
            budget = (
                f" · бюджет ~{poll.budget_rub:,} ₽".replace(",", " ")
                if poll.budget_rub
                else ""
            )
            prefix = "💡" if poll.is_user_idea else "🗳"
            lines.append(
                f"{index}. {prefix} {poll.question}\n"
                f"   Голосов: {votes_count}{budget}"
            )

    if ideas_only:
        lines.extend(
            [
                "",
                "Идеи соседей. При 10 голосах «За» администратор получит сигнал.",
            ]
        )
    else:
        lines.extend(["", "Нажмите на опрос, чтобы проголосовать «За» или «Против»."])
    return "\n".join(lines), kb_polls_list(
        list(polls), page, total_pages, ideas_only
    )


def poll_result_lines(
    count_for: int,
    count_against: int,
    user_value: int | None,
) -> str:
    total = count_for + count_against
    if total == 0:
        return "Пока нет голосов. Будьте первым!"

    percent_for = round(count_for * 100 / total)
    percent_against = 100 - percent_for
    own = "не голосовали"
    if user_value == 1:
        own = "За"
    elif user_value == -1:
        own = "Против"

    return (
        f"✅ За: {count_for} ({percent_for}%)\n"
        f"{progress_bar(count_for, total)}\n"
        f"❌ Против: {count_against} ({percent_against}%)\n"
        f"{progress_bar(count_against, total)}\n\n"
        f"Всего голосов: {total}\n"
        f"Ваш голос: {own}"
    )


async def build_poll_view(
    user_id: int,
    poll_id: int,
) -> tuple[str, dict[str, Any] | None]:
    async with SessionLocal() as session:
        poll = await session.get(Poll, poll_id)
        if poll is None:
            return "❌ Голосование не найдено.", kb_main(user_id == ADMIN_ID)

        rows = (
            await session.execute(
                select(PollVote.value, func.count(PollVote.id))
                .where(PollVote.poll_id == poll_id)
                .group_by(PollVote.value)
            )
        ).all()
        counts = {1: 0, -1: 0}
        for value, amount in rows:
            counts[int(value)] = int(amount)

        own_vote = (
            await session.execute(
                select(PollVote)
                .where(PollVote.poll_id == poll_id, PollVote.user_id == user_id)
                .limit(1)
            )
        ).scalar_one_or_none()
        own_value = own_vote.value if own_vote else None

        author_line = ""
        if poll.is_user_idea and poll.created_by is not None:
            author = await session.get(User, poll.created_by)
            author_line = f"👤 Автор идеи: {display_name(author, poll.created_by)}\n"

    budget = (
        f"💰 Ориентир по бюджету: {poll.budget_rub:,} ₽".replace(",", " ")
        if poll.budget_rub
        else ""
    )
    status = "🟢 Идёт голосование" if poll.is_active else "🔒 Голосование завершено"
    header = "💡 Народная инициатива" if poll.is_user_idea else "🗳 Голосование"

    text = (
        f"{header}: {poll.question}\n\n"
        f"{poll.description or 'Описание не добавлено.'}\n\n"
        f"{author_line}"
        f"{budget}\n"
        f"{status}\n\n"
        f"{poll_result_lines(counts[1], counts[-1], own_value)}"
    )
    return text, kb_poll_card(poll.id, poll.is_active, poll.is_user_idea)


async def build_admin_panel() -> tuple[str, dict[str, Any] | None]:
    async with SessionLocal() as session:
        resources = (
            await session.execute(
                select(func.count())
                .select_from(Resource)
                .where(Resource.is_active.is_(True))
            )
        ).scalar_one()
        active_bookings = (
            await session.execute(
                select(func.count())
                .select_from(Booking)
                .where(Booking.status == "active")
            )
        ).scalar_one()
        new_reports = (
            await session.execute(
                select(func.count())
                .select_from(DamageReport)
                .where(DamageReport.status != "resolved")
            )
        ).scalar_one()
        active_polls = (
            await session.execute(
                select(func.count())
                .select_from(Poll)
                .where(Poll.is_active.is_(True))
            )
        ).scalar_one()
        total_saved = (
            await session.execute(
                select(func.coalesce(func.sum(User.total_saved_rub), 0))
            )
        ).scalar_one()
        blocked_count = (
            await session.execute(
                select(func.count())
                .select_from(User)
                .where(User.blocked_until.is_not(None), User.blocked_until > now_naive())
            )
        ).scalar_one()

    text = (
        "🛡 Админ-панель\n\n"
        f"📦 Ресурсов: {resources}\n"
        f"🟢 Активных броней: {active_bookings}\n"
        f"🛠 Открытых репортов: {new_reports}\n"
        f"🗳 Активных опросов: {active_polls}\n"
        f"💰 Сэкономлено соседями: {fmt_money(int(total_saved))} ₽\n"
        f"🚫 Заблокировано сейчас: {blocked_count}\n\n"
        "Команды (только для администратора):\n"
        "/add_resource Название | Описание | 120\n"
        "/add_poll Вопрос | Описание | 18000\n"
        "/close_poll ID\n"
        "/unblock USER_ID — снять блокировку\n"
        "/reports — открытые репорты\n"
        "/stats — статистика"
    )
    return text, kb_admin_panel()


async def build_admin_stats() -> tuple[str, dict[str, Any] | None]:
    async with SessionLocal() as session:
        users_count = (
            await session.execute(select(func.count()).select_from(User))
        ).scalar_one()
        bookings_total = (
            await session.execute(select(func.count()).select_from(Booking))
        ).scalar_one()
        active_bookings = (
            await session.execute(
                select(func.count())
                .select_from(Booking)
                .where(Booking.status == "active")
            )
        ).scalar_one()
        reports_open = (
            await session.execute(
                select(func.count())
                .select_from(DamageReport)
                .where(DamageReport.status != "resolved")
            )
        ).scalar_one()
        polls_active = (
            await session.execute(
                select(func.count())
                .select_from(Poll)
                .where(Poll.is_active.is_(True))
            )
        ).scalar_one()
        total_saved = (
            await session.execute(
                select(func.coalesce(func.sum(User.total_saved_rub), 0))
            )
        ).scalar_one()
        blocked_count = (
            await session.execute(
                select(func.count())
                .select_from(User)
                .where(User.blocked_until.is_not(None), User.blocked_until > now_naive())
            )
        ).scalar_one()
        top_users = (
            (
                await session.execute(
                    select(User).order_by(desc(User.points)).limit(3)
                )
            )
            .scalars()
            .all()
        )
        top_savers = (
            (
                await session.execute(
                    select(User)
                    .where(User.total_saved_rub > 0)
                    .order_by(desc(User.total_saved_rub))
                    .limit(3)
                )
            )
            .scalars()
            .all()
        )

    top_lines = (
        "\n".join(
            f"{index}. {display_name(user)} — {user.points} {points_word(user.points)}"
            for index, user in enumerate(top_users, start=1)
        )
        or "Пока нет данных."
    )
    saver_lines = (
        "\n".join(
            f"{index}. {display_name(user)} — {fmt_money(user.total_saved_rub)} ₽"
            for index, user in enumerate(top_savers, start=1)
        )
        or "Пока нет данных."
    )

    text = (
        "📊 Статистика «Соседского ресурса»\n\n"
        f"👥 Пользователей: {users_count}\n"
        f"📅 Броней всего: {bookings_total}\n"
        f"🟢 Активных броней: {active_bookings}\n"
        f"🛠 Открытых репортов: {reports_open}\n"
        f"🗳 Активных опросов: {polls_active}\n\n"
        f"💰 Сэкономлено соседями: {fmt_money(int(total_saved))} ₽\n"
        f"🚫 Заблокировано сейчас: {blocked_count}\n\n"
        f"🏆 Топ соседей:\n{top_lines}\n\n"
        f"💰 Топ по экономии:\n{saver_lines}"
    )
    return text, kb_admin_back()


async def build_admin_reports() -> tuple[str, dict[str, Any] | None]:
    async with SessionLocal() as session:
        rows = (
            await session.execute(
                select(DamageReport, User)
                .join(User, User.id == DamageReport.user_id)
                .where(DamageReport.status != "resolved")
                .order_by(desc(DamageReport.created_at))
                .limit(8)
            )
        ).all()

    if not rows:
        return "🛠 Открытых репортов нет. Всё в порядке!", kb_admin_back()

    lines = [f"🛠 Открытые репорты ({len(rows)})", ""]
    reports: list[DamageReport] = []
    for report, user in rows:
        reports.append(report)
        lines.extend(
            [
                f"#{report.id} · {fmt_dt_short(report.created_at)} · "
                f"{display_name(user)}",
                f"   {shorten(report.description, 90)}",
                "",
            ]
        )
    lines.append("Нажмите на репорт, когда проблема решена.")
    return "\n".join(lines), kb_admin_reports(reports)


async def build_admin_inventory() -> tuple[str, dict[str, Any] | None]:
    async with SessionLocal() as session:
        resources = (
            (
                await session.execute(
                    select(Resource).order_by(Resource.id).limit(30)
                )
            )
            .scalars()
            .all()
        )

    if not resources:
        return "📦 Инвентарь пуст.", kb_admin_back()

    lines = ["📦 Инвентарь", ""]
    for resource in resources:
        status = "🟢" if resource.is_active else "⚪ скрыт"
        lines.append(
            f"#{resource.id} {resource.emoji} {resource.name} — "
            f"{resource.default_duration_minutes} мин. {status}"
        )
    lines.extend(
        [
            "",
            "Добавить ресурс:",
            "/add_resource Название | Описание | 120",
        ]
    )
    return "\n".join(lines), kb_admin_back()


async def build_admin_polls() -> tuple[str, dict[str, Any] | None]:
    async with SessionLocal() as session:
        polls = (
            (
                await session.execute(
                    select(Poll).order_by(desc(Poll.created_at)).limit(8)
                )
            )
            .scalars()
            .all()
        )

    if not polls:
        return "🗳 Опросов пока нет.", kb_admin_back()

    lines = ["🗳 Все голосования", ""]
    for poll in polls:
        status = "🟢 активно" if poll.is_active else "🔒 закрыто"
        kind = "💡 идея" if poll.is_user_idea else "🗳 опрос"
        lines.append(f"#{poll.id} · {kind} · {status} · {poll.question}")

    lines.extend(
        [
            "",
            "Создать: /add_poll Вопрос | Описание | 18000",
            "Закрыть: /close_poll ID",
        ]
    )
    return "\n".join(lines), kb_admin_polls(polls)


# ============================================================================
# КОНТЕКСТЫ СООБЩЕНИЙ И ОТПРАВКА
# ============================================================================


@dataclass(slots=True)
class IncomingMessage:
    user_id: int
    target: dict[str, int]
    text: str
    attachments: list[dict[str, Any]]
    sender: dict[str, Any]

    async def reply(
        self,
        text: str,
        keyboard: dict[str, Any] | None = None,
        photo_url: str | None = None,
        photo_token: str | None = None,
    ) -> bool:
        return await send_to_target(
            self.target,
            text,
            keyboard=keyboard,
            photo_url=photo_url,
            photo_token=photo_token,
        )


@dataclass(slots=True)
class CallbackEvent:
    callback_id: str
    user_id: int
    target: dict[str, int]
    payload: dict[str, Any]
    user: dict[str, Any]
    original_message: dict[str, Any] | None

    async def reply(
        self,
        text: str,
        keyboard: dict[str, Any] | None = None,
        photo_url: str | None = None,
        notification: str | None = None,
    ) -> None:
        await callback_reply(
            self,
            text,
            keyboard=keyboard,
            photo_url=photo_url,
            notification=notification,
        )


def target_from_message(
    message: dict[str, Any] | None,
    fallback_user_id: int | None = None,
) -> dict[str, int] | None:
    """
    Определяет, куда отвечать: в ЛС пользователю или в групповой чат.

    Для message_created sender — это пользователь.
    Для message_callback message — это сообщение бота с кнопкой, поэтому
    sender в нём — сам бот. В диалоге в таком случае обязательно передаём
    fallback_user_id из callback.user.user_id.
    """
    if not isinstance(message, dict):
        if fallback_user_id:
            return {"user_id": fallback_user_id}
        return None

    recipient = message.get("recipient") or {}
    sender = message.get("sender") or {}
    chat_type = recipient.get("chat_type")

    if chat_type == "dialog":
        user_id = fallback_user_id or as_int(sender.get("user_id"))
        if user_id:
            return {"user_id": user_id}

    chat_id = as_int(recipient.get("chat_id"))
    if chat_id:
        return {"chat_id": chat_id}

    user_id = fallback_user_id or as_int(sender.get("user_id"))
    if user_id:
        return {"user_id": user_id}

    return None


async def send_to_user(
    user_id: int,
    text: str,
    keyboard: dict[str, Any] | None = None,
    photo_url: str | None = None,
    photo_token: str | None = None,
) -> bool:
    """Отправка сообщения конкретному пользователю MAX."""
    if API is None:
        logger.error("MAX API не инициализирован")
        return False
    try:
        message = await API.send_message(
            user_id=user_id,
            text=text,
            keyboard=keyboard,
            photo_url=photo_url,
            photo_token=photo_token,
        )
        return message is not None
    except Exception:
        logger.exception("Не удалось отправить сообщение пользователю %s", user_id)
        return False


async def send_to_chat(
    chat_id: int,
    text: str,
    keyboard: dict[str, Any] | None = None,
    photo_url: str | None = None,
    photo_token: str | None = None,
) -> bool:
    """Отправка сообщения в групповой чат/канал MAX."""
    if API is None:
        logger.error("MAX API не инициализирован")
        return False
    try:
        message = await API.send_message(
            chat_id=chat_id,
            text=text,
            keyboard=keyboard,
            photo_url=photo_url,
            photo_token=photo_token,
        )
        return message is not None
    except Exception:
        logger.exception("Не удалось отправить сообщение в чат %s", chat_id)
        return False


async def send_to_target(
    target: dict[str, int],
    text: str,
    keyboard: dict[str, Any] | None = None,
    photo_url: str | None = None,
    photo_token: str | None = None,
) -> bool:
    """Отправка в цель, вычисленную из входящего сообщения."""
    chat_id = target.get("chat_id")
    if chat_id is not None:
        return await send_to_chat(
            int(chat_id),
            text,
            keyboard=keyboard,
            photo_url=photo_url,
            photo_token=photo_token,
        )
    user_id = target.get("user_id")
    if user_id is not None:
        return await send_to_user(
            int(user_id),
            text,
            keyboard=keyboard,
            photo_url=photo_url,
            photo_token=photo_token,
        )
    logger.error("Пустая цель отправки: %s", target)
    return False


async def callback_reply(
    event: CallbackEvent,
    text: str,
    keyboard: dict[str, Any] | None = None,
    photo_url: str | None = None,
    notification: str | None = None,
) -> None:
    """
    Отвечает на callback через POST /answers (редактирует сообщение с кнопкой).
    Если MAX не дал обновить сообщение, отправляет обычное сообщение.
    """
    if API is None:
        logger.error("MAX API не инициализирован")
        return

    try:
        answered = await API.answer_callback(
            event.callback_id,
            text=text,
            keyboard=keyboard,
            photo_url=photo_url,
            notification=notification,
        )
        if answered:
            return
        logger.warning("MAX не подтвердил ответ на callback %s", event.callback_id)
    except Exception:
        logger.exception("Не удалось ответить на callback %s", event.callback_id)

    # Fallback: callback мог устареть или сообщение могли удалить.
    await send_to_target(
        event.target,
        text,
        keyboard=keyboard,
        photo_url=photo_url,
    )


def decode_callback_payload(raw: Any) -> dict[str, Any]:
    """Payload MAX приходит строкой JSON; декодируем безопасно."""
    if isinstance(raw, dict):
        return raw
    if not raw or not isinstance(raw, str):
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


async def notify_admin_new_idea(poll_id: int, question: str, author_id: int) -> None:
    async with SessionLocal() as session:
        author = await session.get(User, author_id)

    await send_to_user(
        ADMIN_ID,
        "💡 Новая народная инициатива\n\n"
        f"👤 Автор: {display_name(author, author_id)}\n"
        f"❓ {question}\n\n"
        "Идея уже опубликована в разделе «Народные инициативы».",
        keyboard=kb_admin_idea_open(poll_id),
    )


async def notify_idea_threshold(
    question: str,
    votes_for: int,
    author_id: int | None,
    poll_id: int,
) -> None:
    await send_to_user(
        ADMIN_ID,
        "🚀 Ого, народная инициатива набрала 10 голосов, пора действовать!\n\n"
        f"💡 {question}\n"
        f"✅ Голосов «За»: {votes_for}",
        keyboard=kb_admin_idea_open(poll_id),
    )
    if author_id is not None:
        await send_to_user(
            author_id,
            "🎉 Ваша народная инициатива набрала "
            f"{votes_for} голосов «За»!\n\n"
            f"💡 {question}\n\n"
            "Администратор уже получил сигнал «пора действовать».",
        )


async def publish_user_idea_message(
    ctx: IncomingMessage,
    user_id: int,
    question: str,
    description: str,
) -> bool:
    ok, result_text, poll_id, clean_question = await create_user_idea(
        user_id,
        question,
        description,
    )
    if not ok or poll_id is None:
        await ctx.reply(result_text, keyboard=kb_damage_cancel())
        return False

    USER_STATES.pop(user_id, None)
    await notify_admin_new_idea(poll_id, clean_question, user_id)
    await ctx.reply(
        "✅ Идея опубликована!\n\n"
        f"💡 {clean_question}\n\n"
        "Она уже в разделе «💡 Народные инициативы». "
        "При 10 голосах «За» администратор получит сигнал.",
        keyboard=kb_idea_done(poll_id),
    )
    return True


# ============================================================================
# СЦЕНАРИИ ВВОДА
# ============================================================================


async def handle_booking_start_input(
    ctx: IncomingMessage,
    user_id: int,
    state: dict[str, Any],
    text: str,
) -> None:
    resource_id = int(state.get("resource_id", 0))
    try:
        values = parse_datetimes(text)
    except ValueError:
        await ctx.reply("❌ Неверная дата. Попробуйте ещё раз.")
        return

    if not values:
        await ctx.reply(
            "❌ Не вижу дату. Нужен формат ДД.ММ.ГГГГ ЧЧ:ММ.\n"
            "Например: 25.10.2026 18:00",
            keyboard=kb_booking_retry(resource_id),
        )
        return

    if len(values) > 2:
        await ctx.reply(
            "❌ Слишком много дат. Укажите начало и, при желании, окончание.",
            keyboard=kb_booking_retry(resource_id),
        )
        return

    start = values[0]
    now = now_naive()

    if start < now - timedelta(minutes=1):
        await ctx.reply(
            "❌ Это время уже прошло. Введите будущую дату.",
            keyboard=kb_booking_retry(resource_id),
        )
        return
    if start > now + timedelta(days=366):
        await ctx.reply(
            "❌ Слишком далёкая дата — не больше чем на год вперёд.",
            keyboard=kb_booking_retry(resource_id),
        )
        return

    if len(values) == 2:
        attempt = await try_book(user_id, resource_id, start, values[1])
        if attempt.ok:
            USER_STATES.pop(user_id, None)
            await ctx.reply(attempt.text, keyboard=kb_booking_done())
        else:
            if attempt.blocked:
                USER_STATES.pop(user_id, None)
            else:
                USER_STATES[user_id] = {
                    "step": "booking_start",
                    "resource_id": resource_id,
                }
            failure_keyboard = (
                kb_main(user_id == ADMIN_ID)
                if attempt.blocked
                else kb_booking_conflict(attempt, resource_id)
            )
            suffix = "" if attempt.blocked else "\n\nВведите новое время начала."
            await ctx.reply(
                attempt.text + suffix,
                keyboard=failure_keyboard,
            )
        return

    # Пришло только начало — предлагаем длительность.
    USER_STATES[user_id] = {
        "step": "booking_duration",
        "resource_id": resource_id,
        "start": start,
    }

    async with SessionLocal() as session:
        resource = await session.get(Resource, resource_id)
    resource_name = resource.name if resource else "ресурс"

    await ctx.reply(
        f"✅ Начало: {fmt_dt(start)}\n\n"
        f"На сколько бронируем «{resource_name}»?\n"
        "Выберите кнопкой ниже или отправьте время окончания сообщением.",
        keyboard=kb_duration(resource_id),
    )


async def handle_booking_end_input(
    ctx: IncomingMessage,
    user_id: int,
    state: dict[str, Any],
    text: str,
    from_duration_step: bool = False,
) -> None:
    resource_id = int(state.get("resource_id", 0))
    start = state.get("start")
    if not isinstance(start, datetime):
        USER_STATES.pop(user_id, None)
        await ctx.reply(
            "❌ Сессия бронирования устарела. Выберите ресурс заново.",
            keyboard=kb_main(user_id == ADMIN_ID),
        )
        return

    try:
        values = parse_datetimes(text)
    except ValueError:
        values = []

    if not values:
        if from_duration_step:
            await ctx.reply(
                "⏳ Выберите длительность кнопкой ниже или отправьте время "
                "окончания в формате 25.10.2026 20:00.",
                keyboard=kb_duration(resource_id),
            )
        else:
            await ctx.reply(
                "❌ Не вижу время окончания. Формат: 25.10.2026 20:00",
                keyboard=kb_booking_retry(resource_id),
            )
        return

    end = values[-1]
    attempt = await try_book(user_id, resource_id, start, end)
    if attempt.ok:
        USER_STATES.pop(user_id, None)
        await ctx.reply(attempt.text, keyboard=kb_booking_done())
    else:
        if attempt.blocked:
            USER_STATES.pop(user_id, None)
        else:
            USER_STATES[user_id] = {
                "step": "booking_start",
                "resource_id": resource_id,
            }
        failure_keyboard = (
            kb_main(user_id == ADMIN_ID)
            if attempt.blocked
            else kb_booking_conflict(attempt, resource_id)
        )
        suffix = "" if attempt.blocked else "\n\nВведите новое время начала."
        await ctx.reply(
            attempt.text + suffix,
            keyboard=failure_keyboard,
        )


async def finalize_damage_report(
    ctx: IncomingMessage,
    user_id: int,
    description: str,
    photo_attachment: str | None,
    photo_url: str | None,
) -> None:
    description = description.strip()
    if not (5 <= len(description) <= 800):
        await ctx.reply(
            "❌ Описание должно быть от 5 до 800 символов. "
            "Опишите, что именно сломалось.",
            keyboard=kb_damage_cancel(),
        )
        return

    report, admin_text = await create_damage_report(
        user_id,
        description,
        photo_attachment,
        photo_url,
    )

    admin_sent = await send_to_user(
        ADMIN_ID,
        admin_text,
        keyboard=kb_admin_report_notify(report.id),
        photo_url=report.photo_url,
        photo_token=report.photo_attachment,
    )
    if not admin_sent and (report.photo_url or report.photo_attachment):
        # Иногда MAX не принимает URL/токен изображения — отправляем хотя бы
        # текст с прямой ссылкой и кнопкой закрытия репорта.
        admin_sent = await send_to_user(
            ADMIN_ID,
            admin_text,
            keyboard=kb_admin_report_notify(report.id),
        )

    USER_STATES.pop(user_id, None)

    if admin_sent:
        tail = "Администратор уже получил фото и описание."
    else:
        tail = (
            "Репорт сохранён в базе. Администратор увидит его в панели "
            "«🛠 Репорты»."
        )

    await ctx.reply(
        "✅ Спасибо! Репорт о поломке сохранён.\n\n"
        f"🆔 Номер: #{report.id}\n"
        f"📝 {shorten(description, 200)}\n\n"
        f"{tail}",
        keyboard=kb_main(user_id == ADMIN_ID),
    )


async def handle_damage_photo(
    ctx: IncomingMessage,
    user_id: int,
    photo_attachment: str | None,
    photo_url: str | None,
) -> None:
    caption = (ctx.text or "").strip()
    if 5 <= len(caption) <= 800:
        await finalize_damage_report(
            ctx,
            user_id,
            caption,
            photo_attachment,
            photo_url,
        )
        return

    USER_STATES[user_id] = {
        "step": "damage_desc",
        "photo_attachment": photo_attachment,
        "photo_url": photo_url,
    }
    await ctx.reply(
        "📷 Фото получил!\n\n"
        "Теперь напишите текстом, что случилось. Например: "
        "«Сломалась ручка у шуруповёрта, трещит при работе».",
        keyboard=kb_damage_cancel(),
    )


# ============================================================================
# ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ
# ============================================================================


async def route_message(ctx: IncomingMessage) -> None:
    user_id = ctx.user_id
    sender = ctx.sender
    await ensure_user(
        user_id,
        str(sender.get("first_name") or ""),
        str(sender.get("last_name") or ""),
        sender.get("username"),
    )

    text = (ctx.text or "").strip()
    lower = text.lower()
    command_parts = text.split(maxsplit=1)
    command = command_parts[0].lower() if command_parts else ""
    args = command_parts[1] if len(command_parts) > 1 else ""

    # --- Общие команды -----------------------------------------------------
    if lower in {"/start", "start", "начать", "/menu", "меню", "menu"}:
        USER_STATES.pop(user_id, None)
        menu_text, menu_kb = await build_main_menu(user_id)
        await ctx.reply(menu_text, keyboard=menu_kb)
        return

    if lower in {"/help", "помощь", "help"}:
        help_text, help_kb = build_help(is_admin=(user_id == ADMIN_ID))
        await ctx.reply(help_text, keyboard=help_kb)
        return

    if lower in {"/cancel", "отмена", "cancel"}:
        USER_STATES.pop(user_id, None)
        menu_text, menu_kb = await build_main_menu(user_id)
        await ctx.reply(
            "Ок, отменил. Возвращаемся в меню.\n\n" + menu_text,
            keyboard=menu_kb,
        )
        return

    # --- Админские команды -------------------------------------------------
    is_admin = user_id == ADMIN_ID

    if command == "/add_resource":
        if not is_admin:
            await ctx.reply("🤔 Неизвестная команда. Нажмите /start.")
            return
        if not args.strip():
            await ctx.reply(
                "Формат:\n/add_resource Название | Описание | 120\n\n"
                "Последнее число — стандартная длительность в минутах (необязательно).",
                keyboard=kb_admin_back(),
            )
            return

        parts = [part.strip() for part in args.split("|")]
        if len(parts) < 2 or not parts[0] or not parts[1]:
            await ctx.reply(
                "❌ Нужно минимум: Название | Описание",
                keyboard=kb_admin_back(),
            )
            return

        name = parts[0]
        duration = 120
        description_parts = parts[1:]
        if len(description_parts) >= 2 and description_parts[-1].isdigit():
            duration = int(description_parts[-1])
            description_parts = description_parts[:-1]
        description = " | ".join(description_parts).strip()

        _ok, result_text, _resource_id = await admin_add_resource(
            name, description, duration
        )
        await ctx.reply(result_text, keyboard=kb_admin_back())
        return

    if command == "/add_poll":
        if not is_admin:
            await ctx.reply("🤔 Неизвестная команда. Нажмите /start.")
            return
        if not args.strip():
            await ctx.reply(
                "Формат:\n/add_poll Вопрос | Описание | 18000\n\n"
                "Последнее число — ориентир бюджета (необязательно).",
                keyboard=kb_admin_back(),
            )
            return

        parts = [part.strip() for part in args.split("|")]
        if len(parts) < 2 or not parts[0]:
            await ctx.reply(
                "❌ Нужно минимум: Вопрос | Описание",
                keyboard=kb_admin_back(),
            )
            return

        question = parts[0]
        budget = 0
        description_parts = parts[1:]
        if len(description_parts) >= 2 and description_parts[-1].isdigit():
            budget = int(description_parts[-1])
            description_parts = description_parts[:-1]
        description = " | ".join(description_parts).strip()

        _ok, result_text, _poll_id = await admin_add_poll(
            question, description, budget
        )
        await ctx.reply(result_text, keyboard=kb_admin_back())
        return

    if command == "/close_poll":
        if not is_admin:
            await ctx.reply("🤔 Неизвестная команда. Нажмите /start.")
            return
        poll_id = as_int(args.strip())
        if poll_id is None:
            await ctx.reply("Формат: /close_poll ID", keyboard=kb_admin_back())
            return
        _ok, result_text = await admin_close_poll(poll_id)
        await ctx.reply(result_text, keyboard=kb_admin_back())
        return

    if command in {"/admin", "/admin_panel"}:
        if not is_admin:
            await ctx.reply("🤔 Неизвестная команда. Нажмите /start.")
            return
        panel_text, panel_kb = await build_admin_panel()
        await ctx.reply(panel_text, keyboard=panel_kb)
        return

    if command == "/stats":
        if not is_admin:
            await ctx.reply("🤔 Неизвестная команда. Нажмите /start.")
            return
        stats_text, stats_kb = await build_admin_stats()
        await ctx.reply(stats_text, keyboard=stats_kb)
        return

    if command == "/reports":
        if not is_admin:
            await ctx.reply("🤔 Неизвестная команда. Нажмите /start.")
            return
        reports_text, reports_kb = await build_admin_reports()
        await ctx.reply(reports_text, keyboard=reports_kb)
        return

    if command == "/unblock":
        if not is_admin:
            await ctx.reply("🤔 Неизвестная команда. Нажмите /start.")
            return
        target_id = as_int(args.strip())
        if target_id is None:
            await ctx.reply("Формат: /unblock USER_ID", keyboard=kb_admin_back())
            return
        _ok, result_text = await admin_unblock_user(target_id)
        await ctx.reply(result_text, keyboard=kb_admin_back())
        return

    if lower.startswith("/"):
        await ctx.reply(
            "🤔 Неизвестная команда. Нажмите /start, чтобы открыть меню."
        )
        return

    # --- Фото и состояния --------------------------------------------------
    photo_attachment, photo_url = extract_photo_info(ctx.attachments)
    has_photo = bool(photo_attachment or photo_url)
    state = USER_STATES.get(user_id)

    if has_photo:
        if state and str(state.get("step", "")).startswith("booking"):
            await ctx.reply(
                "📅 Сейчас я жду дату и время, а не фото. "
                "Введите, например: 25.10.2026 18:00",
                keyboard=kb_booking_retry(int(state.get("resource_id", 0))),
            )
            return

        if state and state.get("step") == "damage_desc":
            if photo_attachment:
                state["photo_attachment"] = photo_attachment
            if photo_url:
                state["photo_url"] = photo_url
            caption = (ctx.text or "").strip()
            if 5 <= len(caption) <= 800:
                await finalize_damage_report(
                    ctx,
                    user_id,
                    caption,
                    state.get("photo_attachment"),
                    state.get("photo_url"),
                )
            else:
                await ctx.reply(
                    "📷 Фото обновил. Теперь пришлите текстовое описание поломки.",
                    keyboard=kb_damage_cancel(),
                )
            return

        # Если пользователь просто прислал фото — тоже принимаем как репорт.
        await handle_damage_photo(ctx, user_id, photo_attachment, photo_url)
        return

    if state:
        step = state.get("step")

        if step == "booking_start":
            await handle_booking_start_input(ctx, user_id, state, text)
            return

        if step == "booking_duration":
            try:
                parsed_end = parse_datetimes(text)
            except ValueError:
                parsed_end = []

            if parsed_end:
                await handle_booking_end_input(
                    ctx,
                    user_id,
                    state,
                    text,
                    from_duration_step=True,
                )
            else:
                await ctx.reply(
                    "⏳ Выберите длительность кнопкой ниже или отправьте время "
                    f"окончания.\n\nНачало: {fmt_dt(state['start'])}",
                    keyboard=kb_duration(int(state.get("resource_id", 0))),
                )
            return

        if step == "booking_end":
            await handle_booking_end_input(ctx, user_id, state, text)
            return

        if step == "damage":
            if text:
                await ctx.reply(
                    "📷 Нужно именно фото поломки. Пришлите фотографию "
                    "(можно с подписью) — без неё репорт не приму.",
                    keyboard=kb_damage_cancel(),
                )
            else:
                await ctx.reply(
                    state_reminder_text(state),
                    keyboard=kb_damage_cancel(),
                )
            return

        if step == "damage_desc":
            if 5 <= len(text) <= 800:
                await finalize_damage_report(
                    ctx,
                    user_id,
                    text,
                    state.get("photo_attachment"),
                    state.get("photo_url"),
                )
            else:
                await ctx.reply(
                    "❌ Описание должно быть от 5 до 800 символов. "
                    "Опишите поломку подробнее.",
                    keyboard=kb_damage_cancel(),
                )
            return

        USER_STATES.pop(user_id, None)

    if not text:
        return

    if lower in {"привет", "здравствуйте", "hi", "hello"}:
        menu_text, menu_kb = await build_main_menu(user_id)
        await ctx.reply("👋 Привет! " + menu_text, keyboard=menu_kb)
        return

    await ctx.reply(
        "🤔 Не понял. Пользуйтесь кнопками меню или нажмите /start.\n\n"
        "Подсказка: забронировать вещь можно кнопкой «📦 Забронировать».",
        keyboard=kb_main(is_admin),
    )


def state_reminder_text(state: dict[str, Any]) -> str:
    step = state.get("step")
    if step == "booking_start":
        return (
            "📅 Жду дату и время начала в формате:\n"
            "25.10.2026 18:00\n\n"
            "Можно сразу интервал:\n"
            "25.10.2026 18:00 - 25.10.2026 20:00\n\n"
            "Или нажмите кнопку ниже, чтобы выйти."
        )
    if step in {"booking_duration", "booking_end"}:
        return (
            "⏳ Выберите длительность кнопкой ниже или отправьте дату/время "
            "окончания в формате 25.10.2026 20:00."
        )
    if step == "damage":
        return "📷 Пришлите фото поломки и текстовое описание одним сообщением."
    if step == "damage_desc":
        return "📝 Пришлите текстовое описание поломки (до 800 символов)."
    return "Продолжим? Нажмите /start."


# ============================================================================
# ОБРАБОТЧИК CALLBACK-КНОПОК
# ============================================================================


async def route_callback(event: CallbackEvent) -> None:
    user_id = event.user_id
    payload = event.payload
    cmd = str(payload.get("cmd", ""))
    is_admin = user_id == ADMIN_ID

    # --- Главное меню и бронирование --------------------------------------
    if cmd == "menu":
        USER_STATES.pop(user_id, None)
        text, keyboard = await build_main_menu(user_id)
        await event.reply(text, keyboard)
        return

    if cmd == "book":
        USER_STATES.pop(user_id, None)
        text, keyboard = await build_resources_page(0)
        await event.reply(text, keyboard)
        return

    if cmd == "resources":
        page = as_int(payload.get("page"), 0) or 0
        text, keyboard = await build_resources_page(page)
        await event.reply(text, keyboard)
        return

    if cmd == "resource":
        resource_id = as_int(payload.get("id"))
        if resource_id is None:
            await event.reply("❌ Ресурс не найден.", kb_main(is_admin))
            return
        text, keyboard = await build_resource_card(resource_id)
        await event.reply(text, keyboard)
        return

    if cmd == "resource_slots":
        resource_id = as_int(payload.get("id"))
        if resource_id is None:
            await event.reply("❌ Ресурс не найден.", kb_main(is_admin))
            return
        text, keyboard = await build_resource_slots(resource_id)
        await event.reply(text, keyboard)
        return

    if cmd == "book_resource":
        resource_id = as_int(payload.get("id"))
        if resource_id is None:
            await event.reply("❌ Ресурс не найден.", kb_main(is_admin))
            return

        async with SessionLocal() as session:
            resource = await session.get(Resource, resource_id)
        if resource is None or not resource.is_active:
            await event.reply("❌ Ресурс недоступен.", kb_main(is_admin))
            return

        USER_STATES[user_id] = {
            "step": "booking_start",
            "resource_id": resource_id,
        }
        text = (
            f"📅 Бронирование: {resource.emoji} «{resource.name}»\n\n"
            "Введите начало в формате:\n25.10.2026 18:00\n\n"
            "Можно сразу указать интервал:\n"
            "25.10.2026 18:00 - 25.10.2026 20:00\n\n"
            f"Сейчас: {fmt_dt(now_naive())}\n"
            f"Стандартная длительность: {resource.default_duration_minutes} мин."
        )
        await event.reply(text, kb_booking_retry(resource_id))
        return

    if cmd == "duration":
        state = USER_STATES.get(user_id)
        resource_id = as_int(payload.get("rid"))
        hours = as_int(payload.get("hours"))
        if (
            not state
            or state.get("step") != "booking_duration"
            or resource_id is None
            or state.get("resource_id") != resource_id
            or not isinstance(state.get("start"), datetime)
            or hours is None
            or not (1 <= hours <= 12)
        ):
            USER_STATES.pop(user_id, None)
            await event.reply(
                "❌ Сессия бронирования устарела. Выберите ресурс заново.",
                kb_main(is_admin),
            )
            return

        start = state["start"]
        end = start + timedelta(hours=hours)
        attempt = await try_book(user_id, resource_id, start, end)

        if attempt.ok:
            USER_STATES.pop(user_id, None)
            await event.reply(attempt.text, kb_booking_done())
        else:
            if attempt.blocked:
                USER_STATES.pop(user_id, None)
            else:
                USER_STATES[user_id] = {
                    "step": "booking_start",
                    "resource_id": resource_id,
                }
            failure_keyboard = (
                kb_main(is_admin)
                if attempt.blocked
                else kb_booking_conflict(attempt, resource_id)
            )
            suffix = "" if attempt.blocked else "\n\nВведите новое время начала."
            await event.reply(
                attempt.text + suffix,
                failure_keyboard,
            )
        return

    if cmd == "custom_end":
        state = USER_STATES.get(user_id)
        resource_id = as_int(payload.get("rid"))
        if (
            not state
            or state.get("step") != "booking_duration"
            or resource_id is None
            or state.get("resource_id") != resource_id
            or not isinstance(state.get("start"), datetime)
        ):
            USER_STATES.pop(user_id, None)
            await event.reply(
                "❌ Сессия бронирования устарела. Выберите ресурс заново.",
                kb_main(is_admin),
            )
            return

        state["step"] = "booking_end"
        await event.reply(
            f"✍️ Начало: {fmt_dt(state['start'])}\n\n"
            "Введите дату и время окончания в формате:\n25.10.2026 20:00\n\n"
            "Можно отправить и полный интервал — я возьму последнюю дату "
            "как окончание.",
            kb_booking_retry(resource_id),
        )
        return

    if cmd == "suggest_booking":
        resource_id = as_int(payload.get("rid"))
        start = parse_iso_datetime(payload.get("start"))
        end = parse_iso_datetime(payload.get("end"))
        if resource_id is None or start is None or end is None:
            await event.reply(
                "❌ Не удалось прочитать предложенное время. Выберите заново.",
                kb_main(is_admin),
            )
            return

        attempt = await try_book(user_id, resource_id, start, end)
        if attempt.ok:
            USER_STATES.pop(user_id, None)
            await event.reply(attempt.text, kb_booking_done())
        else:
            if attempt.blocked:
                USER_STATES.pop(user_id, None)
            else:
                USER_STATES[user_id] = {
                    "step": "booking_start",
                    "resource_id": resource_id,
                }
            failure_keyboard = (
                kb_main(is_admin)
                if attempt.blocked
                else kb_booking_conflict(attempt, resource_id)
            )
            await event.reply(attempt.text, failure_keyboard)
        return

    if cmd == "my_bookings":
        page = as_int(payload.get("page"), 0) or 0
        text, keyboard = await build_my_bookings(user_id, page)
        await event.reply(text, keyboard)
        return

    if cmd == "return":
        booking_id = as_int(payload.get("id"))
        if booking_id is None:
            await event.reply("❌ Бронь не найдена.", kb_main(is_admin))
            return
        _ok, result_text = await return_booking(booking_id, user_id)
        await event.reply(
            result_text,
            kb_booking_done(),
            notification="Готово",
        )
        return

    if cmd == "cancel_booking":
        booking_id = as_int(payload.get("id"))
        if booking_id is None:
            await event.reply("❌ Бронь не найдена.", kb_main(is_admin))
            return
        _ok, result_text = await cancel_booking(
            booking_id,
            user_id,
            is_admin=is_admin,
        )
        await event.reply(
            result_text,
            kb_booking_done(),
            notification="Готово",
        )
        return

    # --- Профиль и помощь --------------------------------------------------
    if cmd == "profile":
        text, keyboard = await build_profile(user_id)
        await event.reply(text, keyboard)
        return

    if cmd == "help":
        text, keyboard = build_help(is_admin=is_admin)
        await event.reply(text, keyboard)
        return

    # --- Поломки -----------------------------------------------------------
    if cmd == "damage":
        USER_STATES[user_id] = {"step": "damage"}
        text = (
            "🛠 Сообщение о поломке\n\n"
            "Пришлите одно сообщение: фото повреждённой вещи и подпись "
            "с описанием.\n\n"
            "Например: «У шуруповёрта треснул корпус, искрит».\n\n"
            "Если фото без подписи — я попрошу описание отдельно."
        )
        await event.reply(text, kb_damage_cancel())
        return

    # --- Голосования -------------------------------------------------------
    if cmd == "polls":
        page = as_int(payload.get("page"), 0) or 0
        text, keyboard = await build_polls(user_id, page)
        await event.reply(text, keyboard)
        return

    if cmd == "poll":
        poll_id = as_int(payload.get("id"))
        if poll_id is None:
            await event.reply("❌ Голосование не найдено.", kb_main(is_admin))
            return
        text, keyboard = await build_poll_view(user_id, poll_id)
        await event.reply(text, keyboard)
        return

    if cmd == "vote":
        poll_id = as_int(payload.get("poll_id"))
        value = as_int(payload.get("value"))
        if poll_id is None or value not in (1, -1):
            await event.reply("❌ Некорректный голос.", kb_main(is_admin))
            return
        _ok, _msg = await cast_vote(user_id, poll_id, value)
        text, keyboard = await build_poll_view(user_id, poll_id)
        await event.reply(
            text,
            keyboard,
            notification="Голос учтён",
        )
        return

    # --- Админ-панель ------------------------------------------------------
    if cmd.startswith("admin"):
        if not is_admin:
            await event.reply("⛔ Недоступно.", kb_main(False))
            return

        if cmd == "admin":
            text, keyboard = await build_admin_panel()
            await event.reply(text, keyboard)
            return

        if cmd == "admin_stats":
            text, keyboard = await build_admin_stats()
            await event.reply(text, keyboard)
            return

        if cmd == "admin_reports":
            text, keyboard = await build_admin_reports()
            await event.reply(text, keyboard)
            return

        if cmd == "admin_inventory":
            text, keyboard = await build_admin_inventory()
            await event.reply(text, keyboard)
            return

        if cmd == "admin_polls":
            text, keyboard = await build_admin_polls()
            await event.reply(text, keyboard)
            return

        if cmd == "admin_resolve":
            report_id = as_int(payload.get("id"))
            if report_id is None:
                await event.reply("❌ Репорт не найден.", kb_admin_back())
                return
            ok, _result_text, reporter_id = await resolve_damage_report(report_id)
            text, keyboard = await build_admin_reports()
            # Сначала отвечаем на callback, чтобы MAX не ждал; уведомление
            # автору репорта отправляем уже после этого.
            await event.reply(text, keyboard)
            if ok and reporter_id is not None:
                await send_to_user(
                    reporter_id,
                    f"✅ Ваш репорт о поломке #{report_id} отмечен как решённый. "
                    "Спасибо, что сообщили!",
                )
            return

        if cmd == "admin_close_poll":
            poll_id = as_int(payload.get("id"))
            if poll_id is None:
                await event.reply("❌ Голосование не найдено.", kb_admin_back())
                return
            await admin_close_poll(poll_id)
            text, keyboard = await build_admin_polls()
            await event.reply(text, keyboard)
            return

    # Неизвестная/устаревшая кнопка.
    await event.reply(
        "🤷 Кнопка устарела. Откройте меню заново.",
        kb_main(is_admin),
    )


# ============================================================================
# РАЗБОР UPDATE-СОБЫТИЙ MAX
# ============================================================================


async def handle_message_created(update: dict[str, Any]) -> None:
    message = update.get("message") or {}
    if not isinstance(message, dict):
        return

    sender = message.get("sender") or {}
    if not isinstance(sender, dict):
        return
    if sender.get("is_bot"):
        return

    user_id = as_int(sender.get("user_id"))
    if not user_id:
        return

    target = target_from_message(message)
    if not target:
        logger.warning("Не удалось определить получателя для message_created: %s", message)
        return

    body = message.get("body") or {}
    if not isinstance(body, dict):
        body = {}
    text = str(body.get("text") or "")
    attachments = body.get("attachments") or []
    if not isinstance(attachments, list):
        attachments = []

    ctx = IncomingMessage(
        user_id=user_id,
        target=target,
        text=text,
        attachments=attachments,
        sender=sender,
    )

    try:
        await route_message(ctx)
    except Exception:
        logger.exception("Ошибка при обработке сообщения от %s", user_id)
        with suppress(Exception):
            await ctx.reply(
                "😔 Что-то пошло не так. Попробуйте ещё раз или нажмите /start.",
                keyboard=kb_main(user_id == ADMIN_ID),
            )


async def handle_callback_update(update: dict[str, Any]) -> None:
    callback = update.get("callback") or {}
    if not isinstance(callback, dict):
        return

    callback_id = callback.get("callback_id")
    if not callback_id:
        return
    callback_id = str(callback_id)

    user = callback.get("user") or {}
    if not isinstance(user, dict):
        return

    user_id = as_int(user.get("user_id"))
    if not user_id:
        return

    original_message = update.get("message")
    # В оригинальном сообщении callback sender — бот, а не нажавший пользователь.
    target = target_from_message(original_message, fallback_user_id=user_id)
    if not target:
        target = {"user_id": user_id}

    await ensure_user(
        user_id,
        str(user.get("first_name") or ""),
        str(user.get("last_name") or ""),
        user.get("username"),
    )

    event = CallbackEvent(
        callback_id=callback_id,
        user_id=user_id,
        target=target,
        payload=decode_callback_payload(callback.get("payload")),
        user=user,
        original_message=original_message if isinstance(original_message, dict) else None,
    )

    try:
        await route_callback(event)
    except Exception:
        logger.exception("Ошибка при обработке callback от %s", user_id)
        with suppress(Exception):
            await event.reply(
                "😔 Не удалось обработать кнопку. Попробуйте ещё раз.",
                kb_main(user_id == ADMIN_ID),
            )


async def handle_bot_started(update: dict[str, Any]) -> None:
    user = update.get("user") or {}
    if not isinstance(user, dict):
        return

    user_id = as_int(user.get("user_id"))
    if not user_id:
        return

    await ensure_user(
        user_id,
        str(user.get("first_name") or ""),
        str(user.get("last_name") or ""),
        user.get("username"),
    )

    menu_text, menu_kb = await build_main_menu(user_id)
    await send_to_user(user_id, "👋 Добро пожаловать в MAX!\n\n" + menu_text, menu_kb)


async def handle_update(update: dict[str, Any]) -> None:
    update_type = update.get("update_type")
    if update_type == "message_created":
        await handle_message_created(update)
    elif update_type == "message_callback":
        await handle_callback_update(update)
    elif update_type == "bot_started":
        await handle_bot_started(update)
    else:
        logger.debug("Пропущен тип update: %s", update_type)


# ============================================================================
# LONG POLLING
# ============================================================================


async def poll_forever(api: MaxAPI) -> None:
    """
    Бесконечный Long Polling MAX.

    Маркер указывает на следующее ожидаемое обновление. После получения
    ответа сохраняем marker и передаём его в следующий запрос.
    """
    marker: int | None = None
    delay = 2

    while True:
        try:
            data = await api.get_updates(marker=marker)
            updates = data.get("updates") or []
            if not isinstance(updates, list):
                updates = []
            new_marker = data.get("marker")

            if new_marker is not None:
                marker = as_int(new_marker, marker)
            if updates:
                logger.debug("Получено обновлений MAX: %s", len(updates))
            else:
                # Сервер может вернуть пустой ответ раньше таймаута.
                await asyncio.sleep(1)

            for update in updates:
                if not isinstance(update, dict):
                    continue
                try:
                    await handle_update(update)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Ошибка обработки update: %s", update)

            delay = 2
        except asyncio.CancelledError:
            raise
        except MaxAPIError as error:
            if error.status == 401:
                logger.critical(
                    "MAX отклонил токен (401). Проверьте MAX_TOKEN. Long Polling остановлен."
                )
                return
            logger.error("Ошибка Long Polling MAX: %s. Повтор через %s с", error, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
        except Exception:
            logger.exception(
                "Неожиданная ошибка Long Polling. Повтор через %s с", delay
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


# ============================================================================
# ФОНОВАЯ ЗАДАЧА: НАПОМИНАНИЯ ЗА 15 МИНУТ
# ============================================================================


async def send_booking_reminders() -> None:
    """Находит активные брони, до конца которых осталось <= 15 минут."""
    now = now_naive()
    deadline = now + timedelta(minutes=REMINDER_MINUTES)

    async with SessionLocal() as session:
        rows = (
            await session.execute(
                select(Booking, Resource, User)
                .join(Resource, Resource.id == Booking.resource_id)
                .join(User, User.id == Booking.user_id)
                .where(
                    Booking.status == "active",
                    Booking.notified_15m.is_(False),
                    Booking.end_time > now,
                    Booking.end_time <= deadline,
                )
                .order_by(Booking.end_time)
            )
        ).all()

        if not rows:
            return

        for booking, resource, user in rows:
            seconds_left = max(0.0, (booking.end_time - now).total_seconds())
            minutes_left = max(1, int((seconds_left + 59) // 60))
            first_name = (user.first_name or "сосед").strip()
            if first_name == "Сосед":
                first_name = "сосед"

            text = (
                f"⏰ Эй, {first_name}, ты скоро должен вернуть «{resource.name}»!\n\n"
                f"Бронь №{booking.id} заканчивается в {fmt_dt(booking.end_time)}.\n"
                f"Осталось примерно {minutes_left} мин.\n\n"
                "Верни вещь вовремя и получи +1 балл к репутации."
            )

            sent = await send_to_user(
                user.id,
                text,
                keyboard=kb_return_notify(booking.id),
            )

            # Помечаем даже при ошибке: иначе при закрытых ЛС бот будет
            # пытаться отправить одно и то же каждую минуту.
            booking.notified_15m = True
            await session.commit()
            if sent:
                logger.info(
                    "Напоминание по брони #%s отправлено пользователю %s",
                    booking.id,
                    user.id,
                )
            else:
                logger.warning(
                    "Не удалось отправить напоминание по брони #%s пользователю %s",
                    booking.id,
                    user.id,
                )


async def notification_worker() -> None:
    """Каждую минуту проверяет БД и рассылает напоминания."""
    await asyncio.sleep(10)  # даём боту инициализироваться
    while True:
        try:
            await send_booking_reminders()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка в фоновой задаче напоминаний")
        await asyncio.sleep(REMINDER_INTERVAL_SECONDS)


# ============================================================================
# ЗАПУСК
# ============================================================================


async def main() -> None:
    global API

    if not MAX_TOKEN:
        raise RuntimeError(
            "Не задан MAX_TOKEN. Скопируйте .env.example в .env и укажите "
            "токен бота MAX из настроек на business.max.ru."
        )
    if ADMIN_ID <= 0:
        raise RuntimeError("ADMIN_ID должен быть положительным MAX user_id.")
    if ADMIN_ID == 12345:
        logger.warning(
            "ADMIN_ID всё ещё равен 12345 (заглушка). Репорты и админ-команды "
            "не будут работать, пока вы не укажете свой MAX user_id."
        )

    await init_db()
    logger.info("База данных: %s", DB_PATH)
    logger.info("MAX Bot API: %s", MAX_API_BASE)
    logger.info("Админ MAX ID: %s", ADMIN_ID)

    reminder_task: asyncio.Task | None = None
    try:
        async with MaxAPI(MAX_TOKEN) as api:
            API = api

            try:
                me = await api.get_me()
                logger.info(
                    "Бот MAX: %s (id %s, username @%s)",
                    me.get("name") or me.get("first_name") or "без имени",
                    me.get("user_id"),
                    me.get("username") or "—",
                )
            except MaxAPIError as error:
                if error.status == 401:
                    raise RuntimeError(
                        "MAX_TOKEN недействителен (401). Проверьте токен бота."
                    ) from error
                logger.warning("Не удалось получить /me: %s", error)
            except Exception:
                logger.warning("Не удалось проверить /me", exc_info=True)

            reminder_task = asyncio.create_task(
                notification_worker(),
                name="booking-reminders-max",
            )

            logger.info("Бот «Соседский ресурс» для MAX запущен. Long Polling...")
            try:
                await poll_forever(api)
            finally:
                # Останавливаем фоновую задачу до закрытия HTTP-сессии.
                reminder_task.cancel()
                with suppress(asyncio.CancelledError):
                    await reminder_task
                reminder_task = None
    finally:
        if reminder_task is not None:
            reminder_task.cancel()
            with suppress(asyncio.CancelledError):
                await reminder_task
        API = None
        await engine.dispose()
        logger.info("Бот остановлен, соединения закрыты.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
