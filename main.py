"""
«Соседский ресурс» — VK-бот для посёлка.
=========================================

Один файл: модели SQLAlchemy 2.0 (async), бизнес-логика, клавиатуры,
обработчики vkbottle и фоновая задача напоминаний.

УСТАНОВКА
---------
    pip install "vkbottle>=4.9,<5" "SQLAlchemy[asyncio]>=2.0,<2.1" "aiosqlite>=0.20" "python-dotenv>=1.0" "truststore>=0.9"

ЗАПУСК
------
    1. Создайте сообщество ВКонтакте.
    2. Включите LongPoll API: Управление -> Работа с API -> LongPoll API.
    3. В типах событий включите:
       - Входящие сообщения (message_new);
       - Действия с сообщениями / нажатия callback-кнопок (message_event).
    4. Скопируйте .env.example в .env.
    5. Впишите в .env токен сообщества (VK_TOKEN) и свой VK ID (ADMIN_ID).
    6. python main.py
    Секреты больше не хранятся в исходниках.

ТЕСТЫ
-----
    pip install -r requirements.txt
    pytest -q
    Тесты используют отдельную SQLite-базу и не обращаются к VK API.

ВАЖНО
-----
* Время в боте — локальное время сервера (naive datetime). Для хакатонного
  MVP это удобно: на ноутбуке и на сервере в одном часовом поясе всё совпадает.
* SQLite-файл создаётся рядом с main.py. В этой версии новые поля залога
  добавляются мягкой миграцией. Если хотите гарантированно чистую схему —
  остановите бота и удалите sosedskiy_resurs.db перед первым запуском:
  SQLAlchemy пересоздаст таблицы с новыми колонками. Внимание: удаление БД
  удалит и все старые брони/репутацию.
* Callback-кнопки требуют включённого события message_event в настройках
  сообщества, иначе VK не будет доставлять нажатия.
"""

from __future__ import annotations

import truststore
truststore.inject_into_ssl()

import asyncio
import json
import logging
import os
import re
import secrets
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

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

from vkbottle import Callback, GroupEventType, Keyboard, KeyboardButtonColor
from vkbottle.bot import Bot, Message, MessageEvent

# ============================================================================
# НАСТРОЙКИ — ЧИТАЮТСЯ ИЗ .env / ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
# ============================================================================

load_dotenv(Path(__file__).resolve().with_name(".env"))

VK_TOKEN = os.getenv("VK_TOKEN", "").strip()
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
except ValueError:
    ADMIN_ID = 0

# ============================================================================
# КОНСТАНТЫ И ЛОГИРОВАНИЕ
# ============================================================================

DB_PATH = Path(__file__).resolve().with_name("sosedskiy_resurs.db")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite+aiosqlite:///{DB_PATH.as_posix()}",
)

MIN_BOOKING_MINUTES = 15
MAX_BOOKING_HOURS = 24
RETURN_GRACE_MINUTES = 0
REMINDER_MINUTES = 15
REMINDER_INTERVAL_SECONDS = 60

CANCEL_PENALTY = -5
LATE_CANCEL_PENALTY = -3
LATE_RETURN_PENALTY = -2
ON_TIME_BONUS = 1

# Стартовый капитал репутации: у каждого нового соседа сразу 1000 баллов.
# Существующим пользователям делаем однократный «бонус новосёла» при миграции.
STARTING_REPUTATION = 1000

# Экономический эффект: столько «стоила бы» покупка/аренда вещи,
# если бы сосед делал всё сам. Начисляем за каждую успешную бронь.
SAVED_RUB_PER_BOOKING = 500

# Залог за бронь: списывается при создании и возвращается при успешном
# возврате. При просрочке или поломке — сгорает.
DEPOSIT_POINTS = 3

# Народные инициативы: уведомление админу при количестве голосов «За» > 5,
# то есть начиная с шестого голоса.
MAX_IDEAS_PER_DAY = 3
IDEA_VOTES_THRESHOLD = 6

# Бонус: 24-часовая блокировка за систематические нарушения.
VIOLATION_WINDOW_DAYS = 30
VIOLATION_LIMIT = 2
BLOCK_HOURS = 24

# Поиск ближайшего свободного окна.
SUGGESTION_STEP_MINUTES = 30
SUGGESTION_DAYS_BEFORE = 3
SUGGESTION_DAYS_AFTER = 14

LEVELS: tuple[tuple[int, str, str], ...] = (
    (30, "Староста", "👑"),
    (10, "Сосед", "🤝"),
    (0, "Новичок", "🌱"),
)

WEEKDAYS = ("пн", "вт", "ср", "чт", "пт", "сб", "вс")

# Формат из задания: ДД.ММ.ГГГГ ЧЧ:ММ.
# Разрешаем также разделители «.», «/», «-» и один-два знака в минутах,
# чтобы пользователю было проще, но основной формат остаётся тем же.
DATETIME_RE = re.compile(
    r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})\s+(\d{1,2}):(\d{1,2})"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("sosedskiy-resurs")

# FSM-состояния теперь хранятся в SQLite (таблица user_states).
# После рестарта бот продолжит диалог с того же шага.
NAME_CACHE: dict[int, tuple[str, str, str | None]] = {}

# Сериализует создание броней внутри процесса: два одновременных запроса
# не смогут пройти проверку пересечений и создать конфликтующие записи.
BOOKING_LOCK = asyncio.Lock()

# Отдельная блокировка для возврата/отмены: защищает от двойного нажатия
# кнопки, когда два обработчика одновременно видят статус active.
BOOKING_ACTION_LOCK = asyncio.Lock()


# ============================================================================
# БАЗА ДАННЫХ: МОДЕЛИ
# ============================================================================
#
# ВАЖНО: в этой версии появились:
# - таблица FSM-состояний user_states;
# - поля залога bookings.deposit_amount, bookings.deposit_status;
# - привязка репортов к броням damage_reports.booking_id;
# - краудфандинг polls.goal_points, polls.collected_points, polls.goal_notified.
# Если вы хотите гарантированно пересоздать схему — остановите бота, удалите
# файл sosedskiy_resurs.db и запустите main.py заново: SQLAlchemy создаст
# таблицы уже с новыми колонками. Мягкая миграция ниже тоже умеет добавлять
# эти поля в существующую базу, но удаление БД — самый чистый путь для MVP.
# Внимание: при удалении БД пропадут старые брони, залоги, состояния и история.


def now_naive() -> datetime:
    """Локальное «настенное» время без tzinfo — так его удобно хранить в SQLite."""
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


class UserState(Base):
    """Персистентное FSM-состояние пользователя (переживает рестарт бота)."""

    __tablename__ = "user_states"

    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
        autoincrement=False,
    )
    step: Mapped[str] = mapped_column(String(32), index=True)
    context_json: Mapped[str] = mapped_column(SAText, default="{}")
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
    # Залог: held (заморожен) | returned (возвращён) | forfeited (сгорел).
    deposit_amount: Mapped[int] = mapped_column(Integer, default=DEPOSIT_POINTS)
    deposit_status: Mapped[str] = mapped_column(String(16), default="held")


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
    # Краудфандинг в баллах: цель и уже собранная сумма.
    goal_points: Mapped[int] = mapped_column(Integer, default=50000)
    collected_points: Mapped[int] = mapped_column(Integer, default=0)
    goal_notified: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    # Народная инициатива, созданная обычным пользователем.
    # Индекс создаётся в migrate_sqlite_schema() — так безопаснее для старой БД.
    is_user_idea: Mapped[bool] = mapped_column(Boolean, default=False)
    # Уведомление админу о достижении порога голосов отправляется один раз.
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
    photo_attachment: Mapped[str | None] = mapped_column(String(255), nullable=True)
    photo_url: Mapped[str | None] = mapped_column(SAText, nullable=True)
    # Если поломка произошла во время активной брони — привязываем репорт
    # к брони и сжигаем её залог.
    booking_id: Mapped[int | None] = mapped_column(
        ForeignKey("bookings.id", ondelete="SET NULL"),
        nullable=True,
    )
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


# ============================================================================
# FSM: СОСТОЯНИЯ ДИАЛОГА В SQLITE
# ============================================================================


def _state_json_default(value: Any) -> Any:
    """Даты в context_json сохраняем специальным маркером."""
    if isinstance(value, datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    raise TypeError(f"Неподдерживаемый тип в состоянии: {type(value)!r}")


def _state_json_hook(value: dict[str, Any]) -> Any:
    if value.get("__type__") == "datetime" and "value" in value:
        try:
            return datetime.fromisoformat(str(value["value"]))
        except ValueError:
            return value
    return value


async def get_state(user_id: int) -> dict[str, Any] | None:
    """Загружает шаг и контекст пользователя из SQLite."""
    async with SessionLocal() as session:
        row = await session.get(UserState, user_id)
        if row is None:
            return None
        try:
            context = json.loads(
                row.context_json or "{}",
                object_hook=_state_json_hook,
            )
        except (TypeError, ValueError):
            logger.exception("Повреждён context_json у user_state %s", user_id)
            context = {}
        if not isinstance(context, dict):
            context = {}
        context["step"] = row.step
        return context


async def set_state(user_id: int, state: dict[str, Any] | None) -> None:
    """Сохраняет/обновляет состояние пользователя в SQLite."""
    if not state:
        await clear_state(user_id)
        return

    step = str(state.get("step") or "").strip()
    if not step:
        await clear_state(user_id)
        return

    context = {key: value for key, value in state.items() if key != "step"}
    try:
        context_json = json.dumps(
            context,
            ensure_ascii=False,
            default=_state_json_default,
        )
    except TypeError:
        logger.exception("Не удалось сериализовать состояние пользователя %s", user_id)
        return

    now = now_naive()
    async with SessionLocal() as session:
        row = await session.get(UserState, user_id)
        if row is None:
            session.add(
                UserState(
                    user_id=user_id,
                    step=step,
                    context_json=context_json,
                    updated_at=now,
                )
            )
        else:
            row.step = step
            row.context_json = context_json
            row.updated_at = now
        await session.commit()


async def clear_state(user_id: int) -> None:
    """Удаляет незавершённый сценарий пользователя."""
    async with SessionLocal() as session:
        row = await session.get(UserState, user_id)
        if row is not None:
            await session.delete(row)
            await session.commit()


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
    """Возвращает имена колонок SQLite-таблицы."""
    result = await connection.exec_driver_sql(f"PRAGMA table_info({table})")
    return {str(row[1]) for row in result.fetchall()}


async def _ensure_sqlite_column(
    connection: Any,
    table: str,
    column: str,
    ddl: str,
) -> None:
    """Мягкая миграция для уже работающей базы: добавляет колонку при необходимости."""
    columns = await _table_columns(connection, table)
    if column not in columns:
        await connection.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        logger.info("Миграция БД: добавлена колонка %s.%s", table, column)


async def migrate_sqlite_schema(connection: Any) -> None:
    """
    create_all() создаёт только отсутствующие таблицы, но не меняет уже
    существующие. Поэтому для запущенного бота аккуратно добавляем новые поля.
    """
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
    await _ensure_sqlite_column(
        connection,
        "polls",
        "goal_points",
        "goal_points INTEGER NOT NULL DEFAULT 50000",
    )
    await _ensure_sqlite_column(
        connection,
        "polls",
        "collected_points",
        "collected_points INTEGER NOT NULL DEFAULT 0",
    )
    await _ensure_sqlite_column(
        connection,
        "polls",
        "goal_notified",
        "goal_notified BOOLEAN NOT NULL DEFAULT 0",
    )
    # Для старых броней залог уже не удерживался, поэтому default='returned'.
    # Новые брони получат 'held' из ORM-модели.
    await _ensure_sqlite_column(
        connection,
        "bookings",
        "deposit_amount",
        "deposit_amount INTEGER NOT NULL DEFAULT 3",
    )
    await _ensure_sqlite_column(
        connection,
        "bookings",
        "deposit_status",
        "deposit_status VARCHAR(16) NOT NULL DEFAULT 'returned'",
    )
    await _ensure_sqlite_column(
        connection,
        "damage_reports",
        "booking_id",
        "booking_id INTEGER",
    )
    await connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_polls_is_user_idea ON polls (is_user_idea)"
    )


async def init_db() -> None:
    """Создаёт таблицы, выполняет мягкие миграции и наполняет стартовые данные."""
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await migrate_sqlite_schema(connection)

        # Одноразовый бонус новосёла для уже существующей базы:
        # у всех, у кого меньше 1000 баллов, поднимаем баланс до 1000.
        # PRAGMA user_version не даёт повторять это при каждом рестарте,
        # чтобы заработанные штрафы не обнулялись.
        user_version = (
            await connection.exec_driver_sql("PRAGMA user_version")
        ).scalar_one()
        if int(user_version) < 1:
            await connection.exec_driver_sql(
                "UPDATE users SET points = ? WHERE points < ?",
                (STARTING_REPUTATION, STARTING_REPUTATION),
            )
            await connection.exec_driver_sql("PRAGMA user_version = 1")
            logger.info(
                "Миграция репутации: всем существующим соседям начислено "
                "до %s баллов",
                STARTING_REPUTATION,
            )

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
                        goal_points=50000,
                        is_active=True,
                    )
                )
            logger.info("Добавлены стартовые голосования: %s", len(DEFAULT_POLLS))

        await session.commit()


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================


def shorten(text: str, limit: int = 40) -> str:
    """Обрезает строку под ограничение длины кнопки/сообщения."""
    text = (text or "").strip()
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
    """12345 -> «12 345» — аккуратно для профиля и админ-статистики."""
    return f"{int(amount or 0):,}".replace(",", " ")


def parse_iso_datetime(value: Any) -> datetime | None:
    """Разбирает ISO-дату из callback payload (для кнопки «Занять окно»)."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except (TypeError, ValueError):
        return None
    # В БД храним naive-время; timezone-aware payload от пользователя отклоняем.
    if parsed.tzinfo is not None:
        return None
    return parsed


def ceil_to_step(value: datetime, step_minutes: int = SUGGESTION_STEP_MINUTES) -> datetime:
    """Округляет время вверх до шага поиска (например, до 30 минут)."""
    base = value.replace(second=0, microsecond=0)
    if base < value:
        base += timedelta(minutes=1)
    remainder = base.minute % step_minutes
    if remainder:
        base += timedelta(minutes=step_minutes - remainder)
    return base


def reputation_score(points: int) -> int:
    """
    Уровни считаем от стартового капитала в 1000 баллов:
    1000 — Новичок, 1010 — Сосед, 1030 — Староста.
    """
    return max(0, int(points or 0) - STARTING_REPUTATION)


def level_info(points: int) -> tuple[str, str, int, int | None]:
    """Возвращает (название, иконка, нижняя граница, следующая граница)."""
    score = reputation_score(points)
    for threshold, name, icon in LEVELS:
        if score >= threshold:
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


def new_random_id() -> int:
    """Уникальный random_id для messages.send (VK требует его для ботов)."""
    return (int(time.time() * 1000) + secrets.randbelow(1000)) % 2_000_000_000 + 1


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


def extract_photo_info(message: Message) -> tuple[str | None, str | None]:
    """
    Достаёт из входящего сообщения:
      * строку вложения вида photo123_456_access (её можно переслать админу);
      * URL самой большой версии фото (для архива в БД).

    Используем message.attachments и служебный get_attachment_strings(),
    который vkbottle собирает из тех же attachments.
    """
    attachment_string: str | None = None
    try:
        all_strings = message.get_attachment_strings() or []
    except Exception:
        logger.debug("Не удалось собрать attachment-строки", exc_info=True)
        all_strings = []
    for item in all_strings:
        if item.startswith("photo"):
            attachment_string = item
            break

    photo_url: str | None = None
    for attachment in message.attachments or []:
        photo = getattr(attachment, "photo", None)
        if photo is None:
            continue

        sizes = getattr(photo, "sizes", None) or []
        if sizes:
            best = max(
                sizes,
                key=lambda size: (getattr(size, "width", 0) or 0)
                * (getattr(size, "height", 0) or 0),
            )
            photo_url = getattr(best, "url", None)
            break

    # Резервный вариант: собираем attachment-строку вручную по фото-объекту.
    if attachment_string is None and photo_url is not None:
        for attachment in message.attachments or []:
            photo = getattr(attachment, "photo", None)
            if photo is None:
                continue
            owner_id = getattr(photo, "owner_id", None)
            photo_id = getattr(photo, "id", None)
            if owner_id is not None and photo_id is not None:
                attachment_string = f"photo{owner_id}_{photo_id}"
                access_key = getattr(photo, "access_key", None)
                if access_key:
                    attachment_string += f"_{access_key}"
            break

    return attachment_string, photo_url


# ============================================================================
# КЛАВИАТУРЫ
# ============================================================================

PRIMARY = KeyboardButtonColor.PRIMARY
SECONDARY = KeyboardButtonColor.SECONDARY
POSITIVE = KeyboardButtonColor.POSITIVE
NEGATIVE = KeyboardButtonColor.NEGATIVE


def kb_main(is_admin: bool) -> str:
    """Главное меню. Только callback-кнопки, как требует задание."""
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(Callback("📦 Забронировать", {"cmd": "book"}), color=PRIMARY)
    keyboard.add(Callback("🗓 Мои брони", {"cmd": "my_bookings", "page": 0}), color=SECONDARY)
    keyboard.row()
    keyboard.add(Callback("⭐ Моя репутация", {"cmd": "profile"}), color=SECONDARY)
    keyboard.add(Callback("🗳 Голосования", {"cmd": "polls"}), color=SECONDARY)
    keyboard.add(Callback("💡 Предложить идею", {"cmd": "idea"}), color=POSITIVE)
    keyboard.row()
    keyboard.add(Callback("🛠 Сообщить о поломке", {"cmd": "damage"}), color=NEGATIVE)
    keyboard.add(Callback("ℹ️ Помощь", {"cmd": "help"}), color=SECONDARY)
    if is_admin:
        keyboard.row()
        keyboard.add(Callback("🛡 Админ-панель", {"cmd": "admin"}), color=POSITIVE)
    return keyboard.get_json()


def kb_resources_page(page: int, total_pages: int, resources: list[Resource]) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    for index, resource in enumerate(resources):
        if index and index % 2 == 0:
            keyboard.row()
        keyboard.add(
            Callback(
                f"{resource.emoji} {shorten(resource.name, 28)}",
                {"cmd": "resource", "id": resource.id},
            ),
            color=PRIMARY if index == 0 else SECONDARY,
        )

    if resources:
        keyboard.row()
    if total_pages > 1:
        if page > 0:
            keyboard.add(
                Callback("◀️ Назад", {"cmd": "resources", "page": page - 1}),
                color=SECONDARY,
            )
        if page + 1 < total_pages:
            keyboard.add(
                Callback("Вперёд ▶️", {"cmd": "resources", "page": page + 1}),
                color=SECONDARY,
            )
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_resource_card(resource_id: int) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("✅ Забронировать", {"cmd": "book_resource", "id": resource_id}),
        color=PRIMARY,
    )
    keyboard.add(
        Callback("🕒 Занятость", {"cmd": "resource_slots", "id": resource_id}),
        color=SECONDARY,
    )
    keyboard.row()
    keyboard.add(Callback("🔙 К списку", {"cmd": "resources", "page": 0}), color=SECONDARY)
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_resource_slots(resource_id: int) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("✅ Забронировать", {"cmd": "book_resource", "id": resource_id}),
        color=PRIMARY,
    )
    keyboard.add(
        Callback("🔙 К карточке", {"cmd": "resource", "id": resource_id}),
        color=SECONDARY,
    )
    keyboard.row()
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_duration(resource_id: int) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("1 час", {"cmd": "duration", "rid": resource_id, "hours": 1}),
        color=SECONDARY,
    )
    keyboard.add(
        Callback("2 часа", {"cmd": "duration", "rid": resource_id, "hours": 2}),
        color=PRIMARY,
    )
    keyboard.add(
        Callback("3 часа", {"cmd": "duration", "rid": resource_id, "hours": 3}),
        color=SECONDARY,
    )
    keyboard.row()
    keyboard.add(
        Callback("4 часа", {"cmd": "duration", "rid": resource_id, "hours": 4}),
        color=SECONDARY,
    )
    keyboard.add(
        Callback("✍️ Своё окончание", {"cmd": "custom_end", "rid": resource_id}),
        color=SECONDARY,
    )
    keyboard.row()
    keyboard.add(Callback("❌ Отмена", {"cmd": "menu"}), color=NEGATIVE)
    return keyboard.get_json()


def kb_booking_retry(resource_id: int) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("✍️ Другое время", {"cmd": "book_resource", "id": resource_id}),
        color=PRIMARY,
    )
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_booking_conflict(attempt: "BookingAttempt", fallback_resource_id: int) -> str:
    """Клавиатура после конфликта: сразу занять найденное окно."""
    keyboard = Keyboard(one_time=False, inline=True)
    if (
        attempt.suggested_resource_id is not None
        and attempt.suggested_start is not None
        and attempt.suggested_end is not None
    ):
        label = f"✅ Занять {fmt_dt_short(attempt.suggested_start)}"
        keyboard.add(
            Callback(
                label,
                {
                    "cmd": "suggest_booking",
                    "rid": attempt.suggested_resource_id,
                    "start": attempt.suggested_start.isoformat(timespec="minutes"),
                    "end": attempt.suggested_end.isoformat(timespec="minutes"),
                },
            ),
            color=POSITIVE,
        )
        keyboard.row()
    keyboard.add(
        Callback(
            "✍️ Другое время",
            {"cmd": "book_resource", "id": fallback_resource_id},
        ),
        color=PRIMARY,
    )
    keyboard.add(Callback("📦 Другие ресурсы", {"cmd": "book"}), color=SECONDARY)
    keyboard.row()
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_idea_done(poll_id: int) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("💡 Открыть идею", {"cmd": "poll", "id": poll_id}),
        color=POSITIVE,
    )
    keyboard.add(
        Callback(
            "💡 К народным идеям",
            {"cmd": "polls", "page": 0, "ideas": 1},
        ),
        color=SECONDARY,
    )
    keyboard.row()
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_admin_idea_open(poll_id: int) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("💡 Открыть инициативу", {"cmd": "poll", "id": poll_id}),
        color=POSITIVE,
    )
    keyboard.add(Callback("🛡 В админку", {"cmd": "admin"}), color=SECONDARY)
    return keyboard.get_json()


def kb_booking_done() -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("🗓 Мои брони", {"cmd": "my_bookings", "page": 0}),
        color=PRIMARY,
    )
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_my_bookings(bookings: list[Booking], page: int, total_pages: int) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    for booking in bookings:
        keyboard.add(
            Callback(
                f"✅ Вернуть #{booking.id}",
                {"cmd": "return", "id": booking.id},
            ),
            color=POSITIVE,
        )
        keyboard.add(
            Callback(
                f"❌ Отмена #{booking.id}",
                {"cmd": "cancel_booking", "id": booking.id},
            ),
            color=NEGATIVE,
        )
        keyboard.row()

    if total_pages > 1:
        if page > 0:
            keyboard.add(
                Callback(
                    "◀️ Назад",
                    {"cmd": "my_bookings", "page": page - 1},
                ),
                color=SECONDARY,
            )
        if page + 1 < total_pages:
            keyboard.add(
                Callback(
                    "Вперёд ▶️",
                    {"cmd": "my_bookings", "page": page + 1},
                ),
                color=SECONDARY,
            )
        keyboard.row()

    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_profile() -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("🗓 Мои брони", {"cmd": "my_bookings", "page": 0}),
        color=PRIMARY,
    )
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_polls_list(
    polls: list[Poll],
    page: int = 0,
    total_pages: int = 1,
    ideas_only: bool = False,
) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    for poll in polls:
        prefix = "💡" if poll.is_user_idea else "🗳"
        keyboard.add(
            Callback(
                f"{prefix} {shorten(poll.question, 30)}",
                {"cmd": "poll", "id": poll.id},
            ),
            color=SECONDARY,
        )
        keyboard.row()

    if total_pages > 1:
        if page > 0:
            keyboard.add(
                Callback(
                    "◀️ Назад",
                    {"cmd": "polls", "page": page - 1, "ideas": int(ideas_only)},
                ),
                color=SECONDARY,
            )
        if page + 1 < total_pages:
            keyboard.add(
                Callback(
                    "Вперёд ▶️",
                    {"cmd": "polls", "page": page + 1, "ideas": int(ideas_only)},
                ),
                color=SECONDARY,
            )
        keyboard.row()

    if ideas_only:
        keyboard.add(
            Callback("🗳 Опросы", {"cmd": "polls", "page": 0, "ideas": 0}),
            color=SECONDARY,
        )
    else:
        keyboard.add(
            Callback(
                "💡 Народные инициативы",
                {"cmd": "polls", "page": 0, "ideas": 1},
            ),
            color=SECONDARY,
        )
    keyboard.add(Callback("💡 Предложить идею", {"cmd": "idea"}), color=POSITIVE)
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_poll_card(
    poll_id: int,
    is_active: bool,
    is_user_idea: bool = False,
    goal_reached: bool = False,
) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    if is_active:
        keyboard.add(
            Callback("✅ За", {"cmd": "vote", "poll_id": poll_id, "value": 1}),
            color=POSITIVE,
        )
        keyboard.add(
            Callback("❌ Против", {"cmd": "vote", "poll_id": poll_id, "value": -1}),
            color=NEGATIVE,
        )
        keyboard.row()
        if not goal_reached:
            keyboard.add(
                Callback(
                    "💰 Внести баллы",
                    {"cmd": "contribute", "id": poll_id},
                ),
                color=POSITIVE,
            )
    back_label = "📋 К народным идеям" if is_user_idea else "📋 К голосованиям"
    keyboard.add(
        Callback(
            back_label,
            {"cmd": "polls", "page": 0, "ideas": int(is_user_idea)},
        ),
        color=SECONDARY,
    )
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_damage_cancel() -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(Callback("❌ Отмена", {"cmd": "menu"}), color=NEGATIVE)
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_admin_panel() -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(Callback("📊 Статистика", {"cmd": "admin_stats"}), color=PRIMARY)
    keyboard.add(Callback("🛠 Репорты", {"cmd": "admin_reports"}), color=SECONDARY)
    keyboard.row()
    keyboard.add(Callback("📦 Инвентарь", {"cmd": "admin_inventory"}), color=SECONDARY)
    keyboard.add(Callback("🗳 Опросы", {"cmd": "admin_polls"}), color=SECONDARY)
    keyboard.row()
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_admin_back() -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(Callback("🛡 В админку", {"cmd": "admin"}), color=PRIMARY)
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_admin_reports(reports: list[DamageReport]) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    for index, report in enumerate(reports):
        if index and index % 2 == 0:
            keyboard.row()
        keyboard.add(
            Callback(
                f"✅ Репорт #{report.id}",
                {"cmd": "admin_resolve", "id": report.id},
            ),
            color=POSITIVE,
        )
    if reports:
        keyboard.row()
    keyboard.add(Callback("🛡 В админку", {"cmd": "admin"}), color=SECONDARY)
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_admin_polls(polls: list[Poll]) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    active = [poll for poll in polls if poll.is_active]
    for index, poll in enumerate(active):
        if index and index % 2 == 0:
            keyboard.row()
        keyboard.add(
            Callback(
                f"🔒 Закрыть #{poll.id}",
                {"cmd": "admin_close_poll", "id": poll.id},
            ),
            color=NEGATIVE,
        )
    if active:
        keyboard.row()
    keyboard.add(Callback("🛡 В админку", {"cmd": "admin"}), color=SECONDARY)
    keyboard.add(Callback("🏠 Меню", {"cmd": "menu"}), color=SECONDARY)
    return keyboard.get_json()


def kb_admin_report_notify(report_id: int) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("✅ Отметить решённым", {"cmd": "admin_resolve", "id": report_id}),
        color=POSITIVE,
    )
    keyboard.add(Callback("🛡 В админку", {"cmd": "admin"}), color=SECONDARY)
    return keyboard.get_json()


def kb_return_notify(booking_id: int) -> str:
    keyboard = Keyboard(one_time=False, inline=True)
    keyboard.add(
        Callback("✅ Вернуть", {"cmd": "return", "id": booking_id}),
        color=POSITIVE,
    )
    keyboard.add(
        Callback("🗓 Мои брони", {"cmd": "my_bookings", "page": 0}),
        color=SECONDARY,
    )
    return keyboard.get_json()


# ============================================================================
# ПОЛЬЗОВАТЕЛИ, БАЛЛЫ И БРОНИРОВАНИЕ
# ============================================================================


async def fetch_vk_profile(
    api: Any,
    vk_id: int,
    message: Message | None = None,
) -> tuple[str, str, str | None] | None:
    """Забирает имя/фамилию/username из VK с кэшем в памяти."""
    cached = NAME_CACHE.get(vk_id)
    if cached is not None:
        return cached

    try:
        if message is not None:
            vk_user = await message.get_user()
        else:
            users = await api.users.get(user_ids=[vk_id])
            vk_user = users[0] if users else None

        if vk_user is None:
            return None

        profile = (
            str(getattr(vk_user, "first_name", "") or ""),
            str(getattr(vk_user, "last_name", "") or ""),
            getattr(vk_user, "username", None),
        )
        NAME_CACHE[vk_id] = profile
        return profile
    except Exception:
        logger.warning("Не удалось получить профиль VK для %s", vk_id, exc_info=True)
        return None


async def ensure_user_vk(
    api: Any,
    vk_id: int,
    message: Message | None = None,
) -> User:
    """Создаёт/обновляет пользователя в БД и возвращает его."""
    profile = await fetch_vk_profile(api, vk_id, message=message)
    first_name, last_name, username = profile or ("", "", None)

    async with SessionLocal() as session:
        user = await session.get(User, vk_id)
        changed = False

        if user is None:
            user = User(
                id=vk_id,
                first_name=(first_name or "Сосед").strip() or "Сосед",
                last_name=(last_name or "").strip(),
                username=username,
                points=STARTING_REPUTATION,
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
    Commit делает вызывающая сторона — так изменение брони и баллов
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
    """
    Бонусная фича «Холодный душ»: 2 поздних возврата/отмены за 30 дней —
    блокировка бронирования на 24 часа.

    Блокировка не мешает вернуть/отменить уже существующую бронь.
    """
    if user.blocked_until is not None and user.blocked_until > now:
        return False

    # add_points() только что добавил запись в session; flush делает её
    # видимой для COUNT в этой же транзакции.
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
    # Дружелюбная подсказка при конфликте.
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
    """
    Ищет ближайшее к desired_start свободное окно нужной длительности.

    Просматриваем 3 дня назад и 14 дней вперёд от желаемого времени.
    Учитываем и брони ресурса, и другие активные брони этого пользователя,
    чтобы не предложить время, которое он сам уже занял.
    """
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
            # При равном расстоянии предпочитаем окно после желаемого времени.
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
    """
    Если на нужном ресурсе свободного окна нет — ищем другой ресурс.
    Выбираем вариант, ближайший к желаемому времени.
    """
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

    Почему так: интервалы [start, end) считаются пересекающимися, если
    существующая бронь началась раньше нашего окончания И закончилась
    позже нашего начала. Соседние брони (end == start) конфликтом не считаются.
    """
    now = now_naive()

    if start < now - timedelta(minutes=1):
        return BookingAttempt(False, "❌ Нельзя бронировать время в прошлом.")
    if start > now + timedelta(days=366):
        return BookingAttempt(False, "❌ Слишком далёкая дата. Выберите время в пределах года.")
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

            # Дополнительная защита: один человек не должен занимать два
            # ресурса в одно и то же время.
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

            # Вот она — проверка пересечения интервалов в БД.
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

                # Дружелюбная альтернатива: сначала ищем ближайшее окно
                # на этом же ресурсе, затем — на любом другом.
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
                deposit_amount=DEPOSIT_POINTS,
                deposit_status="held",
            )
            session.add(booking)
            # flush нужен, чтобы получить booking.id для записи в журнал залога.
            await session.flush()

            # Замораживаем залог: списываем с баланса, но не сжигаем.
            # При своевременном возврате вернём, при просрочке/поломке — нет.
            add_points(
                session,
                user,
                -DEPOSIT_POINTS,
                f"Залог за бронь №{booking.id}",
            )
            # Экономия считается в момент успешного создания брони.
            user.total_saved_rub = (
                int(user.total_saved_rub or 0) + SAVED_RUB_PER_BOOKING
            )
            await session.commit()
            await session.refresh(booking)
            await session.refresh(user)

            return BookingAttempt(
                True,
                "✅ Бронь создана!\n\n"
                f"📦 {resource.emoji} «{resource.name}»\n"
                f"🕒 {fmt_interval(start, end)}\n"
                f"🆔 Бронь №{booking.id}\n\n"
                f"🔒 Залог: {DEPOSIT_POINTS} балла заморожены\n"
                f"⭐ Баланс: {user.points} {points_word(user.points)}\n"
                f"💰 Экономия: +{fmt_money(SAVED_RUB_PER_BOOKING)} ₽\n\n"
                "Верните вещь вовремя — залог вернётся и начислим +1 балл. "
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

        # Экономию, начисленную при создании брони, откатываем:
        # иначе можно было бы «наэкономить» отменёнными бронями.
        # При поздней отмене залог сгорает, при ранней/админской — возвращается.
        deposit_text = ""
        if booking.deposit_status in {"held", "forfeited"}:
            user.total_saved_rub = max(
                0,
                int(user.total_saved_rub or 0) - SAVED_RUB_PER_BOOKING,
            )
        if booking.deposit_status == "held":
            deposit = int(booking.deposit_amount or DEPOSIT_POINTS)
            late_cancel = not admin_cancel_other and delta == LATE_CANCEL_PENALTY
            if late_cancel:
                booking.deposit_status = "forfeited"
                deposit_text = f"🔒 Залог сгорел: {deposit} балла.\n"
            else:
                add_points(
                    session,
                    user,
                    deposit,
                    f"Возврат залога по отменённой брони №{booking.id}",
                )
                booking.deposit_status = "returned"
                deposit_text = f"🔓 Залог возвращён: +{deposit} балла.\n"
            deposit_text += "↩️ Экономия по отменённой брони отменена.\n"

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
            f"{deposit_text}"
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
        deposit = (
            int(booking.deposit_amount or DEPOSIT_POINTS)
            if booking.deposit_status == "held"
            else 0
        )

        blocked_now = False
        if on_time:
            # Залог возвращается + бонус за своевременность.
            if deposit:
                add_points(
                    session,
                    user,
                    deposit,
                    f"Возврат залога по брони №{booking.id}",
                )
                booking.deposit_status = "returned"
            add_points(
                session,
                user,
                ON_TIME_BONUS,
                f"Бонус за возврат брони №{booking.id} вовремя",
            )
            if deposit:
                deposit_line = f"🔓 Залог возвращён: +{deposit} балла\n"
            elif booking.deposit_status == "forfeited":
                deposit_line = "🔥 Залог уже сгорел по репорту о поломке.\n"
            else:
                deposit_line = "ℹ️ По этой брони залог не удерживался.\n"
            head = (
                "✅ Возврат принят вовремя!\n\n"
                f"📦 {resource.emoji} «{resource.name}»\n"
                f"🕒 {fmt_interval(booking.start_time, booking.end_time)}\n"
                f"{deposit_line}"
                f"🎁 Бонус: +{ON_TIME_BONUS} балл\n\n"
            )
        else:
            # Просрочка: залог сгорает, плюс сохраняем штраф за опоздание.
            if booking.deposit_status == "held":
                booking.deposit_status = "forfeited"
            late_minutes = max(1, int((now - booking.end_time).total_seconds() // 60))
            add_points(
                session,
                user,
                LATE_RETURN_PENALTY,
                f"Поздний возврат брони №{booking.id} на {late_minutes} мин.",
            )
            blocked_now = await maybe_block_user(session, user, now)
            if deposit:
                deposit_line = f"🔒 Залог сгорел: {deposit} балла\n"
            elif booking.deposit_status == "forfeited":
                deposit_line = "🔥 Залог уже сгорел по репорту о поломке.\n"
            else:
                deposit_line = "ℹ️ По этой брони залог не удерживался.\n"
            head = (
                "⚠️ Возврат с опозданием.\n\n"
                f"📦 {resource.emoji} «{resource.name}»\n"
                f"🕒 {fmt_interval(booking.start_time, booking.end_time)}\n"
                f"⌛ Опоздание: {late_minutes} мин.\n"
                f"{deposit_line}"
                f"📉 Штраф за опоздание: {LATE_RETURN_PENALTY} балла\n\n"
            )

        booking.status = "completed"
        booking.returned_at = now
        await session.commit()
        await session.refresh(user)

        level_name, icon, _floor, _next = level_info(user.points)

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
        user = await session.get(User, user_id)
        if user is None:
            return False, "❌ Пользователь не найден."
        if int(user.points or 0) <= 0:
            return (
                False,
                "Ваш рейтинг слишком низок для голосования. "
                "Сначала верните долги по бронированиям.",
            )

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


async def contribute_to_poll(
    user_id: int,
    poll_id: int,
    amount: int,
) -> tuple[bool, str, bool, str, int, int, bool]:
    """
    Вносит баллы в копилку опроса.
    Возвращает: (успех, текст, цель_достигнута_сейчас, вопрос,
                 собрано, цель, is_user_idea).
    """
    if amount <= 0:
        return False, "❌ Сумма взноса должна быть положительной.", False, "", 0, 0, False

    async with SessionLocal() as session:
        user = await session.get(User, user_id)
        poll = await session.get(Poll, poll_id)
        if user is None:
            return False, "❌ Пользователь не найден.", False, "", 0, 0, False
        if poll is None:
            return False, "❌ Опрос не найден.", False, "", 0, 0, False
        if not poll.is_active:
            return False, "🔒 Это голосование уже завершено.", False, poll.question, poll.collected_points, poll.goal_points, poll.is_user_idea

        remaining = max(0, int(poll.goal_points or 0) - int(poll.collected_points or 0))
        if remaining <= 0:
            return False, "🎉 Цель уже достигнута — спасибо соседям!", False, poll.question, poll.collected_points, poll.goal_points, poll.is_user_idea

        accepted = min(int(amount), remaining)
        if int(user.points or 0) < accepted:
            return (
                False,
                "❌ Недостаточно баллов.\n"
                f"Нужно: {accepted}, у вас: {int(user.points or 0)}.",
                False,
                poll.question,
                poll.collected_points,
                poll.goal_points,
                poll.is_user_idea,
            )

        add_points(
            session,
            user,
            -accepted,
            f"Взнос в голосование №{poll.id}",
        )
        poll.collected_points = int(poll.collected_points or 0) + accepted
        reached_now = (
            int(poll.collected_points) >= int(poll.goal_points)
            and not poll.goal_notified
        )
        if reached_now:
            poll.goal_notified = True

        await session.commit()
        await session.refresh(user)
        await session.refresh(poll)

        progress = progress_bar(
            int(poll.collected_points),
            int(poll.goal_points),
        )
        text = (
            f"💰 Взнос принят: {accepted} баллов.\n\n"
            f"💡 {poll.question}\n"
            f"Копилка: {fmt_money(int(poll.collected_points))} / "
            f"{fmt_money(int(poll.goal_points))} баллов\n"
            f"{progress}\n"
            f"⭐ Ваш баланс: {user.points} {points_word(user.points)}"
        )
        if reached_now:
            text += (
                "\n\n🎉 Цель достигнута! Администратор уже получил уведомление."
            )
        return (
            True,
            text,
            reached_now,
            poll.question,
            int(poll.collected_points),
            int(poll.goal_points),
            poll.is_user_idea,
        )


async def notify_crowdfunding_goal(
    poll_id: int,
    question: str,
    collected: int,
    goal: int,
) -> None:
    await send_to_peer(
        ADMIN_ID,
        "🎉 Ура! Мы собрали на «"
        f"{question}»!\n\n"
        f"💰 Собрано: {fmt_money(collected)} / {fmt_money(goal)} баллов.\n"
        "Пора переходить к закупке и радовать соседей.",
        keyboard=kb_admin_idea_open(poll_id),
    )


# ============================================================================
# РЕПОРТЫ О ПОЛОМКАХ
# ============================================================================


async def create_damage_report(
    user_id: int,
    description: str,
    photo_attachment: str | None,
    photo_url: str | None,
) -> tuple[DamageReport, str, int]:
    """
    Сохраняет репорт. Если у пользователя есть активная бронь с замороженным
    залогом — привязываем репорт к ней и сжигаем залог.
    """
    async with SessionLocal() as session:
        user = await session.get(User, user_id)

        active_booking = (
            await session.execute(
                select(Booking)
                .where(
                    Booking.user_id == user_id,
                    Booking.status == "active",
                    Booking.deposit_status == "held",
                )
                .order_by(desc(Booking.start_time))
                .limit(1)
            )
        ).scalar_one_or_none()

        forfeited_deposit = 0
        booking_id: int | None = None
        if active_booking is not None:
            forfeited_deposit = int(
                active_booking.deposit_amount or DEPOSIT_POINTS
            )
            booking_id = active_booking.id
            active_booking.deposit_status = "forfeited"

        report = DamageReport(
            user_id=user_id,
            description=description,
            photo_attachment=photo_attachment,
            photo_url=photo_url,
            booking_id=booking_id,
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
        if booking_id is not None:
            admin_text += (
                f"\n\n🔒 Залог {forfeited_deposit} балла сгорел "
                f"по брони №{booking_id}."
            )
        if photo_url:
            admin_text += f"\n\n🔗 Фото: {photo_url}"
        return report, admin_text, forfeited_deposit


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
            goal_points=50000,
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
    """Бонусная админ-команда: снять 24-часовую блокировку с соседа."""
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
    """
    Публикует народную инициативу сразу (is_active=True).
    Для защиты от спама — не больше MAX_IDEAS_PER_DAY идей за 24 часа.
    """
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
                "Дайте соседям время проголосовать и попробуйте завтра.",
                None,
                question,
            )

        poll = Poll(
            question=question,
            description=description,
            budget_rub=0,
            goal_points=50000,
            is_active=True,
            is_user_idea=True,
            idea_notified=False,
            created_by=user_id,
        )
        session.add(poll)
        await session.commit()
        await session.refresh(poll)
        return True, "✅ Идея опубликована!", poll.id, question


async def check_idea_threshold(
    poll_id: int,
) -> tuple[str, int, int | None, int] | None:
    """
    Если народная инициатива набрала больше 5 голосов «За»
    (IDEA_VOTES_THRESHOLD = 6), один раз помечаем это и возвращаем данные
    для уведомления админа.
    """
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


async def notify_admin_new_idea(poll_id: int, question: str, author_id: int) -> None:
    """Сообщает админу о новой народной инициативе."""
    async with SessionLocal() as session:
        author = await session.get(User, author_id)

    await send_to_peer(
        ADMIN_ID,
        "💡 Новая народная инициатива\n\n"
        f"👤 Автор: {display_name(author, author_id)}\n"
        f"❓ {question}\n\n"
        "Идея уже опубликована в разделе «Народные инициативы». "
        "Можно следить за голосами или закрыть опрос.",
        keyboard=kb_admin_idea_open(poll_id),
    )


async def notify_idea_threshold(
    question: str,
    votes_for: int,
    author_id: int | None,
    poll_id: int,
) -> None:
    """Уведомляет админа и автора, когда идея набрала больше 5 голосов «За»."""
    await send_to_peer(
        ADMIN_ID,
        "🚀 Ого, народная инициатива набрала больше 5 голосов, "
        "пора действовать!\n\n"
        f"💡 {question}\n"
        f"✅ Голосов «За»: {votes_for}\n\n"
        "Откройте инициативу и решите, что делать дальше.",
        keyboard=kb_admin_idea_open(poll_id),
    )

    if author_id is not None:
        await send_to_peer(
            author_id,
            "🎉 Ваша народная инициатива набрала "
            f"{votes_for} голосов «За»!\n\n"
            f"💡 {question}\n\n"
            "Администратор уже получил сигнал «пора действовать».",
        )


async def idea_quota_left(user_id: int) -> int:
    """Сколько идей пользователь ещё может предложить за сутки."""
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


async def publish_user_idea_message(
    message: Message,
    user_id: int,
    question: str,
    description: str,
) -> bool:
    """Создаёт идею, уведомляет админа и отвечает автору."""
    ok, result_text, poll_id, clean_question = await create_user_idea(
        user_id,
        question,
        description,
    )
    if not ok or poll_id is None:
        await message.answer(result_text, keyboard=kb_damage_cancel())
        return False

    await clear_state(user_id)
    await notify_admin_new_idea(poll_id, clean_question, user_id)
    await message.answer(
        "✅ Идея опубликована!\n\n"
        f"💡 {clean_question}\n\n"
        "Она уже в разделе «💡 Народные инициативы». "
        "Когда наберётся больше 5 голосов «За», администратор получит "
        "сигнал «пора действовать».",
        keyboard=kb_idea_done(poll_id),
    )
    return True


# ============================================================================
# СБОРЩИКИ ЭКРАНОВ
# ============================================================================


async def build_main_menu(vk_id: int) -> tuple[str, str]:
    async with SessionLocal() as session:
        user = await session.get(User, vk_id)
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
        name = f"Сосед #{vk_id}"
    else:
        points = int(user.points or 0)
        saved_rub = int(user.total_saved_rub or 0)
        name = display_name(user)

    level_name, icon, _floor, _next = level_info(points)

    text = (
        f"🏡 «Соседский ресурс»\n"
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
    return text, kb_main(vk_id == ADMIN_ID)


def build_help(is_admin: bool = False) -> tuple[str, str]:
    text = (
        "ℹ️ Как всё устроено\n\n"
        "📦 Бронирование\n"
        "Выберите ресурс, введите начало в формате:\n"
        "25.10.2026 18:00\n\n"
        "Можно сразу указать интервал:\n"
        "25.10.2026 18:00 - 25.10.2026 20:00\n\n"
        "Бот проверит пересечение с другими бронями и назовёт того, "
        "кто уже занял слот.\n\n"
        "⭐ Репутация и залог\n"
        f"• {DEPOSIT_POINTS} балла замораживаются при создании брони;\n"
        "• вернули вовремя — залог возвращается и +1 балл;\n"
        "• опоздали или сообщили о поломке — залог сгорает;\n"
        "• −5 баллов — отмена больше чем за час до начала;\n"
        "• −3 балла — отмена меньше чем за час;\n"
        "• −2 балла — дополнительный штраф за поздний возврат.\n\n"
        f"Стартовый капитал репутации: {STARTING_REPUTATION} баллов.\n"
        "Уровни считаются от него:\n"
        f"• {STARTING_REPUTATION}–{STARTING_REPUTATION + 9} — 🌱 Новичок;\n"
        f"• {STARTING_REPUTATION + 10}–{STARTING_REPUTATION + 29} — 🤝 Сосед;\n"
        f"• {STARTING_REPUTATION + 30}+ — 👑 Староста.\n\n"
        "💰 Экономия\n"
        "Каждая успешная бронь = 500 ₽ условной экономии: "
        "вещь не пришлось покупать самому. Сумма копится в профиле. "
        "При отмене брони начисленная экономия откатывается.\n\n"
        "🛠 Поломка\n"
        "Нажмите кнопку, отправьте фото и описание. Репорт сразу уйдёт "
        "администратору и сохранится в БД.\n\n"
        "🗳 Голосования и сборы\n"
        "Голосовать могут только соседи с балансом репутации > 0. "
        "Один человек — один голос, мнение можно поменять до закрытия.\n"
        "Внутри опроса можно нажать «💰 Внести баллы» и поддержать "
        "покупку. Баллы списываются с баланса и попадают в копилку.\n\n"
        "💡 Народные инициативы\n"
        "Нажмите «Предложить идею» и отправьте вопрос + описание. "
        "Идея сразу появится в разделе «Народные инициативы». "
        "После 5 голосов «За» администратор получит сигнал.\n"
        "Можно и командой: /idea Вопрос | Описание\n\n"
        "🚫 Блокировка\n"
        "Два поздних возврата или отмены за 30 дней — бронирование "
        "недоступно 24 часа. Администратор может снять блокировку.\n\n"
        "⏰ Напоминание\n"
        "За 15 минут до конца брони бот напишет в личные сообщения."
    )
    return text, kb_main(is_admin)


async def build_resources_page(page: int = 0) -> tuple[str, str]:
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


async def build_resource_card(resource_id: int) -> tuple[str, str]:
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


async def build_resource_slots(resource_id: int) -> tuple[str, str]:
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


async def build_profile(vk_id: int) -> tuple[str, str]:
    async with SessionLocal() as session:
        user = await session.get(User, vk_id)
        if user is None:
            return "❌ Профиль не найден. Нажмите /start.", kb_main(vk_id == ADMIN_ID)

        active_count = (
            await session.execute(
                select(func.count())
                .select_from(Booking)
                .where(Booking.user_id == vk_id, Booking.status == "active")
            )
        ).scalar_one()
        completed = (
            (
                await session.execute(
                    select(Booking)
                    .where(Booking.user_id == vk_id, Booking.status == "completed")
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
                .where(Booking.user_id == vk_id, Booking.status == "cancelled")
            )
        ).scalar_one()
        logs = (
            (
                await session.execute(
                    select(ReputationLog)
                    .where(ReputationLog.user_id == vk_id)
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
        and booking.returned_at <= booking.end_time + timedelta(minutes=RETURN_GRACE_MINUTES)
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

    score = reputation_score(points)
    level_name, icon, floor, next_threshold = level_info(points)
    if next_threshold is not None:
        current = max(score, floor)
        progress = progress_bar(current - floor, next_threshold - floor)
        until_next = max(0, next_threshold - score)
        next_line = (
            f"До уровня выше: {until_next} {points_word(until_next)}\n"
            f"{progress}"
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
        f"⭐ Моя репутация\n\n"
        f"👤 {display_name(user)}\n"
        f"Баллы: {points} {points_word(points)}\n"
        f"Уровень: {icon} {level_name}\n"
        f"💰 Сэкономлено денег: {fmt_money(saved_rub)} ₽\n"
        f"{block_line}"
        f"{next_line}\n\n"
        f"📊 Статистика\n"
        f"• Активных броней: {active_count}\n"
        f"• Завершено: {len(completed)}\n"
        f"• Возвратов вовремя: {on_time}\n"
        f"• С опозданием: {late}\n"
        f"• Отменено: {cancelled_count}\n\n"
        f"🕘 Последние изменения:\n{logs_text}"
    )
    return text, kb_profile()


async def build_my_bookings(vk_id: int, page: int = 0) -> tuple[str, str]:
    page_size = 3
    async with SessionLocal() as session:
        active = (
            (
                await session.execute(
                    select(Booking, Resource)
                    .join(Resource, Resource.id == Booking.resource_id)
                    .where(Booking.user_id == vk_id, Booking.status == "active")
                    .order_by(Booking.start_time)
                )
            )
            .all()
        )
        completed_count = (
            await session.execute(
                select(func.count())
                .select_from(Booking)
                .where(Booking.user_id == vk_id, Booking.status == "completed")
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
            kb_main(vk_id == ADMIN_ID),
        )

    lines = [f"🗓 Мои активные брони · {page + 1}/{total_pages}", ""]
    for booking, resource in page_items:
        deposit_label = ""
        if booking.deposit_status == "held":
            deposit_label = (
                f" · 🔒 залог {int(booking.deposit_amount or DEPOSIT_POINTS)}"
            )
        elif booking.deposit_status == "forfeited":
            deposit_label = " · 🔥 залог сгорел"
        lines.extend(
            [
                f"📦 «{resource.name}»",
                f"🕒 {fmt_interval(booking.start_time, booking.end_time)}",
                f"Статус: {booking_status_label(booking)} · №{booking.id}"
                f"{deposit_label}",
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
    vk_id: int,
    page: int = 0,
    ideas_only: bool = False,
) -> tuple[str, str]:
    page_size = 4
    async with SessionLocal() as session:
        # В общем списке показываем и опросы администрации, и народные
        # инициативы. Отдельный режим ideas_only — только идеи.
        condition = [Poll.is_active.is_(True)]
        if ideas_only:
            condition.append(Poll.is_user_idea.is_(True))

        total = (
            await session.execute(
                select(func.count())
                .select_from(Poll)
                .where(*condition)
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
                    "Станьте первым: нажмите «💡 Предложить идею» и расскажите, "
                    "что нужно изменить во дворе.",
                    kb_polls_list([], page, total_pages, ideas_only=True),
                )
            return (
                "🗳 Активных голосований пока нет.\n"
                "Администратор может создать опрос командой /add_poll, "
                "а вы — предложить народную идею.",
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
            collected = (
                f" · копилка {fmt_money(int(poll.collected_points or 0))}/"
                f"{fmt_money(int(poll.goal_points or 0))}"
                if poll.goal_points
                else ""
            )
            lines.append(
                f"{index}. {prefix} {poll.question}\n"
                f"   Голосов: {votes_count}{budget}{collected}"
            )

    if ideas_only:
        lines.extend(
            [
                "",
                "Это идеи соседей. Голосуйте «За»: после 5 голосов "
                "администратор получит сигнал «пора действовать».",
            ]
        )
    else:
        lines.extend(["", "Нажмите на опрос, чтобы проголосовать «За» или «Против»."])
    return "\n".join(lines), kb_polls_list(
        list(polls), page, total_pages, ideas_only
    )


def poll_result_lines(count_for: int, count_against: int, user_value: int | None) -> str:
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


async def build_poll_view(vk_id: int, poll_id: int) -> tuple[str, str]:
    async with SessionLocal() as session:
        poll = await session.get(Poll, poll_id)
        if poll is None:
            return "❌ Голосование не найдено.", kb_main(vk_id == ADMIN_ID)

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

        voter = await session.get(User, vk_id)
        can_vote = voter is not None and int(voter.points or 0) > 0

        own_vote = (
            await session.execute(
                select(PollVote)
                .where(PollVote.poll_id == poll_id, PollVote.user_id == vk_id)
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
    access_line = ""
    if not can_vote:
        access_line = (
            "🚫 Голосование недоступно: баланс репутации 0 или ниже.\n"
            "Сначала верните долги по бронированиям.\n\n"
        )

    goal_reached = int(poll.collected_points or 0) >= int(poll.goal_points or 0)
    crowdfunding_line = ""
    if poll.goal_points:
        crowdfunding_line = (
            "💰 Копилка: "
            f"{fmt_money(int(poll.collected_points or 0))} / "
            f"{fmt_money(int(poll.goal_points))} баллов\n"
            f"{progress_bar(int(poll.collected_points or 0), int(poll.goal_points))}\n"
        )
        if goal_reached:
            crowdfunding_line += "🎉 Цель достигнута!\n"

    text = (
        f"{header}: {poll.question}\n\n"
        f"{poll.description or 'Описание не добавлено.'}\n\n"
        f"{author_line}"
        f"{budget}\n"
        f"{crowdfunding_line}"
        f"{access_line}"
        f"{status}\n\n"
        f"{poll_result_lines(counts[1], counts[-1], own_value)}"
    )
    return text, kb_poll_card(
        poll.id,
        poll.is_active,
        poll.is_user_idea,
        goal_reached=goal_reached,
    )


async def build_admin_panel() -> tuple[str, str]:
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
        ideas_active = (
            await session.execute(
                select(func.count())
                .select_from(Poll)
                .where(
                    Poll.is_active.is_(True),
                    Poll.is_user_idea.is_(True),
                )
            )
        ).scalar_one()
        total_saved = (
            await session.execute(
                select(func.coalesce(func.sum(User.total_saved_rub), 0))
            )
        ).scalar_one()
        total_collected = (
            await session.execute(
                select(func.coalesce(func.sum(Poll.collected_points), 0))
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
        f"🗳 Активных опросов: {active_polls} "
        f"(💡 идей: {ideas_active})\n"
        f"💰 Сэкономлено денег: {fmt_money(int(total_saved))} ₽\n"
        f"🏦 Собрано баллов на инициативы: {fmt_money(int(total_collected))}\n"
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


async def build_admin_stats() -> tuple[str, str]:
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
        ideas_active = (
            await session.execute(
                select(func.count())
                .select_from(Poll)
                .where(
                    Poll.is_active.is_(True),
                    Poll.is_user_idea.is_(True),
                )
            )
        ).scalar_one()
        total_saved = (
            await session.execute(
                select(func.coalesce(func.sum(User.total_saved_rub), 0))
            )
        ).scalar_one()
        total_collected = (
            await session.execute(
                select(func.coalesce(func.sum(Poll.collected_points), 0))
            )
        ).scalar_one()
        blocked_count = (
            await session.execute(
                select(func.count())
                .select_from(User)
                .where(User.blocked_until.is_not(None), User.blocked_until > now_naive())
            )
        ).scalar_one()

        now = now_naive()
        week_start = now - timedelta(days=7)
        resources_for_util = (
            (
                await session.execute(
                    select(Resource)
                    .where(Resource.is_active.is_(True))
                    .order_by(Resource.id)
                    .limit(15)
                )
            )
            .scalars()
            .all()
        )
        bookings_for_util = (
            (
                await session.execute(
                    select(Booking).where(
                        Booking.status.in_(("active", "completed")),
                        Booking.start_time < now,
                        Booking.end_time > week_start,
                    )
                )
            )
            .scalars()
            .all()
        )
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

    top_lines = "\n".join(
        f"{index}. {display_name(user)} — {user.points} {points_word(user.points)}"
        for index, user in enumerate(top_users, start=1)
    ) or "Пока нет данных."
    saver_lines = "\n".join(
        f"{index}. {display_name(user)} — {fmt_money(user.total_saved_rub)} ₽"
        for index, user in enumerate(top_savers, start=1)
    ) or "Пока нет данных."

    # Утилизация: сколько часов ресурс был занят за последние 7 дней.
    occupied: dict[int, float] = {resource.id: 0.0 for resource in resources_for_util}
    for booking in bookings_for_util:
        overlap_start = max(booking.start_time, week_start)
        overlap_end = min(booking.end_time, now)
        if overlap_end > overlap_start:
            occupied[booking.resource_id] = occupied.get(
                booking.resource_id, 0.0
            ) + (overlap_end - overlap_start).total_seconds() / 3600
    utilization_lines = []
    for resource in resources_for_util:
        hours = occupied.get(resource.id, 0.0)
        percent = min(100, round(hours / (7 * 24) * 100))
        utilization_lines.append(
            f"{resource.emoji} {resource.name} — загружен на {percent}% "
            f"({hours:.1f} ч)"
        )
    utilization_text = (
        "\n".join(utilization_lines) if utilization_lines else "Пока нет данных."
    )

    text = (
        "📊 Статистика «Соседского ресурса»\n\n"
        f"👥 Пользователей: {users_count}\n"
        f"📅 Броней всего: {bookings_total}\n"
        f"🟢 Активных броней: {active_bookings}\n"
        f"🛠 Открытых репортов: {reports_open}\n"
        f"🗳 Активных опросов: {polls_active} (💡 идей: {ideas_active})\n\n"
        f"💰 Сэкономлено денег: {fmt_money(int(total_saved))} ₽\n"
        f"🏦 Собрано баллов: {fmt_money(int(total_collected))}\n"
        f"🚫 Заблокировано сейчас: {blocked_count}\n\n"
        f"📈 Утилизация ресурсов за 7 дней:\n{utilization_text}\n\n"
        f"🏆 Топ соседей:\n{top_lines}\n\n"
        f"💰 Топ по экономии:\n{saver_lines}"
    )
    return text, kb_admin_back()


async def build_admin_reports() -> tuple[str, str]:
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


async def build_admin_inventory() -> tuple[str, str]:
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


async def build_admin_polls() -> tuple[str, str]:
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
        crowdfunding = (
            f" · копилка {fmt_money(int(poll.collected_points or 0))}/"
            f"{fmt_money(int(poll.goal_points or 0))}"
            if poll.goal_points
            else ""
        )
        lines.append(
            f"#{poll.id} · {kind} · {status}{crowdfunding} · {poll.question}"
        )

    lines.extend(
        [
            "",
            "Создать: /add_poll Вопрос | Описание | 18000",
            "Закрыть: /close_poll ID",
        ]
    )
    return "\n".join(lines), kb_admin_polls(polls)


# ============================================================================
# ОТПРАВКА СООБЩЕНИЙ
# ============================================================================


async def send_to_peer(
    peer_id: int,
    text: str,
    keyboard: str | None = None,
    attachment: str | None = None,
) -> bool:
    """Надёжная отправка в произвольный peer (ЛС, беседа, админ)."""
    payload: dict[str, Any] = {
        "peer_id": peer_id,
        "message": text,
        "random_id": new_random_id(),
    }
    if keyboard:
        payload["keyboard"] = keyboard
    if attachment:
        payload["attachment"] = attachment

    try:
        await bot.api.messages.send(**payload)
        return True
    except Exception:
        logger.exception("Не удалось отправить сообщение peer_id=%s", peer_id)
        return False


async def edit_or_send(event: MessageEvent, text: str, keyboard: str | None = None) -> None:
    """Пытается отредактировать сообщение с кнопкой; иначе отправляет новое."""
    try:
        await event.edit_message(message=text, keyboard=keyboard)
    except Exception:
        logger.debug("Не удалось отредактировать сообщение, отправляю новое", exc_info=True)
        await send_to_peer(event.peer_id, text, keyboard=keyboard)


async def ack_event(event: MessageEvent) -> None:
    """Быстро гасим «крутилку» на callback-кнопке."""
    try:
        await event.send_empty_answer()
    except Exception:
        logger.debug("send_empty_answer не сработал, пробую snackbar", exc_info=True)
        with suppress(Exception):
            await event.show_snackbar("Готово")


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
    if step == "idea_question":
        return "💡 Напишите вопрос народной инициативы (5–200 символов)."
    if step == "idea_description":
        return "📝 Добавьте описание идеи (5–1000 символов)."
    if step == "contribute_poll":
        return "💰 Введите сумму взноса в баллах, например: 500."
    return "Продолжим? Нажмите /start."


# ============================================================================
# СЦЕНАРИИ ВВОДА
# ============================================================================


async def handle_booking_start_input(
    message: Message,
    user_id: int,
    state: dict[str, Any],
    text: str,
) -> None:
    resource_id = int(state.get("resource_id", 0))
    try:
        values = parse_datetimes(text)
    except ValueError:
        await message.answer("❌ Неверная дата. Попробуйте ещё раз.")
        return

    if not values:
        await message.answer(
            "❌ Не вижу дату. Нужен формат ДД.ММ.ГГГГ ЧЧ:ММ.\n"
            "Например: 25.10.2026 18:00",
            keyboard=kb_booking_retry(resource_id),
        )
        return

    if len(values) > 2:
        await message.answer(
            "❌ Слишком много дат. Укажите начало и, при желании, окончание.",
            keyboard=kb_booking_retry(resource_id),
        )
        return

    start = values[0]
    now = now_naive()

    if start < now - timedelta(minutes=1):
        await message.answer(
            "❌ Это время уже прошло. Введите будущую дату.",
            keyboard=kb_booking_retry(resource_id),
        )
        return
    if start > now + timedelta(days=366):
        await message.answer(
            "❌ Слишком далёкая дата — не больше чем на год вперёд.",
            keyboard=kb_booking_retry(resource_id),
        )
        return

    if len(values) == 2:
        attempt = await try_book(user_id, resource_id, start, values[1])
        if attempt.ok:
            await clear_state(user_id)
            await message.answer(attempt.text, keyboard=kb_booking_done())
        else:
            if attempt.blocked:
                await clear_state(user_id)
            else:
                await set_state(
                    user_id,
                    {
                        "step": "booking_start",
                        "resource_id": resource_id,
                    },
                )
            failure_keyboard = (
                kb_main(user_id == ADMIN_ID)
                if attempt.blocked
                else kb_booking_conflict(attempt, resource_id)
            )
            suffix = "" if attempt.blocked else "\n\nВведите новое время начала."
            await message.answer(
                attempt.text + suffix,
                keyboard=failure_keyboard,
            )
        return

    # Пришло только начало — предлагаем длительность.
    await set_state(
        user_id,
        {
            "step": "booking_duration",
            "resource_id": resource_id,
            "start": start,
        },
    )

    async with SessionLocal() as session:
        resource = await session.get(Resource, resource_id)
    resource_name = resource.name if resource else "ресурс"

    await message.answer(
        f"✅ Начало: {fmt_dt(start)}\n\n"
        f"На сколько бронируем «{resource_name}»?\n"
        "Выберите кнопкой ниже или отправьте время окончания сообщением.",
        keyboard=kb_duration(resource_id),
    )


async def handle_booking_end_input(
    message: Message,
    user_id: int,
    state: dict[str, Any],
    text: str,
    from_duration_step: bool = False,
) -> None:
    resource_id = int(state.get("resource_id", 0))
    start = state.get("start")
    if not isinstance(start, datetime):
        await clear_state(user_id)
        await message.answer(
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
            await message.answer(
                "⏳ Выберите длительность кнопкой ниже или отправьте время "
                "окончания в формате 25.10.2026 20:00.",
                keyboard=kb_duration(resource_id),
            )
        else:
            await message.answer(
                "❌ Не вижу время окончания. Формат: 25.10.2026 20:00",
                keyboard=kb_booking_retry(resource_id),
            )
        return

    end = values[-1]
    attempt = await try_book(user_id, resource_id, start, end)
    if attempt.ok:
        await clear_state(user_id)
        await message.answer(attempt.text, keyboard=kb_booking_done())
    else:
        if attempt.blocked:
            await clear_state(user_id)
        else:
            await set_state(
                user_id,
                {
                    "step": "booking_start",
                    "resource_id": resource_id,
                },
            )
        failure_keyboard = (
            kb_main(user_id == ADMIN_ID)
            if attempt.blocked
            else kb_booking_conflict(attempt, resource_id)
        )
        suffix = "" if attempt.blocked else "\n\nВведите новое время начала."
        await message.answer(
            attempt.text + suffix,
            keyboard=failure_keyboard,
        )


async def finalize_damage_report(
    message: Message,
    user_id: int,
    description: str,
    photo_attachment: str | None,
    photo_url: str | None,
) -> None:
    description = description.strip()
    if not (5 <= len(description) <= 800):
        await message.answer(
            "❌ Описание должно быть от 5 до 800 символов. "
            "Опишите, что именно сломалось.",
            keyboard=kb_damage_cancel(),
        )
        return

    report, admin_text, forfeited_deposit = await create_damage_report(
        user_id,
        description,
        photo_attachment,
        photo_url,
    )

    admin_sent = await send_to_peer(
        ADMIN_ID,
        admin_text,
        keyboard=kb_admin_report_notify(report.id),
        attachment=report.photo_attachment,
    )
    if not admin_sent and report.photo_attachment:
        # Иногда VK не даёт переиспользовать вложение из чужого диалога.
        # В таком случае отправляем хотя бы текст и прямую ссылку на фото.
        admin_sent = await send_to_peer(
            ADMIN_ID,
            admin_text,
            keyboard=kb_admin_report_notify(report.id),
        )

    await clear_state(user_id)

    if admin_sent:
        tail = "Администратор уже получил фото и описание."
    else:
        tail = (
            "Репорт сохранён в базе. Администратор увидит его в панели "
            "«🛠 Репорты»."
        )

    deposit_tail = ""
    if forfeited_deposit and report.booking_id is not None:
        deposit_tail = (
            "\n🔒 Залог "
            f"{forfeited_deposit} балла сгорел по активной брони "
            f"№{report.booking_id}."
        )

    await message.answer(
        "✅ Спасибо! Репорт о поломке сохранён.\n\n"
        f"🆔 Номер: #{report.id}\n"
        f"📝 {shorten(description, 200)}\n"
        f"{deposit_tail}\n\n"
        f"{tail}",
        keyboard=kb_main(user_id == ADMIN_ID),
    )


async def handle_damage_photo(
    message: Message,
    user_id: int,
    photo_attachment: str | None,
    photo_url: str | None,
) -> None:
    caption = (message.text or "").strip()
    if 5 <= len(caption) <= 800:
        await finalize_damage_report(
            message,
            user_id,
            caption,
            photo_attachment,
            photo_url,
        )
        return

    await set_state(
        user_id,
        {
            "step": "damage_desc",
            "photo_attachment": photo_attachment,
            "photo_url": photo_url,
        },
    )
    await message.answer(
        "📷 Фото получил!\n\n"
        "Теперь напишите текстом, что случилось. Например: "
        "«Сломалась ручка у шуруповёрта, трещит при работе».",
        keyboard=kb_damage_cancel(),
    )


# ============================================================================
# ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ
# ============================================================================


async def route_message(message: Message) -> None:
    if message.from_id <= 0:
        return

    user_id = message.from_id
    await ensure_user_vk(message.ctx_api, user_id, message=message)

    text = (message.text or "").strip()
    lower = text.lower()
    command_parts = text.split(maxsplit=1)
    command = command_parts[0].lower() if command_parts else ""
    args = command_parts[1] if len(command_parts) > 1 else ""

    # VK-кнопка «Начать» может присылать payload вместо текста.
    payload = None
    with suppress(Exception):
        payload = message.get_payload_json()
    is_start_payload = isinstance(payload, dict) and payload.get("command") == "start"

    # --- Общие команды -----------------------------------------------------
    if lower in {"/start", "start", "начать", "/menu", "меню", "menu"} or is_start_payload:
        await clear_state(user_id)
        menu_text, menu_kb = await build_main_menu(user_id)
        await message.answer(menu_text, keyboard=menu_kb)
        return

    if lower in {"/help", "помощь", "help"}:
        help_text, help_kb = build_help(is_admin=(user_id == ADMIN_ID))
        await message.answer(help_text, keyboard=help_kb)
        return

    if lower in {"/cancel", "отмена", "cancel"}:
        await clear_state(user_id)
        menu_text, menu_kb = await build_main_menu(user_id)
        await message.answer("Ок, отменил. Возвращаемся в меню.\n\n" + menu_text, keyboard=menu_kb)
        return

    # --- Админские команды -------------------------------------------------
    is_admin = user_id == ADMIN_ID

    if command == "/add_resource":
        if not is_admin:
            await message.answer("🤔 Неизвестная команда. Нажмите /start.")
            return
        if not args.strip():
            await message.answer(
                "Формат:\n/add_resource Название | Описание | 120\n\n"
                "Последнее число — стандартная длительность в минутах (необязательно).",
                keyboard=kb_admin_back(),
            )
            return

        parts = [part.strip() for part in args.split("|")]
        if len(parts) < 2 or not parts[0] or not parts[1]:
            await message.answer(
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

        ok, result_text, _resource_id = await admin_add_resource(
            name, description, duration
        )
        await message.answer(result_text, keyboard=kb_admin_back())
        return

    if command == "/add_poll":
        if not is_admin:
            await message.answer("🤔 Неизвестная команда. Нажмите /start.")
            return
        if not args.strip():
            await message.answer(
                "Формат:\n/add_poll Вопрос | Описание | 18000\n\n"
                "Последнее число — ориентир бюджета (необязательно).",
                keyboard=kb_admin_back(),
            )
            return

        parts = [part.strip() for part in args.split("|")]
        if len(parts) < 2 or not parts[0]:
            await message.answer(
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
        await message.answer(result_text, keyboard=kb_admin_back())
        return

    if command == "/close_poll":
        if not is_admin:
            await message.answer("🤔 Неизвестная команда. Нажмите /start.")
            return
        poll_id = as_int(args.strip())
        if poll_id is None:
            await message.answer("Формат: /close_poll ID", keyboard=kb_admin_back())
            return
        _ok, result_text = await admin_close_poll(poll_id)
        await message.answer(result_text, keyboard=kb_admin_back())
        return

    if command in {"/admin", "/admin_panel"}:
        if not is_admin:
            await message.answer("🤔 Неизвестная команда. Нажмите /start.")
            return
        panel_text, panel_kb = await build_admin_panel()
        await message.answer(panel_text, keyboard=panel_kb)
        return

    if command == "/stats":
        if not is_admin:
            await message.answer("🤔 Неизвестная команда. Нажмите /start.")
            return
        stats_text, stats_kb = await build_admin_stats()
        await message.answer(stats_text, keyboard=stats_kb)
        return

    if command == "/reports":
        if not is_admin:
            await message.answer("🤔 Неизвестная команда. Нажмите /start.")
            return
        reports_text, reports_kb = await build_admin_reports()
        await message.answer(reports_text, keyboard=reports_kb)
        return

    if command == "/unblock":
        if not is_admin:
            await message.answer("🤔 Неизвестная команда. Нажмите /start.")
            return
        target_id = as_int(args.strip())
        if target_id is None:
            await message.answer(
                "Формат: /unblock USER_ID",
                keyboard=kb_admin_back(),
            )
            return
        _ok, result_text = await admin_unblock_user(target_id)
        await message.answer(result_text, keyboard=kb_admin_back())
        return

    if command == "/idea":
        if args.strip():
            if "|" not in args:
                await message.answer(
                    "Формат: /idea Вопрос | Описание\n"
                    "Или нажмите «💡 Предложить идею» и введите данные по шагам.",
                    keyboard=kb_main(is_admin),
                )
                return
            question, _, description = args.partition("|")
            await publish_user_idea_message(
                message,
                user_id,
                question.strip(),
                description.strip(),
            )
            return

        if await idea_quota_left(user_id) <= 0:
            await message.answer(
                f"❌ Вы уже предложили {MAX_IDEAS_PER_DAY} идеи за сутки. "
                "Попробуйте завтра.",
                keyboard=kb_main(is_admin),
            )
            return
        await set_state(user_id, {"step": "idea_question"})
        await message.answer(
            "💡 Народная инициатива\n\n"
            "Шаг 1 из 2. Напишите вопрос, который стоит решить всем двором.\n\n"
            "Например: «Поставить шлагбаум во дворе?»",
            keyboard=kb_damage_cancel(),
        )
        return

    if lower.startswith("/"):
        await message.answer(
            "🤔 Неизвестная команда. Нажмите /start, чтобы открыть меню."
        )
        return

    # --- Фото и состояния --------------------------------------------------
    photo_attachment, photo_url = extract_photo_info(message)
    has_photo = bool(photo_attachment or photo_url)
    state = await get_state(user_id)

    if has_photo:
        if state and str(state.get("step", "")).startswith("booking"):
            await message.answer(
                "📅 Сейчас я жду дату и время, а не фото. "
                "Введите, например: 25.10.2026 18:00",
                keyboard=kb_booking_retry(int(state.get("resource_id", 0))),
            )
            return

        if state and state.get("step") in {"idea_question", "idea_description"}:
            await message.answer(
                "💡 Для народной инициативы нужен текст, а не фото.\n"
                "Напишите вопрос или описание сообщением.",
                keyboard=kb_damage_cancel(),
            )
            return

        if state and state.get("step") == "contribute_poll":
            await message.answer(
                "💰 Для взноса нужно число баллов, а не фото.\n"
                "Например: 500",
                keyboard=kb_damage_cancel(),
            )
            return

        if state and state.get("step") == "damage_desc":
            if photo_attachment:
                state["photo_attachment"] = photo_attachment
            if photo_url:
                state["photo_url"] = photo_url
            await set_state(user_id, state)
            caption = (message.text or "").strip()
            if 5 <= len(caption) <= 800:
                await finalize_damage_report(
                    message,
                    user_id,
                    caption,
                    state.get("photo_attachment"),
                    state.get("photo_url"),
                )
            else:
                await message.answer(
                    "📷 Фото обновил. Теперь пришлите текстовое описание поломки.",
                    keyboard=kb_damage_cancel(),
                )
            return

        # Если пользователь просто прислал фото — тоже принимаем как репорт.
        await handle_damage_photo(message, user_id, photo_attachment, photo_url)
        return

    if state:
        step = state.get("step")

        if step == "booking_start":
            await handle_booking_start_input(message, user_id, state, text)
            return

        if step == "booking_duration":
            try:
                parsed_end = parse_datetimes(text)
            except ValueError:
                parsed_end = []

            if parsed_end:
                await handle_booking_end_input(
                    message,
                    user_id,
                    state,
                    text,
                    from_duration_step=True,
                )
            else:
                await message.answer(
                    f"⏳ Выберите длительность кнопкой ниже или отправьте время "
                    f"окончания.\n\nНачало: {fmt_dt(state['start'])}",
                    keyboard=kb_duration(int(state.get("resource_id", 0))),
                )
            return

        if step == "booking_end":
            await handle_booking_end_input(message, user_id, state, text)
            return

        if step == "contribute_poll":
            poll_id = int(state.get("poll_id", 0))
            digits = re.sub(r"[^\d]", "", text or "")
            amount = as_int(digits)
            if poll_id <= 0 or amount is None or amount <= 0:
                await message.answer(
                    "❌ Введите целое число баллов, например: 500.",
                    keyboard=kb_damage_cancel(),
                )
                return

            (
                ok,
                contribution_text,
                reached_now,
                question,
                collected,
                goal,
                is_user_idea,
            ) = await contribute_to_poll(user_id, poll_id, amount)

            if not ok:
                await message.answer(
                    contribution_text,
                    keyboard=kb_damage_cancel(),
                )
                return

            await clear_state(user_id)
            await message.answer(
                contribution_text,
                keyboard=kb_poll_card(
                    poll_id,
                    True,
                    is_user_idea,
                    goal_reached=collected >= goal,
                ),
            )
            if reached_now:
                await notify_crowdfunding_goal(
                    poll_id,
                    question,
                    collected,
                    goal,
                )
            return

        if step == "damage":
            if text:
                await message.answer(
                    "📷 Нужно именно фото поломки. Пришлите фотографию "
                    "(можно с подписью) — без неё репорт не приму.",
                    keyboard=kb_damage_cancel(),
                )
            else:
                await message.answer(
                    state_reminder_text(state),
                    keyboard=kb_damage_cancel(),
                )
            return

        if step == "damage_desc":
            if 5 <= len(text) <= 800:
                await finalize_damage_report(
                    message,
                    user_id,
                    text,
                    state.get("photo_attachment"),
                    state.get("photo_url"),
                )
            else:
                await message.answer(
                    "❌ Описание должно быть от 5 до 800 символов. "
                    "Опишите поломку подробнее.",
                    keyboard=kb_damage_cancel(),
                )
            return

        if step == "idea_question":
            question = " ".join(text.split())
            if 5 <= len(question) <= 200:
                await set_state(
                    user_id,
                    {
                        "step": "idea_description",
                        "idea_question": question,
                    },
                )
                await message.answer(
                    "✅ Вопрос принят:\n"
                    f"💡 {question}\n\n"
                    "Шаг 2 из 2. Добавьте описание: почему это важно, "
                    "что даст соседям и сколько примерно стоит.\n"
                    "От 5 до 1000 символов.",
                    keyboard=kb_damage_cancel(),
                )
            else:
                await message.answer(
                    "❌ Вопрос должен быть от 5 до 200 символов. Попробуйте ещё раз.",
                    keyboard=kb_damage_cancel(),
                )
            return

        if step == "idea_description":
            question = str(state.get("idea_question") or "").strip()
            if not question:
                await set_state(user_id, {"step": "idea_question"})
                await message.answer(
                    "❌ Вопрос потерялся. Напишите его заново.",
                    keyboard=kb_damage_cancel(),
                )
                return

            if 5 <= len(text) <= 1000:
                await publish_user_idea_message(message, user_id, question, text)
            else:
                await message.answer(
                    "❌ Описание должно быть от 5 до 1000 символов. "
                    "Попробуйте ещё раз.",
                    keyboard=kb_damage_cancel(),
                )
            return

        await clear_state(user_id)

    if not text:
        return

    if lower in {"привет", "здравствуйте", "hi", "hello"}:
        menu_text, menu_kb = await build_main_menu(user_id)
        await message.answer("👋 Привет! " + menu_text, keyboard=menu_kb)
        return

    await message.answer(
        "🤔 Не понял. Пользуйтесь кнопками меню или нажмите /start.\n\n"
        "Подсказка: забронировать вещь можно кнопкой «📦 Забронировать».",
        keyboard=kb_main(is_admin),
    )


# ============================================================================
# ОБРАБОТЧИК CALLBACK-КНОПОК
# ============================================================================


async def route_callback(event: MessageEvent) -> None:
    user_id = event.user_id
    payload = event.payload or {}
    if not isinstance(payload, dict):
        await ack_event(event)
        return

    cmd = str(payload.get("cmd", ""))
    is_admin = user_id == ADMIN_ID

    await ack_event(event)
    await ensure_user_vk(bot.api, user_id)

    # --- Главное меню и бронирование --------------------------------------
    if cmd == "menu":
        await clear_state(user_id)
        text, keyboard = await build_main_menu(user_id)
        await edit_or_send(event, text, keyboard)
        return

    if cmd == "book":
        await clear_state(user_id)
        text, keyboard = await build_resources_page(0)
        await edit_or_send(event, text, keyboard)
        return

    if cmd == "resources":
        page = as_int(payload.get("page"), 0) or 0
        text, keyboard = await build_resources_page(page)
        await edit_or_send(event, text, keyboard)
        return

    if cmd == "resource":
        resource_id = as_int(payload.get("id"))
        if resource_id is None:
            await edit_or_send(event, "❌ Ресурс не найден.", kb_main(is_admin))
            return
        text, keyboard = await build_resource_card(resource_id)
        await edit_or_send(event, text, keyboard)
        return

    if cmd == "resource_slots":
        resource_id = as_int(payload.get("id"))
        if resource_id is None:
            await edit_or_send(event, "❌ Ресурс не найден.", kb_main(is_admin))
            return
        text, keyboard = await build_resource_slots(resource_id)
        await edit_or_send(event, text, keyboard)
        return

    if cmd == "book_resource":
        resource_id = as_int(payload.get("id"))
        if resource_id is None:
            await edit_or_send(event, "❌ Ресурс не найден.", kb_main(is_admin))
            return

        async with SessionLocal() as session:
            resource = await session.get(Resource, resource_id)
            user = await session.get(User, user_id)
        if resource is None or not resource.is_active:
            await edit_or_send(event, "❌ Ресурс недоступен.", kb_main(is_admin))
            return
        if (
            user is not None
            and user.blocked_until is not None
            and user.blocked_until > now_naive()
        ):
            await edit_or_send(
                event,
                "🚫 Бронирование временно недоступно.\n\n"
                f"Вы заблокированы до {fmt_dt(user.blocked_until)}.",
                kb_main(is_admin),
            )
            return

        await set_state(
            user_id,
            {
                "step": "booking_start",
                "resource_id": resource_id,
            },
        )
        text = (
            f"📅 Бронирование: {resource.emoji} «{resource.name}»\n\n"
            f"Введите начало в формате:\n25.10.2026 18:00\n\n"
            "Можно сразу указать интервал:\n"
            "25.10.2026 18:00 - 25.10.2026 20:00\n\n"
            f"Сейчас: {fmt_dt(now_naive())}\n"
            f"Стандартная длительность: {resource.default_duration_minutes} мин."
        )
        await edit_or_send(event, text, kb_booking_retry(resource_id))
        return

    if cmd == "duration":
        state = await get_state(user_id)
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
            await clear_state(user_id)
            await edit_or_send(
                event,
                "❌ Сессия бронирования устарела. Выберите ресурс заново.",
                kb_main(is_admin),
            )
            return

        start = state["start"]
        end = start + timedelta(hours=hours)
        attempt = await try_book(user_id, resource_id, start, end)

        if attempt.ok:
            await clear_state(user_id)
            await edit_or_send(event, attempt.text, kb_booking_done())
        else:
            if attempt.blocked:
                await clear_state(user_id)
            else:
                await set_state(
                    user_id,
                    {
                        "step": "booking_start",
                        "resource_id": resource_id,
                    },
                )
            failure_keyboard = (
                kb_main(is_admin)
                if attempt.blocked
                else kb_booking_conflict(attempt, resource_id)
            )
            suffix = "" if attempt.blocked else "\n\nВведите новое время начала."
            await edit_or_send(
                event,
                attempt.text + suffix,
                failure_keyboard,
            )
        return

    if cmd == "custom_end":
        state = await get_state(user_id)
        resource_id = as_int(payload.get("rid"))
        if (
            not state
            or state.get("step") != "booking_duration"
            or resource_id is None
            or state.get("resource_id") != resource_id
            or not isinstance(state.get("start"), datetime)
        ):
            await clear_state(user_id)
            await edit_or_send(
                event,
                "❌ Сессия бронирования устарела. Выберите ресурс заново.",
                kb_main(is_admin),
            )
            return

        state["step"] = "booking_end"
        await set_state(user_id, state)
        await edit_or_send(
            event,
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
            await edit_or_send(
                event,
                "❌ Не удалось прочитать предложенное время. Выберите заново.",
                kb_main(is_admin),
            )
            return

        attempt = await try_book(user_id, resource_id, start, end)
        if attempt.ok:
            await clear_state(user_id)
            await edit_or_send(event, attempt.text, kb_booking_done())
        else:
            if attempt.blocked:
                await clear_state(user_id)
            else:
                await set_state(
                    user_id,
                    {
                        "step": "booking_start",
                        "resource_id": resource_id,
                    },
                )
            failure_keyboard = (
                kb_main(is_admin)
                if attempt.blocked
                else kb_booking_conflict(attempt, resource_id)
            )
            await edit_or_send(event, attempt.text, failure_keyboard)
        return

    if cmd == "my_bookings":
        page = as_int(payload.get("page"), 0) or 0
        text, keyboard = await build_my_bookings(user_id, page)
        await edit_or_send(event, text, keyboard)
        return

    if cmd == "return":
        booking_id = as_int(payload.get("id"))
        if booking_id is None:
            await edit_or_send(event, "❌ Бронь не найдена.", kb_main(is_admin))
            return
        ok, result_text = await return_booking(booking_id, user_id)
        await edit_or_send(event, result_text, kb_booking_done())
        return

    if cmd == "cancel_booking":
        booking_id = as_int(payload.get("id"))
        if booking_id is None:
            await edit_or_send(event, "❌ Бронь не найдена.", kb_main(is_admin))
            return
        ok, result_text = await cancel_booking(booking_id, user_id, is_admin=is_admin)
        await edit_or_send(event, result_text, kb_booking_done())
        return

    # --- Профиль и помощь --------------------------------------------------
    if cmd == "profile":
        text, keyboard = await build_profile(user_id)
        await edit_or_send(event, text, keyboard)
        return

    if cmd == "help":
        text, keyboard = build_help(is_admin=is_admin)
        await edit_or_send(event, text, keyboard)
        return

    # --- Поломки -----------------------------------------------------------
    if cmd == "damage":
        await set_state(user_id, {"step": "damage"})
        text = (
            "🛠 Сообщение о поломке\n\n"
            "Пришлите одно сообщение: фото повреждённой вещи и подпись "
            "с описанием.\n\n"
            "Например: «У шуруповёрта треснул корпус, искрит».\n\n"
            "Если фото без подписи — я попрошу описание отдельно."
        )
        await edit_or_send(event, text, kb_damage_cancel())
        return

    # --- Народные инициативы ----------------------------------------------
    if cmd == "idea":
        if await idea_quota_left(user_id) <= 0:
            await edit_or_send(
                event,
                f"❌ Вы уже предложили {MAX_IDEAS_PER_DAY} идеи за сутки.\n"
                "Дайте соседям время проголосовать и попробуйте завтра.",
                kb_main(is_admin),
            )
            return
        await set_state(user_id, {"step": "idea_question"})
        await edit_or_send(
            event,
            "💡 Народная инициатива\n\n"
            "Шаг 1 из 2. Напишите вопрос, который стоит решить всем двором.\n\n"
            "Например: «Поставить шлагбаум во дворе?»\n\n"
            "Вопрос должен быть от 5 до 200 символов.",
            kb_damage_cancel(),
        )
        return

    # --- Голосования -------------------------------------------------------
    if cmd == "polls":
        page = as_int(payload.get("page"), 0) or 0
        ideas_only = bool(as_int(payload.get("ideas"), 0))
        text, keyboard = await build_polls(user_id, page, ideas_only)
        await edit_or_send(event, text, keyboard)
        return

    if cmd == "poll":
        poll_id = as_int(payload.get("id"))
        if poll_id is None:
            await edit_or_send(event, "❌ Голосование не найдено.", kb_main(is_admin))
            return
        text, keyboard = await build_poll_view(user_id, poll_id)
        await edit_or_send(event, text, keyboard)
        return

    if cmd == "contribute":
        poll_id = as_int(payload.get("id"))
        if poll_id is None:
            await edit_or_send(event, "❌ Голосование не найдено.", kb_main(is_admin))
            return
        async with SessionLocal() as session:
            poll = await session.get(Poll, poll_id)
        if poll is None or not poll.is_active:
            await edit_or_send(
                event,
                "❌ Это голосование уже завершено.",
                kb_main(is_admin),
            )
            return
        if int(poll.collected_points or 0) >= int(poll.goal_points or 0):
            await edit_or_send(
                event,
                "🎉 Цель уже достигнута — спасибо соседям!",
                kb_poll_card(
                    poll.id,
                    poll.is_active,
                    poll.is_user_idea,
                    goal_reached=True,
                ),
            )
            return

        await set_state(
            user_id,
            {
                "step": "contribute_poll",
                "poll_id": poll.id,
                "question": poll.question,
            },
        )
        await edit_or_send(
            event,
            "💰 Внести баллы в копилку\n\n"
            f"💡 {poll.question}\n"
            f"Собрано: {fmt_money(int(poll.collected_points or 0))} / "
            f"{fmt_money(int(poll.goal_points or 0))} баллов.\n\n"
            "Сколько баллов внести? Отправьте число, например: 500.\n"
            "Баллы спишутся с вашего баланса и попадут в копилку опроса.",
            kb_damage_cancel(),
        )
        return

    if cmd == "vote":
        poll_id = as_int(payload.get("poll_id"))
        value = as_int(payload.get("value"))
        if poll_id is None or value not in (1, -1):
            await edit_or_send(event, "❌ Некорректный голос.", kb_main(is_admin))
            return
        ok, vote_message = await cast_vote(user_id, poll_id, value)
        text, keyboard = await build_poll_view(user_id, poll_id)
        if not ok:
            await edit_or_send(
                event,
                f"⚠️ {vote_message}\n\n{text}",
                keyboard,
            )
            return
        await edit_or_send(event, text, keyboard)

        alert = await check_idea_threshold(poll_id)
        if alert is not None:
            question, votes_for, author_id, idea_poll_id = alert
            await notify_idea_threshold(
                question,
                votes_for,
                author_id,
                idea_poll_id,
            )
        return

    # --- Админ-панель ------------------------------------------------------
    if cmd.startswith("admin"):
        if not is_admin:
            await edit_or_send(event, "⛔ Недоступно.", kb_main(False))
            return

        if cmd == "admin":
            text, keyboard = await build_admin_panel()
            await edit_or_send(event, text, keyboard)
            return

        if cmd == "admin_stats":
            text, keyboard = await build_admin_stats()
            await edit_or_send(event, text, keyboard)
            return

        if cmd == "admin_reports":
            text, keyboard = await build_admin_reports()
            await edit_or_send(event, text, keyboard)
            return

        if cmd == "admin_inventory":
            text, keyboard = await build_admin_inventory()
            await edit_or_send(event, text, keyboard)
            return

        if cmd == "admin_polls":
            text, keyboard = await build_admin_polls()
            await edit_or_send(event, text, keyboard)
            return

        if cmd == "admin_resolve":
            report_id = as_int(payload.get("id"))
            if report_id is None:
                await edit_or_send(event, "❌ Репорт не найден.", kb_admin_back())
                return
            ok, result_text, reporter_id = await resolve_damage_report(report_id)
            if ok and reporter_id is not None:
                await send_to_peer(
                    reporter_id,
                    f"✅ Ваш репорт о поломке #{report_id} отмечен как решённый. "
                    "Спасибо, что сообщили!",
                )
            text, keyboard = await build_admin_reports()
            await edit_or_send(event, text, keyboard)
            return

        if cmd == "admin_close_poll":
            poll_id = as_int(payload.get("id"))
            if poll_id is None:
                await edit_or_send(event, "❌ Голосование не найдено.", kb_admin_back())
                return
            await admin_close_poll(poll_id)
            text, keyboard = await build_admin_polls()
            await edit_or_send(event, text, keyboard)
            return

    # Неизвестная/устаревшая кнопка.
    await edit_or_send(
        event,
        "🤷 Кнопка устарела. Откройте меню заново.",
        kb_main(is_admin),
    )


# ============================================================================
# РЕГИСТРАЦИЯ ОБРАБОТЧИКОВ
# ============================================================================

bot = Bot(token=VK_TOKEN)


@bot.on.message()
async def on_message(message: Message) -> None:
    try:
        await route_message(message)
    except Exception:
        logger.exception("Ошибка при обработке сообщения от %s", message.from_id)
        with suppress(Exception):
            await message.answer(
                "😔 Что-то пошло не так. Попробуйте ещё раз или нажмите /start.",
                keyboard=kb_main(message.from_id == ADMIN_ID),
            )


@bot.on.raw_event(GroupEventType.MESSAGE_EVENT, dataclass=MessageEvent)
async def on_callback(event: MessageEvent) -> None:
    try:
        await route_callback(event)
    except Exception:
        logger.exception("Ошибка при обработке callback от %s", event.user_id)
        with suppress(Exception):
            await send_to_peer(
                event.peer_id,
                "😔 Не удалось обработать кнопку. Попробуйте ещё раз.",
                keyboard=kb_main(event.user_id == ADMIN_ID),
            )


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

            sent = await send_to_peer(
                user.id,
                text,
                keyboard=kb_return_notify(booking.id),
            )

            # Помечаем даже при ошибке отправки: иначе при закрытых ЛС бот
            # будет пытаться отправить одно и то же каждую минуту.
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
    if not VK_TOKEN:
        raise RuntimeError(
            "Не задан VK_TOKEN. Скопируйте .env.example в .env и укажите "
            "токен сообщества: Управление -> Работа с API -> Токены."
        )
    if ADMIN_ID <= 0:
        raise RuntimeError("ADMIN_ID должен быть положительным VK ID пользователя.")
    if ADMIN_ID == 12345:
        logger.warning(
            "ADMIN_ID всё ещё равен 12345 (заглушка). Репорты и админ-команды "
            "не будут работать, пока вы не укажете свой VK ID."
        )

    await init_db()
    logger.info("База данных: %s", DB_PATH)
    logger.info("Админ VK ID: %s", ADMIN_ID)

    reminder_task = asyncio.create_task(
        notification_worker(),
        name="booking-reminders",
    )

    try:
        logger.info("Бот «Соседский ресурс» запущен. Polling...")
        await bot.run_polling()
    finally:
        reminder_task.cancel()
        with suppress(asyncio.CancelledError):
            await reminder_task
        with suppress(Exception):
            await bot.api.http_client.close()
        await engine.dispose()
        logger.info("Бот остановлен, соединения закрыты.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
