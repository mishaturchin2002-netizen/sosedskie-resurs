"""
Автотесты «Соседского ресурса» (VK-бот).

Запуск:
    pip install -r requirements.txt
    pytest -q

Тесты используют отдельную SQLite-базу, не трогают рабочую
sosedskiy_resurs.db и не делают запросов к VK API.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Переменные окружения должны быть выставлены ДО импорта main:
# main.py создаёт engine и Bot на этапе импорта.
PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))
TEST_DB = PROJECT_DIR / "_test_sosedskiy_resurs.db"
os.environ["VK_TOKEN"] = "test_token"
os.environ["ADMIN_ID"] = "1"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"

import pytest
import pytest_asyncio

import main as bot_main


pytestmark = pytest.mark.asyncio


def pytest_sessionfinish(session, exitstatus):  # noqa: ARG001
    """Убираем тестовую базу после прогона."""
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(f"{TEST_DB}{suffix}").unlink()
        except OSError:
            pass


@pytest_asyncio.fixture
async def db():
    """Чистая БД и один тестовый ресурс перед каждым тестом."""
    await bot_main.engine.dispose()
    async with bot_main.engine.begin() as connection:
        await connection.run_sync(bot_main.Base.metadata.drop_all)
        await connection.run_sync(bot_main.Base.metadata.create_all)

    async with bot_main.SessionLocal() as session:
        resource = bot_main.Resource(
            name="Тестовый мангал",
            description="Ресурс для pytest",
            emoji="🔥",
            default_duration_minutes=120,
            is_active=True,
        )
        session.add(resource)
        await session.commit()
        await session.refresh(resource)

    # Lock-объекты не должны переезжать между event loop разных тестов.
    bot_main.BOOKING_LOCK = asyncio.Lock()
    bot_main.BOOKING_ACTION_LOCK = asyncio.Lock()

    yield resource

    await bot_main.engine.dispose()


async def make_user(user_id: int, points: int = 1000) -> bot_main.User:
    async with bot_main.SessionLocal() as session:
        user = bot_main.User(
            id=user_id,
            first_name=f"Сосед{user_id}",
            last_name="Тестов",
            points=points,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user


async def test_booking_overlap_is_rejected(db):
    """Пересечение интервалов: вторая бронь должна получить отказ."""
    await make_user(101)
    await make_user(102)

    start = bot_main.now_naive() + timedelta(days=2)
    end = start + timedelta(hours=2)

    first = await bot_main.try_book(101, db.id, start, end)
    assert first.ok, first.text

    second = await bot_main.try_book(
        102,
        db.id,
        start + timedelta(minutes=30),
        end + timedelta(minutes=30),
    )
    assert not second.ok
    assert "уже забронировано" in second.text
    assert "Сосед101" in second.text


async def test_successful_booking_freezes_deposit(db):
    """Успешная бронь списывает залог 3 балла и считает экономию 500 ₽."""
    await make_user(201)

    start = bot_main.now_naive() + timedelta(days=3)
    end = start + timedelta(hours=2)
    attempt = await bot_main.try_book(201, db.id, start, end)
    assert attempt.ok, attempt.text
    assert attempt.booking_id is not None

    async with bot_main.SessionLocal() as session:
        user = await session.get(bot_main.User, 201)
        booking = await session.get(bot_main.Booking, attempt.booking_id)

    assert user is not None
    assert booking is not None
    assert user.points == 997
    assert user.total_saved_rub == 500
    assert booking.deposit_status == "held"
    assert booking.deposit_amount == 3


async def test_return_refunds_deposit_and_adds_bonus(db):
    """Возврат вовремя: залог возвращается, начисляется +1 балл."""
    await make_user(301)

    start = bot_main.now_naive() + timedelta(days=1)
    end = start + timedelta(hours=2)
    attempt = await bot_main.try_book(301, db.id, start, end)
    assert attempt.ok
    assert attempt.booking_id is not None

    # Делаем бронь «текущей», чтобы проверить именно возврат, а не будущую.
    now = bot_main.now_naive()
    async with bot_main.SessionLocal() as session:
        booking = await session.get(bot_main.Booking, attempt.booking_id)
        booking.start_time = now - timedelta(hours=1)
        booking.end_time = now + timedelta(hours=1)
        await session.commit()

    ok, text = await bot_main.return_booking(attempt.booking_id, 301)
    assert ok, text

    async with bot_main.SessionLocal() as session:
        user = await session.get(bot_main.User, 301)
        booking = await session.get(bot_main.Booking, attempt.booking_id)

    assert user is not None
    assert booking is not None
    assert user.points == 1001  # 1000 − 3 залог + 3 возврат + 1 бонус
    assert booking.deposit_status == "returned"
    assert booking.status == "completed"


async def test_fsm_state_survives_reload(db):
    """FSM-состояние сохраняется в SQLite и восстанавливается."""
    await make_user(401)
    start = datetime(2026, 10, 25, 18, 0)

    await bot_main.set_state(
        401,
        {
            "step": "booking_duration",
            "resource_id": db.id,
            "start": start,
        },
    )

    restored = await bot_main.get_state(401)
    assert restored is not None
    assert restored["step"] == "booking_duration"
    assert restored["resource_id"] == db.id
    assert restored["start"] == start

    await bot_main.clear_state(401)
    assert await bot_main.get_state(401) is None


async def test_vote_blocked_for_zero_reputation(db):
    """Голосование запрещено при балансе 0."""
    await make_user(601, points=0)
    async with bot_main.SessionLocal() as session:
        poll = bot_main.Poll(
            question="Тестовое голосование?",
            description="Проверка ограничения",
            goal_points=0,
            collected_points=0,
            is_active=True,
        )
        session.add(poll)
        await session.commit()
        await session.refresh(poll)
        poll_id = poll.id

    ok, message = await bot_main.cast_vote(601, poll_id, 1)
    assert not ok
    assert "рейтинг слишком низок" in message


async def test_crowdfunding_contribution_reaches_goal(db):
    """Взнос списывает баллы и закрывает цель, не превышая её."""
    await make_user(501)

    async with bot_main.SessionLocal() as session:
        poll = bot_main.Poll(
            question="Купить снегоуборочную машину?",
            description="Тестовая инициатива",
            budget_rub=0,
            goal_points=100,
            collected_points=0,
            is_active=True,
        )
        session.add(poll)
        await session.commit()
        await session.refresh(poll)
        poll_id = poll.id

    result = await bot_main.contribute_to_poll(501, poll_id, 150)
    ok, _text, reached_now, _question, collected, goal, _is_idea = result
    assert ok
    assert reached_now
    assert collected == 100
    assert goal == 100

    async with bot_main.SessionLocal() as session:
        user = await session.get(bot_main.User, 501)
        poll = await session.get(bot_main.Poll, poll_id)

    assert user is not None
    assert poll is not None
    assert user.points == 900  # списалось только 100 — остаток цели
    assert poll.collected_points == 100
    assert poll.goal_notified is True
