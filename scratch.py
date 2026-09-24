import asyncio
from sqlalchemy import select, func
from fairlane.models import Worker, WorkerStatus
from fairlane.db import async_session_factory

async def main():
    async with async_session_factory() as db:
        active = await db.execute(select(func.count(Worker.id)).where(Worker.status == WorkerStatus.ACTIVE))
        print("ACTIVE with Enum:", active.scalar_one_or_none())
        
        active_str = await db.execute(select(func.count(Worker.id)).where(Worker.status == "ACTIVE"))
        print("ACTIVE with str:", active_str.scalar_one_or_none())

asyncio.run(main())
