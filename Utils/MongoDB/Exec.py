import asyncio
import json
from typing import Any, Dict, List, Optional

from Utils.MongoDB.Server import Server


class Exec:
    def __init__(self, db_name: str = "university", worker_count: int = 3):
        self.server = Server()
        self.db_name = db_name
        self.worker_count = worker_count
        self.processor: asyncio.Queue = asyncio.Queue()
        self._workers: List[asyncio.Task] = []
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker(i + 1))
            for i in range(self.worker_count)
        ]

    async def stop(self) -> None:
        if not self._running:
            return

        # Send sentinel values to stop workers
        for _ in self._workers:
            await self.processor.put(None)

        await self.processor.join()

        for task in self._workers:
            await task

        self._workers.clear()
        self._running = False

    async def add_to_queue(self, query: Dict[str, Any]) -> None:
        await self.processor.put(query)

    async def add_many_to_queue(self, queries: List[Dict[str, Any]]) -> None:
        for query in queries:
            await self.processor.put(query)

    async def load_from_json(self, file_path: str) -> List[Dict[str, Any]]:
        def _read():
            with open(file_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return payload.get("queries", [])

        queries = await asyncio.to_thread(_read)
        await self.add_many_to_queue(queries)
        return queries

    async def _worker(self, worker_id: int) -> None:
        while True:
            query = await self.processor.get()
            try:
                if query is None:  # sentinel
                    return

                result = await self.execute_query(query)
                print(f"[worker-{worker_id}] OK -> {query.get('action')} {query.get('entity')} | {result}")
            except Exception as ex:
                print(f"[worker-{worker_id}] ERROR -> {ex} | query={query}")
            finally:
                self.processor.task_done()

    async def execute_query(self, query: Dict[str, Any]) -> Any:
        action = query.get("action")
        entity = query.get("entity")
        options = query.get("options", {}) or {}

        if not action or not entity:
            raise ValueError("query must contain 'action' and 'entity'")

        # Server methods are sync, run in thread pool
        if action == "get":
            return await asyncio.to_thread(
                self.server.getRecords,
                self.db_name,
                entity,
                query.get("fields", []),
                query.get("where", {}),
                options.get("group_by"),
                options.get("sort"),
                options.get("limit"),
            )

        if action == "add":
            return await asyncio.to_thread(
                self.server.addRecords,
                self.db_name,
                entity,
                query.get("data"),
            )

        if action == "change":
            return await asyncio.to_thread(
                self.server.changeRecords,
                self.db_name,
                entity,
                query.get("where", {}),
                query.get("data", {}),
                options.get("upsert", False),
                options.get("multi", True),
            )

        if action == "remove":
            # Optional cascade example for student -> embedded takes cleanup
            if options.get("cascade", False) and entity == "student":
                where = query.get("where", {})
                # Remove matching students first
                student_result = await asyncio.to_thread(
                    self.server.removeRecords,
                    self.db_name,
                    entity,
                    where,
                    options.get("multi", True),
                )
                # If you need explicit additional cascade rules, add them here
                return {"student_delete": student_result, "cascade": True}

            return await asyncio.to_thread(
                self.server.removeRecords,
                self.db_name,
                entity,
                query.get("where", {}),
                options.get("multi", True),
            )

        raise ValueError(f"unsupported action: {action}")


# Optional quick runner
async def run_sample() -> None:
    exec_engine = Exec(db_name="university", worker_count=4)
    await exec_engine.start()
    await exec_engine.load_from_json(r"d:\CS432-Assignment-1\Utils\MongoDB\betterCRUD.json")
    await exec_engine.stop()


if __name__ == "__main__":
    asyncio.run(run_sample())