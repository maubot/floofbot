from typing import Iterable
from dataclasses import dataclass
import html
import time

from maubot import MessageEvent, Plugin
from maubot.handlers import command
from mautrix.types import EventID, MatrixURI, UserID
from mautrix.util.async_db import Connection, Database, Scheme, UpgradeTable
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper
from mautrix.util.formatter import EntityString, EntityType, MatrixParser, SimpleEntity

SimpleEntityString = EntityString[SimpleEntity, EntityType]


class EntityParser(MatrixParser[SimpleEntityString]):
    fs = SimpleEntityString


@dataclass
class RateLimitBucket:
    user_id: UserID
    last_timestamp: float
    count: float


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("floof")
        helper.copy("count_overflow_message")
        helper.copy("ratelimit_overflow_reaction")
        helper.copy("ratelimit_capacity")
        helper.copy("ratelimit_refill_per")


upgrade_table = UpgradeTable()


@upgrade_table.register(description="Initial revision", upgrades_to=1)
async def upgrade_v1(conn: Connection, scheme: Scheme) -> None:
    await conn.execute("""
        CREATE TABLE flooferboard (
            user_id TEXT   NOT NULL,
            count   BIGINT NOT NULL,

            PRIMARY KEY (user_id)
        );
    """)
    await conn.execute("""
        CREATE TABLE floofeeboard (
            user_id TEXT   NOT NULL,
            count   BIGINT NOT NULL,

            PRIMARY KEY (user_id)
        );
    """)


@upgrade_table.register(description="Add individual floofs table", upgrades_to=2)
async def upgrade_v2(conn: Connection, scheme: Scheme) -> None:
    await conn.execute("""
        CREATE TABLE floof (
            event_id  TEXT    NOT NULL,
            floofee   TEXT    NOT NULL,
            floofer   TEXT    NOT NULL,
            timestamp BIGINT  NOT NULL,
            count     INTEGER NOT NULL,

            PRIMARY KEY (event_id, floofee)
        );
    """)


class FloofBot(Plugin):
    database: Database
    flood_tracker: dict[UserID, RateLimitBucket]
    ratelimit_rate: float
    ratelimit_capacity: float
    ratelimit_overflow_reaction: str
    count_overflow_message: str
    floof_html: str
    parser = EntityParser()

    async def start(self) -> None:
        self.flood_tracker = {}
        self.on_external_config_update()

    @classmethod
    def get_config_class(cls) -> type[BaseProxyConfig]:
        return Config

    @classmethod
    def get_db_upgrade_table(cls) -> UpgradeTable:
        return upgrade_table

    def on_external_config_update(self) -> None:
        self.config.load_and_update()
        self.ratelimit_capacity = float(self.config["ratelimit_capacity"])
        self.ratelimit_rate = 1 / float(self.config["ratelimit_refill_per"])
        self.ratelimit_overflow_reaction = self.config["ratelimit_overflow_reaction"]
        self.count_overflow_message = self.config["count_overflow_message"]
        self.floof_html = self.config["floof"]

    def _get_bucket(self, user_id: UserID) -> RateLimitBucket:
        now = time.monotonic()
        try:
            bucket = self.flood_tracker[user_id]
        except KeyError:
            bucket = self.flood_tracker[user_id] = RateLimitBucket(
                user_id=user_id,
                last_timestamp=now,
                count=self.ratelimit_capacity,
            )
        else:
            tokens_to_add = (now - bucket.last_timestamp) * self.ratelimit_rate
            bucket.count = min(self.ratelimit_capacity, bucket.count + tokens_to_add)
            bucket.last_timestamp = now
        return bucket

    def _allow_ratelimit(self, user_id: UserID, tokens_to_use: float) -> bool:
        bucket = self._get_bucket(user_id)
        # This intentionally checks against 1 instead of tokens_to_use: going negative is allowed
        if bucket.count < 1:
            return False
        bucket.count -= tokens_to_use
        return True

    @command.new("furrylimit", aliases=["fluffylimit", "flooflimit", "floolimit"])
    @command.argument("unused", pass_raw=True, required=False)
    async def furry_limit(self, event: MessageEvent, unused: str = "") -> None:
        if unused == ":3":
            await event.react(":3")
        bucket = self._get_bucket(event.sender)
        bucket.count -= 0.1
        await event.react(f"{bucket.count:.2f}")

    def _make_mention(self, user_id: UserID) -> str:
        return f'<a href="{MatrixURI.build(user_id).matrix_to_url}">{html.escape(user_id)}</a>'

    def _make_floof_list(
        self, items: list[tuple[UserID, int]], own_user_id: UserID
    ) -> Iterable[str]:
        total_floofs = sum(count for _, count in items)
        for i, (user_id, count) in enumerate(items):
            if i > 4 and user_id != own_user_id:
                continue
            strong = strongend = ""
            if user_id == own_user_id:
                strong = "<strong>"
                strongend = "</strong>"
            yield f"<br>{strong}#{i+1}: {self._make_mention(user_id)}: {count} ({count / total_floofs * 100:.1f}%){strongend}</li>"

    @command.new("floofboars")
    async def floofboars(self, event: MessageEvent) -> None:
        await event.react("🐗")

    @command.new(
        "floofboard",
        aliases=["flooboard", "furryboard", "fluffyboard", "flooferboard", "floofeeboard"],
    )
    @command.argument("unused", pass_raw=True, required=False)
    async def floofboard(self, event: MessageEvent, unused: str = "") -> None:
        if unused == ":3":
            await event.react(":3")
        async with self.database.acquire() as conn:
            floofers = await conn.fetch(
                "SELECT user_id, count FROM flooferboard ORDER BY count DESC"
            )
            floofees = await conn.fetch(
                "SELECT user_id, count FROM floofeeboard ORDER BY count DESC"
            )
            own_top_floofee = await conn.fetchrow(
                "SELECT floofee AS user_id, SUM(count) AS count FROM floof WHERE floofer=$1 GROUP BY 1 ORDER BY 2 DESC LIMIT 1",
                event.sender,
            )
            own_top_floofer = await conn.fetchrow(
                "SELECT floofer AS user_id, SUM(count) AS count FROM floof WHERE floofee=$1 GROUP BY 1 ORDER BY 2 DESC LIMIT 1",
                event.sender,
            )

        output = [
            "<p>",
            f"<b>Floofees</b> ({len(floofees)} total users)",
            *self._make_floof_list(floofees, event.sender),
            "</p><p>",
            f"<b>Floofers</b> ({len(floofers)} total users)",
            *self._make_floof_list(floofers, event.sender),
            "</p><p>",
            (
                f"Your top floofer is {self._make_mention(own_top_floofer["user_id"])} ({own_top_floofer["count"]} floofs)"
                if own_top_floofer
                else "You haven't been floofed yet"
            ),
            "<br>",
            (
                f"Your top floofee is {self._make_mention(own_top_floofee["user_id"])} ({own_top_floofee["count"]} floofs)"
                if own_top_floofee
                else "You haven't floofed anyone yet"
            ),
            "</p>",
        ]
        await event.reply(
            "\n".join(output),
            allow_html=True,
            markdown=False,
            extra_content={
                "body": "Floofboard (only available in HTML)",
                "m.mentions": {},
            },
        )

    @command.new("floof", aliases=["floo", "*****"])
    @command.argument("floof_count", parser=int, label="floof count")
    @command.argument("target", pass_raw=True, label="targets...")
    async def floof(self, event: MessageEvent, floof_count: int, target: str) -> EventID:
        if not event.content.formatted_body:
            return await event.reply("Floof target users must be specified as @mentions in HTML")
        es = await self.parser.parse(event.content.formatted_body)
        mentions: dict[UserID, str] = {}
        for ent in es.entities:
            if ent.type == EntityType.USER_MENTION:
                displayname = es.text[ent.offset : ent.offset + ent.length]
                if len(displayname) > 50:
                    displayname = displayname[:40] + "…"
                mentions[ent.extra_info["user_id"]] = displayname
        if not mentions:
            return await event.reply("Floof target users must be specified as @mentions in HTML")
        elif len(mentions) > 5:
            return await event.reply("You can only floof up to 5 users at a time")
        elif event.sender in mentions:
            return await event.reply("You cannot floof yourself")
        elif floof_count < len(mentions):
            return await event.reply(
                f"You must include at least one floof per recipient ({floof_count} < {len(mentions)})"
            )
        was_encrypted = event.get("mautrix", {}).get("was_encrypted", False)
        limit = 950 if was_encrypted else 1200
        if event.sender != "@kaesa:neoshadow.co" and "@kaesa:neoshadow.co" not in mentions:
            limit = 200
        if floof_count > limit:
            if not self._allow_ratelimit(event.sender, 0.75):
                return await event.react(self.ratelimit_overflow_reaction)
            return await event.reply(self.count_overflow_message, allow_html=True, markdown=False)
        if not self._allow_ratelimit(event.sender, 1 + floof_count * 0.01):
            return await event.react(self.ratelimit_overflow_reaction)
        target_html_parts = []
        per_user_floofs = int(floof_count / len(mentions))
        current_time = int(time.time() * 1000)
        async with self.database.acquire() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO flooferboard (user_id, count) VALUES ($1, $2) "
                "ON CONFLICT (user_id) DO UPDATE SET count=flooferboard.count + excluded.count",
                event.sender,
                floof_count,
            )
            for user_id, displayname in mentions.items():
                target_html_parts.append(
                    f'<a href="{MatrixURI.build(user_id).matrix_to_url}">{html.escape(displayname)}</a>'
                )
                await conn.execute(
                    "INSERT INTO floofeeboard (user_id, count) VALUES ($1, $2) "
                    "ON CONFLICT (user_id) DO UPDATE SET count=floofeeboard.count + excluded.count",
                    user_id,
                    per_user_floofs,
                )
                await conn.execute(
                    "INSERT INTO floof (event_id, floofee, floofer, timestamp, count) VALUES ($1, $2, $3, $4, $5)",
                    event.event_id,
                    user_id,
                    event.sender,
                    current_time,
                    per_user_floofs,
                )

        return await event.respond(
            " ".join(target_html_parts) + " " + (self.floof_html * floof_count),
            allow_html=True,
            markdown=False,
            extra_content={
                "m.mentions": {
                    "user_ids": list(mentions.keys()),
                }
            },
        )
