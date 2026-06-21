from typing import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
import html
import time

from maubot import MessageEvent, Plugin
from maubot.handlers import command, event
from mautrix.types import EventID, EventType, MatrixURI, MessageType, UserID
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
        helper.copy("birthdays")
        helper.copy("addicted_users")
        helper.copy("opted_out")
        helper.copy("admins")


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
    birthdays: dict[tuple[int, int], list[UserID]]
    addicted_users: set[UserID]
    opted_out: set[UserID]
    parser = EntityParser()

    async def start(self) -> None:
        self.flood_tracker = {}
        self.on_external_config_update()
        for user in self.addicted_users:
            self._get_bucket(user).count = -15
        for user in self.opted_out:
            self._get_bucket(user)

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
        self.birthdays = {}
        for user_id, (month, day) in self.config["birthdays"].items():
            self.birthdays.setdefault((month, day), []).append(user_id)
        self.addicted_users = set(self.config["addicted_users"])
        self.opted_out = set(self.config["opted_out"])

    def _get_bucket(self, user_id: UserID) -> RateLimitBucket:
        now = time.monotonic()
        try:
            bucket = self.flood_tracker[user_id]
        except KeyError:
            bucket = self.flood_tracker[user_id] = RateLimitBucket(
                user_id=user_id,
                last_timestamp=now,
                count=-299 if user_id in self.opted_out else self.ratelimit_capacity,
            )
        else:
            tokens_to_add = (now - bucket.last_timestamp) * self.ratelimit_rate
            bucket.count = min(self.ratelimit_capacity, bucket.count + tokens_to_add)
            bucket.last_timestamp = now
            if user_id in self.opted_out:
                bucket.count = -299
        return bucket

    def _allow_ratelimit(self, user_id: UserID, tokens_to_use: float) -> bool:
        bucket = self._get_bucket(user_id)
        # This intentionally checks against 1 instead of tokens_to_use: going negative is allowed
        if bucket.count < 1:
            # Overdraft fee
            bucket.count -= 2.5 if user_id in self.addicted_users else 0.25
            return False
        bucket.count -= tokens_to_use
        return True

    @event.on(EventType.ROOM_MESSAGE)
    async def ploo(self, event: MessageEvent) -> None:
        if (
            event.content.msgtype == MessageType.TEXT
            and event.content.body.startswith("?ploo")
            and event.sender not in self.opted_out
        ):
            await event.react("mxc://9f.fi/f")

    @command.new("furrylimit", aliases=["fluffylimit", "flooflimit", "floolimit"])
    @command.argument("unused", pass_raw=True, required=False)
    async def furry_limit(self, event: MessageEvent, unused: str = "") -> None:
        if unused == ":3":
            await event.react(":3")
        bucket = self._get_bucket(event.sender)
        bucket.count -= 1 if event.sender in self.addicted_users else 0.1
        await event.react(f"{bucket.count:.2f}")

    def _make_mention(self, user_id: UserID) -> str:
        return f'<a href="{MatrixURI.build(user_id).matrix_to_url}">{html.escape(user_id)}</a>'

    def _make_floof_list(
        self, items: list[tuple[UserID, int]], own_user_id: UserID
    ) -> Iterable[str]:
        total_floofs = sum(count for _, count in items)
        i = -1
        for user_id, count in items:
            if user_id in self.opted_out:
                continue
            i += 1
            if i > 4 and user_id != own_user_id:
                continue
            strong = strongend = ""
            if user_id == own_user_id:
                strong = "<strong>"
                strongend = "</strong>"
            yield f"<br>{strong}#{i+1}: {self._make_mention(user_id)}: {count} ({count / total_floofs * 100:.1f}%){strongend}</li>"

    def _floof_cost(self, x: int) -> float:
        return max(
            1,
            0.02 * min(x, 300)
            + 0.01 * max(0, min(x, 500) - 300)
            + 0.02 * max(0, min(x, 800) - 500)
            + 0.03 * max(0, min(x, 950) - 800)
            + 0.01 * max(0, x - 950),
        )

    @command.new("floofboars")
    async def floofboars(self, event: MessageEvent) -> None:
        if event.sender not in self.opted_out:
            await event.react("🐗")

    @command.new("floofout")
    async def floofout(self, event: MessageEvent) -> None:
        if event.sender in self.opted_out:
            await event.reply("You have already opted out of floofing")
            return
        elif event.sender == "@kaesa:neoshadow.co":
            await event.reply("The floofy pet mascot can't opt out of floofing")
            return
        self.opted_out.add(event.sender)
        self.config["opted_out"] = list(self.opted_out)
        self.config.save()
        await event.reply("You have opted out of floofing")

    @command.new("floofin")
    async def floofin(self, event: MessageEvent) -> None:
        if event.sender not in self.opted_out:
            await event.reply("You haven't opted out of floofing")
            return
        self._get_bucket(event.sender)  # Reset the bucket count to -99
        self.opted_out.remove(event.sender)
        self.config["opted_out"] = list(self.opted_out)
        self.config.save()
        await event.reply("You have opted back into floofing")

    @command.new("floofreindex")
    async def floofreindex(self, event: MessageEvent) -> None:
        if event.sender not in self.config["admins"]:
            return
        start = time.monotonic()
        async with self.database.acquire() as conn, conn.transaction():
            await conn.execute("DELETE FROM flooferboard")
            await conn.execute("DELETE FROM floofeeboard")
            await conn.execute("""
                INSERT INTO flooferboard (user_id, count)
                SELECT floofer, SUM(count) AS count
                FROM floof
                GROUP BY 1
            """)
            await conn.execute("""
                INSERT INTO floofeeboard (user_id, count)
                SELECT floofee, SUM(count) AS count
                FROM floof
                GROUP BY 1
            """)
        duration = (time.monotonic() - start) * 1000.0
        await event.reply(f"Reindexed floofboard in {duration:.2f} ms")

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

        own_top = [
            "<p>",
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
        if event.sender in self.opted_out:
            own_top = ["<p>You have opted out of floofing</p>"]
        output = [
            "<p>",
            f"<b>Floofees</b> ({len(floofees)} total users)",
            *self._make_floof_list(floofees, event.sender),
            "</p><p>",
            f"<b>Floofers</b> ({len(floofers)} total users)",
            *self._make_floof_list(floofers, event.sender),
            "</p>",
            *own_top,
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

    @command.new("floof")
    @command.argument("floof_count", parser=int, label="floof count")
    @command.argument("target", pass_raw=True, label="targets...")
    async def floof(self, event: MessageEvent, floof_count: int, target: str) -> EventID:
        if not event.content.formatted_body:
            return await event.reply("Floof target users must be specified as @mentions in HTML")
        elif "m.mentions" not in event.content:
            return await event.reply("Using intentional mentions is required when floofing")
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
        elif len(mentions) > 12:
            return await event.reply("You can only floof up to 12 users at a time")
        elif event.sender in mentions:
            return await event.reply("You cannot floof yourself")
        elif floof_count < len(mentions):
            return await event.reply(
                f"You must include at least one floof per recipient ({floof_count} < {len(mentions)})"
            )
        for user_id in list(mentions.keys()):
            if user_id in self.opted_out:
                mentions.pop(user_id)
        if len(mentions) == 0:
            return await event.reply("All of the target users have opted out of being floofed")
        was_encrypted = event.get("mautrix", {}).get("was_encrypted", False)
        limit = 950 if was_encrypted else 1200
        cost_multiplier = 1
        df = datetime.now() - timedelta(hours=6)
        current_date = (df.month, df.day)
        if len(mentions) > 1:
            cost_multiplier = 1.05 ** len(mentions)
        if event.sender in self.addicted_users:
            cost_multiplier *= 1.5
        privileged_senders = self.birthdays.get(current_date, [])
        if any(event.sender == x for x in privileged_senders) or (
            len(mentions) <= len(privileged_senders)
            and all(x in privileged_senders for x in mentions.keys())
        ):
            cost_multiplier = 0.9
        if not self._allow_ratelimit(
            event.sender,
            0.75 if floof_count > limit else (self._floof_cost(floof_count) * cost_multiplier),
        ):
            return await event.react(self.ratelimit_overflow_reaction)
        if floof_count > limit:
            return await event.reply(self.count_overflow_message, allow_html=True, markdown=False)
        target_html_parts = []
        target_text_parts = []
        per_user_floofs = int(floof_count / len(mentions))
        floof_count = per_user_floofs * len(mentions)
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
                target_text_parts.append(displayname)
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

        alt_text = f"{floof_count} floofs"
        if len(mentions) > 1:
            alt_text += f" ({per_user_floofs} per recipient)"
        first_floof_with_alt = " " + self.floof_html[:-1] + f' alt="{alt_text}" >'
        return await event.respond(
            " ".join(target_html_parts)
            + first_floof_with_alt
            + (self.floof_html * (floof_count - 1)),
            allow_html=True,
            markdown=False,
            extra_content={
                "body": f"{alt_text} to {", ".join(target_text_parts)}",
                "m.mentions": {},
            },
        )
