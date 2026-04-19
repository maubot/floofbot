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

    def _get_bucket(self, user_id: UserID, save: bool) -> tuple[float, RateLimitBucket]:
        now = time.monotonic()
        bucket = self.flood_tracker[user_id]
        time_passed = now - bucket.last_timestamp
        tokens_to_add = time_passed * self.ratelimit_rate
        count = min(self.ratelimit_capacity, bucket.count + tokens_to_add)
        if save:
            bucket.last_timestamp = now
            bucket.count = count
        return count, bucket

    def _allow_ratelimit(self, user_id: UserID, count: int = 0) -> bool:
        tokens_to_use = 1 + max(count * 0.01, -1)

        if user_id in self.flood_tracker:
            _, bucket = self._get_bucket(user_id, save=True)
            # This intentionally checks against 1 instead of tokens_to_use: going negative is allowed
            if bucket.count < 1:
                return False
            bucket.count -= tokens_to_use
        else:
            self.flood_tracker[user_id] = RateLimitBucket(
                user_id=user_id,
                last_timestamp=time.monotonic(),
                count=self.ratelimit_capacity - tokens_to_use,
            )
        return True

    @command.new("furrylimit")
    async def furry_limit(self, event: MessageEvent) -> None:
        if event.sender in self.flood_tracker:
            count, _ = self._get_bucket(event.sender, save=False)
            await event.react(f"{count:.2f}")
        else:
            await event.react(f"{self.ratelimit_capacity:.2f}")

    def _make_floof_list(
        self, items: list[tuple[UserID, str]], own_user_id: UserID
    ) -> Iterable[str]:
        for i, (user_id, count) in enumerate(items):
            if i > 4 and user_id != own_user_id:
                continue
            strong = strongend = ""
            if user_id == own_user_id:
                strong = "<strong>"
                strongend = "</strong>"
            yield f'<br><strong>#{i+1}:</strong> {strong}<a href="{MatrixURI.build(user_id).matrix_to_url}">{html.escape(user_id)}</a>: {count}{strongend}</li>'

    @command.new("floofboard", aliases=["furryboard", "flooferboard", "floofeeboard"])
    async def floofboard(self, event: MessageEvent) -> None:
        async with self.database.acquire() as conn:
            floofers = await conn.fetch(
                "SELECT user_id, count FROM flooferboard ORDER BY count DESC"
            )
            floofees = await conn.fetch(
                "SELECT user_id, count FROM floofeeboard ORDER BY count DESC"
            )

        output = [
            "<p>",
            f"<b>Floofees</b> ({len(floofees)} total users)",
            *self._make_floof_list(floofees, event.sender),
            "</p><p>",
            f"<b>Floofers</b> ({len(floofers)} total users)",
            *self._make_floof_list(floofers, event.sender),
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

    @command.new("floof")
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
        elif (
            event.sender != "@kaesa:neoshadow.co"
            and event.room_id == "!c10y-QhEklSZsfs-x96D7gFJy1v0b8BSsCapmE5XpAU"
            and "@kaesa:neoshadow.co" not in mentions
        ):
            return await event.reply(
                '<img src="mxc://9f.fi/a" data-mx-emoticon height="32" alt=":neocat_floof:" title=":neocat_floof:"> '
                "Floofing the official Continuwuity pet mascot is required (see <https://forgejo.ellis.link/continuwuation/continuwuity/pulls/1600>)",
                allow_html=True,
            )
        was_encrypted = event.get("mautrix", {}).get("was_encrypted", False)
        limit = 950 if was_encrypted else 1200
        if floof_count > limit:
            if not self._allow_ratelimit(event.sender, count=-50):
                return await event.react(self.ratelimit_overflow_reaction)
            return await event.reply(self.count_overflow_message, allow_html=True, markdown=False)
        if not self._allow_ratelimit(event.sender, floof_count):
            return await event.react(self.ratelimit_overflow_reaction)
        target_html_parts = []
        per_user_floofs = int(floof_count / len(mentions))
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
