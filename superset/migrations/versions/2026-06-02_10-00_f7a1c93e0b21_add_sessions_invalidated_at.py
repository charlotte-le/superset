# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""add sessions_invalidated_at to user_attribute

Revision ID: f7a1c93e0b21
Revises: 31dae2559c05
Create Date: 2026-06-02 10:00:00.000000

"""

from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

from superset.migrations.shared.utils import (
    add_columns,
    create_index,
    drop_columns,
    drop_index,
)

# revision identifiers, used by Alembic.
revision = "f7a1c93e0b21"
down_revision = "31dae2559c05"

TABLE = "user_attribute"
COLUMN = "sessions_invalidated_at"
INDEX = "ix_user_attribute_sessions_invalidated_at"
UQ = "uq_user_attribute_user_id"


MERGE_COLUMNS = ("avatar_url", "welcome_dashboard_id", "sessions_invalidated_at")

# Lightweight table stub used to build the data-migration statements as SQLAlchemy
# expressions, so no SQL is assembled through string formatting.
USER_ATTRIBUTE = sa.table(
    TABLE,
    sa.column("id"),
    sa.column("user_id"),
    sa.column("created_on"),
    sa.column("changed_on"),
    *(sa.column(name) for name in MERGE_COLUMNS),
)


def upgrade():
    add_columns(TABLE, sa.Column(COLUMN, sa.DateTime(), nullable=True))
    create_index(TABLE, INDEX, [COLUMN])
    # The model treats ``user_attribute`` as one row per user (all readers use
    # ``extra_attributes[0]``). Enforce that invariant so the session-invalidation
    # upsert is race-safe. Collapse any pre-existing duplicates into the lowest
    # ``id`` per user before adding the unique constraint.
    #
    # The merge/de-dup is driven in Python rather than via correlated subqueries:
    # MySQL forbids referencing the UPDATE/DELETE target table in a subquery
    # (error 1093), so a portable SQL form is awkward. Reading the duplicate rows
    # and resolving the winner in Python works identically on SQLite/MySQL/Postgres.
    _dedupe_user_attributes()
    # SQLite has no ALTER ... ADD CONSTRAINT, so use batch mode (copy-and-move)
    # which all dialects support.
    with op.batch_alter_table(TABLE) as batch_op:
        batch_op.create_unique_constraint(UQ, ["user_id"])
    # Backfill an epoch for accounts that are already disabled at upgrade time.
    # Without this, a user disabled before this lands and later re-enabled would
    # have no epoch, so a pre-existing session (which also carries no recorded
    # login time) would silently revive. Stamping the epoch now means any such
    # outstanding session is treated as invalidated.
    _backfill_disabled_users()


def _dedupe_user_attributes():
    """Collapse duplicate ``user_attribute`` rows into the lowest ``id`` per user.

    Settings stored only on a higher-``id`` duplicate are merged forward into the
    kept row (the kept row's NULL columns take the lowest-``id`` non-NULL sibling
    value) so nothing is silently lost, then the redundant rows are deleted.
    """
    bind = op.get_bind()
    rows = bind.execute(
        sa.select(
            USER_ATTRIBUTE.c.id,
            USER_ATTRIBUTE.c.user_id,
            *(USER_ATTRIBUTE.c[name] for name in MERGE_COLUMNS),
        ).order_by(USER_ATTRIBUTE.c.id)
    ).fetchall()

    by_user: dict[int, list] = {}
    for row in rows:
        mapping = row._mapping
        if mapping["user_id"] is None:
            continue
        by_user.setdefault(mapping["user_id"], []).append(mapping)

    for user_rows in by_user.values():
        if len(user_rows) < 2:
            continue
        # Rows are ordered by id, so the first is the keeper.
        keeper = user_rows[0]
        duplicates = user_rows[1:]
        updates = {}
        for column in MERGE_COLUMNS:
            if keeper[column] is not None:
                continue
            for dup in duplicates:
                if dup[column] is not None:
                    updates[column] = dup[column]
                    break
        if updates:
            bind.execute(
                USER_ATTRIBUTE.update()
                .where(USER_ATTRIBUTE.c.id == keeper["id"])
                .values(updates)
            )
        bind.execute(
            USER_ATTRIBUTE.delete().where(
                USER_ATTRIBUTE.c.id.in_([dup["id"] for dup in duplicates])
            )
        )


def _backfill_disabled_users():
    """Stamp the invalidation epoch for users that are already disabled.

    Upserts one ``user_attribute`` row per disabled user (``ab_user.active`` is
    false) that has no epoch yet, so re-enabling such an account does not revive
    a session that predates this feature. Done in Python to stay portable across
    SQLite/MySQL/Postgres; the unique constraint added above guarantees one row
    per user, so the insert path cannot create duplicates.
    """
    bind = op.get_bind()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # FAB stores ``active`` as a boolean/tinyint; some legacy rows leave it NULL,
    # which is not "explicitly disabled", so only match a false value.
    disabled_user_ids = [
        row._mapping["id"]
        for row in bind.execute(
            sa.text("SELECT id FROM ab_user WHERE active = :inactive"),
            {"inactive": False},
        ).fetchall()
    ]
    if not disabled_user_ids:
        return

    existing = {
        row._mapping["user_id"]
        for row in bind.execute(sa.select(USER_ATTRIBUTE.c.user_id)).fetchall()
    }

    for user_id in disabled_user_ids:
        if user_id in existing:
            bind.execute(
                USER_ATTRIBUTE.update()
                .where(
                    USER_ATTRIBUTE.c.user_id == user_id,
                    USER_ATTRIBUTE.c[COLUMN].is_(None),
                )
                .values({COLUMN: now, "changed_on": now})
            )
        else:
            bind.execute(
                USER_ATTRIBUTE.insert().values(
                    {
                        "user_id": user_id,
                        COLUMN: now,
                        "created_on": now,
                        "changed_on": now,
                    }
                )
            )


def downgrade():
    with op.batch_alter_table(TABLE) as batch_op:
        batch_op.drop_constraint(UQ, type_="unique")
    drop_index(TABLE, INDEX)
    drop_columns(TABLE, COLUMN)
