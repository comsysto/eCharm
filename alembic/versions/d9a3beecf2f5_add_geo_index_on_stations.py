"""add geo index on stations

Revision ID: d9a3beecf2f5
Revises: 5b976b2d97af
Create Date: 2023-01-30 10:36:48.353947

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision = 'd9a3beecf2f5'
down_revision = '5b976b2d97af'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    conn.execute(text("""
    CREATE INDEX stations_point_geom_idx
    ON stations 
    USING GIST (point);
    """))

def downgrade():
    conn = op.get_bind()
    conn.execute("DROP INDEX stations_point_geom_idx;")
