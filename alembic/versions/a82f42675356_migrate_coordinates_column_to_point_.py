"""migrate coordinates column to point column of geography type

Revision ID: a82f42675356
Revises: d9a3beecf2f5
Create Date: 2023-01-30 10:37:32.971267

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a82f42675356'
down_revision = 'd9a3beecf2f5'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    conn.execute("""
    UPDATE stations 
    SET point = ST_PointFromWkb(coordinates, 4326)::geography;
    """)

    conn.execute("""
        ALTER TABLE stations 
        DROP COLUMN coordinates;
    """)

def downgrade():
    pass
