"""add duplicate station id merged_station_source

Revision ID: 554f653de799
Revises: f3cec738ecd8
Create Date: 2023-07-08 00:48:56.900343

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '554f653de799'
down_revision = 'f3cec738ecd8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("merged_station_source", sa.Column("duplicate_station_id", sa.Integer(), nullable=False))


def downgrade():
    op.drop_column("merged_station_source", "duplicate_station_id")
