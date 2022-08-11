"""added columns

Revision ID: 8941b66b24e2
Revises: 9a298d5411a0
Create Date: 2022-01-25 10:39:48.498783

"""
import sqlalchemy as sa

# revision identifiers, used by Alembic.
from geoalchemy2 import Geometry

from alembic import op

revision = "8941b66b24e2"
down_revision = "9a298d5411a0"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("stations", sa.Column("authentication", sa.String(), nullable=True))
    op.add_column(
        "stations",
        sa.Column("coordinates", Geometry(geometry_type="POINT"), nullable=True),
    )
    op.add_column("stations", sa.Column("data_source", sa.String(), nullable=True))
    op.add_column("stations", sa.Column("date_created", sa.Date(), nullable=True))
    op.add_column("stations", sa.Column("date_updated", sa.Date(), nullable=True))
    op.add_column("stations", sa.Column("payment", sa.String(), nullable=True))
    op.add_column("stations", sa.Column("raw_data", sa.String(), nullable=True))
    op.drop_column("stations", "city")
    op.drop_column("stations", "house_number")
    op.drop_column("stations", "street")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "stations",
        sa.Column("street", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "stations",
        sa.Column("house_number", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "stations", sa.Column("city", sa.VARCHAR(), autoincrement=False, nullable=True)
    )
    op.drop_column("stations", "raw_data")
    op.drop_column("stations", "payment")
    op.drop_column("stations", "date_updated")
    op.drop_column("stations", "date_created")
    op.drop_column("stations", "data_source")
    op.drop_column("stations", "coordinates")
    op.drop_column("stations", "authentication")
    # ### end Alembic commands ###