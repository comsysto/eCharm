import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool


current_path = os.path.abspath(".")
sys.path.append(current_path)
from charging_stations_pipelines import settings
from charging_stations_pipelines.models import address, charging, station

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
from charging_stations_pipelines import models
target_metadata = models.Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.
config.set_main_option("sqlalchemy.url", settings.db_uri)


def exclude_tables_from_config(config_):
    tables_ = config_.get("tables", None)
    if tables_ is not None:
        tables = tables_.split(",")
    return tables


# Excluded tables are defined in alembic.ini in section [alembic:exclude], here for PostGIS table spatial_ref_sys
exclude_tables = exclude_tables_from_config(config.get_section('alembic:exclude'))

restrict_tables = [address.Address.__tablename__,
                 charging.Charging.__tablename__,
                 station.Station.__tablename__,
                 station.MergedStationSource.__tablename__]


def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and name in exclude_tables:
        print(f"Not including table {name}, because it's excluded")
        return False
    elif type_ == "table" and settings.restrict_tables and name not in restrict_tables:
        print(f"Not including table {name}, because it's not in strict tables")
        return False
    elif type_ == "table" and settings.restrict_tables and name not in restrict_tables:
        print(f"Not including table {name}, because it's not in strict tables")
        return False
    else:
        return True


def include_name(name, type_, parent_names):
    if type_ == "schema":
        return name in [settings.db_schema]
    else:
        return True


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
        version_table_schema=target_metadata.schema,
        include_schemas=True,
        include_name=include_name,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    alembic_config = config.get_section(config.config_ini_section)
    alembic_config["sqlalchemy.url"] = settings.db_uri
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_object=include_object,
            version_table_schema=target_metadata.schema,
            include_schemas=True,
            include_name=include_name,
        )

        with context.begin_transaction():
            context.execute(f'set search_path to {target_metadata.schema},public')
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
