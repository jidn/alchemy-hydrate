"""Hydrate database from CSV files for sqlalchemy.declaration_base.

Hydration assume the tables and those with foreign keys into those tables
have been cleared.  We then dependant data before data which debends on it.

Example:

    hydrate_all_with_sync_connection(
        Path(__file__).parent.parent / "models/csv"),
        get_connection()   # use synchronous version not async
    )

    This will wrap all the table clears and inserts into a transaction even
    if there are commits throughout the process.
"""

import itertools
import logging
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import Any

from alchemy_hydrate import LowerCaseDictReader, TransformData
from sqlalchemy import Connection, Table, func, insert, text
from sqlalchemy.orm import Session

from malta.db.clear import clear_all_tables, clear_tables
from malta.models.base import Base

log = logging.getLogger("db.hydrate")


def reset_postgresql_sequence(session, table: Table):
    """Resets the sequence for a given model in PostgreSQL."""
    table_name = table.name
    pk_column_name = table.primary_key.columns[0].name
    sequence_name = f"{table_name}_{pk_column_name}_seq"

    max_id = session.execute(func.max(sequence_name)).scalar()

    if max_id is not None:
        session.execute(
            text(f"SELECT setval('{sequence_name}', :max_id, true)"), {"max_id": max_id}
        )
        session.commit()
        log.info("PostgreSQL sequence '%s' reset to %s.", sequence_name, max_id)


def reset_mssql_identity(session, table: Table):
    """Reseeds the IDENTITY for a given model in MSSQL."""
    table_name = table.name
    max_id = session.execute(func.max(table.primary_key.columns[0])).scalar()

    if max_id is not None:
        session.execute(
            text(f"DBCC CHECKIDENT ('{table_name}', RESEED, :max_id)"),
            {"max_id": max_id},
        )
        session.commit()
        log.info(
            "MSSQL IDENTITY for table 'table_name' reseeded to %s.", table_name, max_id
        )
    else:
        # If the table is empty, you might want to reseed to 0 or 1 depending on your preference.
        # Here we reseed to 0, so the first entry will be 1.
        session.execute(text(f"DBCC CHECKIDENT ('{table_name}', RESEED, 0)"))
        session.commit()
        print("MSSQL IDENTITY for empty table '%s' reseeded to 0.", table_name)


def reset_sqlite_sequence(session, table: Table):
    """Resets the autoincrement counter for a given model in SQLite."""
    if False:
        table_name = table.name
        # pk_column_name = model.__mapper__.primary_key[0].name
        # max_id = session.execute(func.max(getattr(table, pk_column_name))).scalar()
        max_id = session.execute(func.max(table.primary_key.columns[0])).scalar()

        if max_id is not None:
            try:
                session.execute(
                    text(
                        "UPDATE sqlite_sequence SET seq = :max_id WHERE name = :table_name"
                    ),
                    {"max_id": max_id, "table_name": table_name},
                )
                session.commit()
                log.info(
                    "SQLite sequence for table '%s' reset to %s.", table_name, max_id
                )
            except Exception as e:
                # This can fail if the table name is not in sqlite_sequence.
                # This happens if no rows have ever been deleted, or if AUTOINCREMENT was not specified.
                # In such cases, SQLite handles it automatically, but we'll print a notice.
                session.rollback()
                log.error(
                    f"Could not update sqlite_sequence for '{table_name}'. This is often okay. Error: %s",
                    table_name,
                    e,
                )


def reset_autoincrement(session, table: Table):
    """
    Detects the dialect from the session and calls the correct
    autoincrement reset function for the given model.
    """

    try:
        dialect_name = session.bind.dialect.name
    except AttributeError:
        print("Error: The session is not bound to an engine.")
        return

    dispatcher = {
        "postgresql": reset_postgresql_sequence,
        "mssql": reset_mssql_identity,
        "sqlite": reset_sqlite_sequence,
    }

    reset_function = dispatcher.get(dialect_name)

    if reset_function:
        reset_function(session, table)
    else:
        raise SystemExit(
            f"Autoincrement reset for dialect '{dialect_name}' is not supported."
        )


def transform_csv_file(table: Table, path: Path) -> list[Any]:
    """Create instances from CSV.

    Args:
        table: for mapping CSV data.
        path: of CSV file.

    Returns:
        List of model instances.

    Raises:
        ValueError: Converting CSV values to Model types.
    """

    data = []
    transform = TransformData(table)

    with open(path) as csv_file:
        reader = LowerCaseDictReader(csv_file)
        transform_column_names = {_.name for _ in transform}
        # CSV column headers should be in transform
        headers = set(getattr(reader, "fieldnames", []))
        if unknown := headers.difference(transform_column_names):
            log.warning("%s has unexpected headers: %s", path.name, unknown)

        for row in reader:
            try:
                table_data = transform(row)
                data.append(table_data)

            except ValueError as ex:
                log.error("ERROR %s %s: %s", table.name, reader.line_num, ex)
                raise ValueError(f"{path.name}:{reader.line_num}  {ex}")
            except Exception as ex:
                # We are just going to log the error and continue.  We know
                # this didn't work; but show all the errors without stacktrace.
                log.exception(ex)
                print(ex)

    return data


def hydrate_csv_file(
    session: Session, table: Table, path: Path, in_groups_of: int = 100
) -> int:
    """Hydrate the given model with the CSV.

    Args:
        session: for database connection.
        table: for mapping CSV data.
        path: of CSV file.
        in_groups_of: The number of models instance to commit at a time.

    Returns:
        The number of instances committed to the database.

    Raises:
        ValueError: Converting CSV values to Model types.
        Various database errors.
    """

    count: int = 0
    log.debug("BEGIN %s from %s", table.name, path.name)

    data = transform_csv_file(table, path)

    insert_stmt = table.insert()
    for chunk in itertools.batched(data, in_groups_of, strict=False):
        try:
            session.execute(insert_stmt, chunk)
        except Exception as ex:
            log.error(ex.with_traceback(None))
            raise SystemExit(ex)
        count += len(chunk)
        session.commit()
    log.debug("ENDED %s with %d rows", table.name, count)
    reset_autoincrement(session, table)
    return count


def hydrate_csv_directory(table_names: list[str], directory: Path, session: Session):
    """Hydrate CSVs in a directory matching table names.

    Args:
        table_names: The table names to process.
        directory: of CSV files.
        session: for commiting data.

    Raises:
        ValueError: when unable to create instance for insert into DB.
        Various DB errors.
    """
    # A lookup dict[table_name:str, absolute_path: Path]
    csvs: dict[str, Path] = {_.stem.lower(): _ for _ in get_csv_files(directory)}
    tables = table_lookup()

    association_tables = []
    # Use SQLAlchemys ordering to prevent inserting data when the foreign
    # keys doen't even exist yet.
    for table_name in table_names:
        if table_name in csvs:
            if "__" in table_name:
                association_tables.append(table_name)
                continue

            elif table_name in tables:
                hydrate_csv_file(session, tables[table_name], csvs.pop(table_name))

    for table_name in association_tables:
        hydrate_csv_file(session, tables[table_name], csvs.pop(table_name))

    log.debug("CSVs not processed: %s", sorted(csvs.keys()))


def hydrate_with_sync_conn(sync_conn: Connection, table_names: list[str]):
    """Hydrate CSV directory with synchonous DB connection.

    This will clear the tables with found CSV and roll back transaction if
    there is an Exception in the process.

    Args:
        sync_connection: for use with the DB.

    Raises:
        ValueError: on CSV invalid conversion to Model instance.
    """
    # TODO: wrap this all up in a save point transaction for rollback
    # on any error.
    session = Session(sync_conn)
    log.debug("Begin sync session %s", session)
    hydrate_csv_directory(
        table_names,
        # TODO: path should be in config settings
        Path(__file__).parent.parent / "models/csv",
        session,
    )
    session.commit()
    log.debug("Ended sync session %s", session)


async def hydrate_tables(table_names: list[str], clear_all: bool):
    """Initalize the database tables."""
    from malta.db import get_connection

    async with get_connection() as conn:
        # TODO: allow for transaction rollback if it fails
        if clear_all:
            await conn.run_sync(clear_all_tables)
        else:
            await conn.run_sync(clear_tables, table_names)
        await conn.run_sync(hydrate_with_sync_conn, table_names)


def get_csv_files(directory: str | Path) -> Generator[Path]:
    """Generate absolute file paths of CSV files (case-insensitive).

    Args:
        directory: where CSVs are located.

    Yields:
        An absolute path of a CSV file found in the directory.
    """
    directory_path = Path(directory).resolve()
    for item in directory_path.iterdir():
        if item.is_file and item.suffix.lower() == ".csv":
            yield item.resolve()


def model_lookup() -> dict[str, Any]:
    return {
        getattr(v, "__tablename__", "unknown"): v
        # Key is unused class name like 'ActionSet'
        for _, v in Base.registry._class_registry.items()
        if hasattr(v, "__tablename__")
    }


def table_lookup() -> dict[str, Table]:
    return {table.name: table for table in Base.metadata.sorted_tables}


def get_table_order(only: Iterable[str] = []) -> list[str]:
    """Get the tables in the order they should by hydrated.

    Dependicies first, ie ActionSet before ActionSet.

    Args:
        only: give me tables from iterable.

    Returns:
        List of table names.
    """
    return [t.name for t in Base.metadata.sorted_tables if not only or t.name in only]


if __name__ == "__main__":
    """Clear all table and hydrate."""
    import asyncio
    import pprint
    from argparse import ArgumentParser

    logging.basicConfig(level=logging.DEBUG)

    cmd_line = ArgumentParser()
    cmd_line.add_argument(
        "-t", dest="tables", metavar="TABLE", action="append", help="Table"
    )
    cmd_line.add_argument(
        "--clear-picked", action="store_true", help="Clear given tables."
    )
    cmd_line.add_argument("-n", "--dry-run", action="store_true", help="No database")
    args = cmd_line.parse_args()

    if not args.tables:
        args.tables = get_table_order()

    if args.dry_run:
        models = model_lookup()
        example = get_table_order(args.tables)
        for name in get_table_order(args.tables):
            csv = (Path(__file__).parent.parent / f"models/csv/{name}.csv").resolve()
            data = transform_csv_file(models[name], csv)
            pprint.pp(data, width=120)
    else:
        asyncio.run(hydrate_tables(args.tables, not args.clear_picked))
