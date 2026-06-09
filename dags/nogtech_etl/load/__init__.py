"""Etapa de carga da ETL NogTech."""

from nogtech_etl.load.postgres import load_to_postgres

__all__ = ["load_to_postgres"]
