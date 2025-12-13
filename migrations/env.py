import logging
from logging.config import fileConfig

# === CORRECCIÓN DE RUTA Y CONTEXTO ===
# === FORZANDO EL COMMIT FINAL ===
# El archivo está limpio de sintaxis.
import os
import sys
from pathlib import Path

# Añade la ruta del directorio padre (raíz del proyecto)
MIGRATIONS_DIR = Path(os.path.abspath(os.path.dirname(__file__)))
sys.path.append(str(MIGRATIONS_DIR.parent))

# Importamos la aplicación *después* de corregir el path
from app import app, db
from flask_migrate import Migrate
# ======================================

from flask import current_app
from alembic import context

config = context.config

fileConfig(config.config_file_name)
logger = logging.getLogger('alembic.env')

# NOTA: get_engine() y get_engine_url() AHORA SOLO SE LLAMAN DENTRO DEL app_context()


def get_engine():
    try:
        # this works with Flask-SQLAlchemy<3 and Alchemical
        return current_app.extensions['migrate'].db.get_engine()
    except (TypeError, AttributeError):
        # this works with Flask-SQLAlchemy>=3
        return current_app.extensions['migrate'].db.engine


def get_engine_url():
    # Esta función ahora se llama dentro del app_context, por lo que current_app existe.
    try:
        return get_engine().url.render_as_string(hide_password=False).replace(
            '%', '%%')
    except AttributeError:
        return str(get_engine().url).replace('%', '%%')


def get_metadata(target_db):
    if hasattr(target_db, 'metadatas'):
        return target_db.metadatas[None]
    return target_db.metadata


def run_migrations_offline():
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url, target_metadata=get_metadata(db), literal_binds=True
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.
    
    CRÍTICO: Ejecutamos toda la configuración aquí, dentro del app_context.
    """

    # 1. Configurar la URL de conexión dentro del contexto de Flask
    target_db = current_app.extensions['migrate'].db
    config.set_main_option('sqlalchemy.url', get_engine_url())

    # 2. Configuración de directivas (como ya estaba)
    def process_revision_directives(context, revision, directives):
        if getattr(config.cmd_opts, 'autogenerate', False):
            script = directives[0]
            if script.upgrade_ops.is_empty():
                directives[:] = []
                logger.info('No changes in schema detected.')

    conf_args = current_app.extensions['migrate'].configure_args
    if conf_args.get("process_revision_directives") is None:
        conf_args["process_revision_directives"] = process_revision_directives

    # 3. Conectar y migrar
    connectable = get_engine()

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=get_metadata(target_db),
            **conf_args
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    # Este contexto envuelve toda la lógica de Flask/DB
    with app.app_context():
        run_migrations_online()
```

