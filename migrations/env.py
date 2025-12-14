import logging
from logging.config import fileConfig

# === CORRECCIÓN DE RUTA Y CONTEXTO ===
import os
import sys
from pathlib import Path

# 1. Ajuste del Path para importar 'app'
MIGRATIONS_DIR = Path(os.path.abspath(os.path.dirname(__file__)))
sys.path.append(str(MIGRATIONS_DIR.parent))

# 2. Importamos la aplicación (debe estar limpia en app.py)
from app import app
# ======================================

from alembic import context
from flask import current_app

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
fileConfig(config.config_file_name)
logger = logging.getLogger('alembic.env')


def run_migrations_offline():
    """Run migrations in 'offline' mode. (Usado para 'alembic upgrade --sql')"""
    # En producción, solo usamos el modo online
    raise NotImplementedError("Offline mode is not used for this deployment.")


def run_migrations_online():
    """Run migrations in 'online' mode."""
    
    # CRÍTICO: Obtenemos la URL de la DB DE Render directamente
    # y configuramos el contexto de Alembic para usar esa URL.
    connectable = current_app.extensions['migrate'].db.engine

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=app.extensions['migrate'].db.metadata,
            render_as_batch=True # Opción recomendada para PostgreSQL/MySQL
        )

        try:
            with context.begin_transaction():
                context.run_migrations()
        except Exception as e:
            logger.error(f"Migration error: {e}")
            raise


if context.is_offline_mode():
    run_migrations_offline()
else:
    # Ejecutamos la migración dentro del contexto de Flask para que current_app exista
    with app.app_context():
        run_migrations_online()
```

### **Paso 39: Despliegue de la Solución Final**
