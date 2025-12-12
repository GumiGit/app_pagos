from app import db, app, Cliente, Suscripcion

# 🔹 Esto fuerza a Flask a registrar todos los modelos antes de crear las tablas
with app.app_context():
    print("Eliminando cualquier base previa...")
    db.drop_all()
    print("Creando nuevas tablas...")
    db.create_all()
    print("✅ Base de datos creada correctamente con todas las tablas.")
