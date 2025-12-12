from app import app, db
from app import PaquetePrecio, User # Asegúrate de importar User para crear el admin
from datetime import date
from werkzeug.security import generate_password_hash

def crear_paquetes():
    # 🛑 FIX CRÍTICO: Crear todas las tablas ANTES de hacer cualquier consulta 🛑
    db.create_all() 

    # 1. Crear el usuario Admin si no existe
    # Usamos PaquetePrecio.query.count() como chequeo alternativo para evitar errores si la tabla User estuviera vacía.
    # Pero el chequeo por db.session.get(User, 1) es más directo.
    
    # Intenta obtener el primer usuario.
    # Nota: Si el usuario ya existe, esto no hace nada.
    if not User.query.filter_by(username='admin').first():
        user = User(
            username='admin',
            password=generate_password_hash('09876', method='pbkdf2:sha256'),
            # 🛑 CAMPOS REQUERIDOS PARA EL ROL 🛑
            full_name='Super Administrador Principal',
            email='admin@gumi.com',
            role='SUPERADMIN'  # <-- ¡ASIGNACIÓN CRUCIAL!
        )
        db.session.add(user)
        db.session.commit()
        print(">>> USUARIO SUPERADMIN INICIAL CREADO.")

    # 2. Inicializar Precios
    # Limpiamos solo si queremos que este script reemplace siempre los datos.
    # Si la tabla ya tiene datos, la lógica del if puede ser más compleja,
    # pero para la siembra inicial, PaquetePrecio.query.delete() está bien.
    PaquetePrecio.query.delete() 
    db.session.commit()

    # Descuentos por vigencia
    descuentos = {
        "MENSUAL": 0.00,
        "TRIMESTRAL": 0.10,
        "SEMESTRAL": 0.15,
        "ANUAL": 0.20,
    }

    # Precios Base (el precio de 1 mes)
    base_precios = {
        "MÉXICO": {"Iguana": 650, "Chango": 750, "Elefante": 830, "Abeja": 280, "Clínica": 325},
        "COLOMBIA": {"Iguana": 67500, "Chango": 75000, "Clínica": 35000},
        "LATAM": {"Iguana": 17.5, "Chango": 20, "Clínica": 10},
    }

    monedas = {"MÉXICO": "MXN", "COLOMBIA": "COP", "LATAM": "USD"}
    meses_por_vigencia = {"MENSUAL": 1, "TRIMESTRAL": 3, "SEMESTRAL": 6, "ANUAL": 12}
    hoy = date.today()

    for pais, paquetes in base_precios.items():
        for paquete, base in paquetes.items():
            moneda = monedas[pais]
            
            # Iterar sobre las vigencias PAGADAS
            for vig, desc in descuentos.items():
                meses = meses_por_vigencia[vig]
                
                # Cálculo de precio normal
                precio_normal = base * meses * (1 - desc)
                precio_normal = round(precio_normal / 10) * 10

                # Cálculo de precio con descuento por sucursal
                precio_sucursal = precio_normal * 0.8
                precio_sucursal = round(precio_sucursal / 10) * 10

                # Registrar precio normal
                db.session.add(PaquetePrecio(
                    pais=pais,
                    paquete=paquete,
                    vigencia=vig,
                    precio=precio_normal,
                    moneda=moneda,
                    fecha_vigencia=hoy,
                ))

                # Registrar precio para sucursal
                db.session.add(PaquetePrecio(
                    pais=pais,
                    paquete=paquete + " (Sucursal)",
                    vigencia=vig,
                    precio=precio_sucursal,
                    moneda=moneda,
                    fecha_vigencia=hoy,
                ))

    db.session.commit()
    print("✅ Catálogo de paquetes, precios y descuentos PAGADOS creado correctamente.")


if __name__ == "__main__":
    with app.app_context():
        crear_paquetes()