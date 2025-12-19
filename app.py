from flask import Flask, render_template, request, redirect, url_for, send_file, flash, jsonify, current_app
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.utils import secure_filename
from flask_migrate import Migrate

# =========== INICIO DE IMPORTS EXTERNOS Y DE SISTEMA ===========
import io
import traceback
import re 
from datetime import date, datetime, timedelta 
from dateutil.relativedelta import relativedelta
import calendar
import os

# Librerías de terceros
import pandas as pd
from decimal import Decimal 
import locale # <-- Importación del módulo de localización

# Librerías de seguridad y base de datos
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import or_, extract, func, desc, and_, distinct, BigInteger # BigInteger añadido aquí

# Forms (WTForms)
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, SelectField
from wtforms.validators import DataRequired, Email, Length, Optional

# Configuración de Logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Inicialización de la aplicación
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'un_secreto_muy_largo_y_dificil_de_adivinar_para_la_sesion') 

# Configuración del Locale
try:
    locale.setlocale(locale.LC_ALL, 'es_MX.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'es_ES.UTF8')
    except locale.Error:
        print("Advertencia: No se pudo configurar locale.LC_ALL para español.")

# ... El resto de tu configuración y modelos ...

# Función de utilidad para formatear moneda (usando Decimal)
def format_currency(value, moneda='MXN'):
    if value is None:
        return f"{moneda} 0.00"
    try:
        # Asegura que sea Decimal para la precisión
        value = Decimal(value)
        # Usamos el símbolo y el código de moneda para la representación
        if moneda == 'MXN':
            return locale.currency(value, symbol='$', grouping=True)
        elif moneda == 'COP':
            return f"COP {value:,.2f}"
        else: # USD u otros
            return f"{moneda} {value:,.2f}"
    except Exception:
        return f"{moneda} 0.00"

app.jinja_env.globals.update(format_currency=format_currency)


# 🔹 Configuración de base de datos (guarda database.db en la raíz del proyecto)
basedir = os.path.abspath(os.path.dirname(__file__))


# 🔹 Configuración de base de datos
# 1. Usa la variable de entorno DATABASE_URL (para Render/PostgreSQL)
# 2. Si no existe (entorno local), usa SQLite ('sqlite:///database.db')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL') or 'sqlite:///' + os.path.join(basedir, 'database.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False 

# La línea 'basedir = os.path.abspath(os.path.dirname(__file__))' que ya tenías
# puede ser movida o dejada, pero es menos crítica ahora que priorizamos la URL de entorno.

# 🛑 1. DEFINICIÓN DE DB (PRIMERO)
db = SQLAlchemy(app)
migrate = Migrate(app, db, render_as_batch=True, compare_type=True)

# ========== Login ==========
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# 🛑 2. DEFINICIÓN DE TODOS LOS MODELOS (SEGUNDO)
# ========== Modelos ==========
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False) 
    full_name = db.Column(db.String(150), nullable=True)
    email = db.Column(db.String(150), nullable=True)
    role = db.Column(db.String(20), default='LECTOR', nullable=False)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password, method='pbkdf2:sha256')
    
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

# NUEVO MODELO: Transacciones Bancarias (Para conciliación)
# 🛑 CRÍTICO: Esta clase ahora existe antes de que se use en las rutas
class BankTransaction(db.Model):
    # CRÍTICO: Cambiado a BigInteger para soportar IDs negativos grandes (timestamps)
    id = db.Column(db.BigInteger, primary_key=True) 
    date = db.Column(db.Date, nullable=False)
    concept = db.Column(db.String(255), nullable=False)
    debit = db.Column(db.Numeric(10, 2), nullable=True) 
    credit = db.Column(db.Numeric(10, 2), nullable=True)
    total_balance = db.Column(db.Numeric(10, 2), nullable=True)
    is_conciliated = db.Column(db.Boolean, default=False)
    
    # 🛑 NUEVAS COLUMNAS para Pre-Conciliación Sucursal (NO se usaba Pago en la conciliacion_list)
    status = db.Column(db.String(50), default='PENDIENTE')
    negocio_conciliado = db.Column(db.String(255), nullable=True) # Guarda nombres de negocios/sucursales
    num_factura_conciliado = db.Column(db.String(50), nullable=True) # Guarda el número de factura

class Pago(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(100), nullable=False)
    correo = db.Column(db.String(100), nullable=False)
    monto = db.Column(db.Numeric(10, 2), nullable=False) 
    numero_whatsapp = db.Column(db.String(20), nullable=True)
    whatsapp_enviado = db.Column(db.DateTime, nullable=True)
    cliente_id = db.Column(db.Integer, db.ForeignKey('cliente.id'), nullable=True)
    fecha_pago = db.Column(db.Date, nullable=False, default=date.today)
    metodo_pago = db.Column(db.String(50), nullable=True)
    otro_metodo_pago = db.Column(db.String(100), nullable=True)
    factura_pago = db.Column(db.Boolean, default=False)
    numero_factura = db.Column(db.String(50), nullable=True)
    paquete = db.Column(db.String(100), nullable=True)
    vigencia = db.Column(db.String(20), nullable=True)
    motivo_descuento = db.Column(db.String(150), nullable=True)
    moneda = db.Column(db.String(5), nullable=True)
    status = db.Column(db.String(20), default='ACTIVO', nullable=False)
    
    paquete_precio_id = db.Column(db.Integer, db.ForeignKey('paquete_precio.id'), nullable=True)
    paquete_precio = db.relationship('PaquetePrecio', backref='pagos_detalle', lazy=True)

    # Vínculo con la Transacción Bancaria para Conciliación
    # Ahora que BankTransaction.id es BigInteger, esta clave foránea funciona correctamente
    bank_transaction_id = db.Column(db.BigInteger, db.ForeignKey('bank_transaction.id'), nullable=True, unique=True)
    bank_transaction = db.relationship('BankTransaction', backref='pago', uselist=False)

class Cliente(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    negocio = db.Column(db.String(150), nullable=False) # <- CRÍTICO: Este campo debe existir.
    nombre_contacto = db.Column(db.String(120), nullable=False)
    mail = db.Column(db.String(120), nullable=False)
    telefono = db.Column(db.String(30), nullable=False)
    telefono_secundario_1 = db.Column(db.String(30), nullable=True)
    telefono_secundario_2 = db.Column(db.String(30), nullable=True)
    telefono_secundario_3 = db.Column(db.String(30), nullable=True)
    pais = db.Column(db.String(50), nullable=False) # MÉXICO|COLOMBIA|LATAM
    localidad = db.Column(db.String(120), nullable=True)
    status_cliente = db.Column(db.String(20), default='Activo')

    # Datos fiscales
    requiere_factura = db.Column(db.Boolean, default=False)
    razon_social = db.Column(db.String(200), nullable=True)
    rfc = db.Column(db.String(20), nullable=True)
    codigo_postal = db.Column(db.String(10), nullable=True)
    regimen_fiscal = db.Column(db.String(4), nullable=True) # p.ej. "601"
    uso_cfdi = db.Column(db.String(4), nullable=True) # p.ej. "G03"
    mail_facturas = db.Column(db.String(120), nullable=True)

    # Relación con pagos (una sola vez)
    pagos = db.relationship('Pago', backref='cliente', lazy=True)

    # Campos de “primer pago” capturados en el alta
    fecha_pago = db.Column(db.Date)
    metodo_pago = db.Column(db.String(50))
    otro_metodo_pago = db.Column(db.String(100))
    factura_pago = db.Column(db.Boolean, default=False)
    numero_factura = db.Column(db.String(50))
    motivo_descuento = db.Column(db.String(150), nullable=True)

class Suscripcion(db.Model):
    id = db.Column(db.Integer, primary_key=True)

    # vínculo con cliente
    cliente_id = db.Column(db.Integer, db.ForeignKey('cliente.id'), nullable=False)
    cliente = db.relationship(
        'Cliente',
        foreign_keys=[cliente_id],
        backref=db.backref('suscripcion', uselist=False)
    )

    # campos de negocio
    id_gumi = db.Column(db.String(50), nullable=True) # 🔸 CRÍTICO: Este campo faltaba.
    status = db.Column(db.String(20), nullable=False) # Activo | Suspendido | Eliminado | En prueba
    server = db.Column(db.String(100), nullable=False)
    fecha_inicio = db.Column(db.Date, nullable=False)
    paquete = db.Column(db.String(100), nullable=False)
    vigencia = db.Column(db.String(20), nullable=False)
    vence_en = db.Column(db.Date, nullable=True)
    proximo_pago = db.Column(db.Date, nullable=True)

    # matriz / sucursal
    es_sucursal = db.Column(db.Boolean, default=False)
    matriz_id = db.Column(db.Integer, db.ForeignKey('cliente.id'), nullable=True)
    matriz = db.relationship('Cliente', foreign_keys=[matriz_id], backref='sucursales')

    observaciones = db.Column(db.Text, nullable=True)

class PaquetePrecio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pais = db.Column(db.String(50), nullable=False)
    paquete = db.Column(db.String(50), nullable=False)
    vigencia = db.Column(db.String(20), nullable=False)
    precio = db.Column(db.Numeric(10, 2), nullable=False) 
    moneda = db.Column(db.String(10), nullable=False)
    fecha_vigencia = db.Column(db.Date, nullable=False)
    
    name = db.Column(db.String(150), nullable=True) 
    duration_months = db.Column(db.Integer, nullable=True)
    is_active = db.Column(db.Boolean, default=True) 

    def __repr__(self):
        return f"<Paquete {self.paquete} - {self.pais} ({self.vigencia})>"

# ========== Fin de Modelos ==========


# 🛑 3. RESTO DE FUNCIONES Y RUTAS (TERCERO)

# =======================================================
# DECORADOR DE ROLES Y PERMISOS
# =======================================================
from functools import wraps
from flask import abort

def role_required(allowed_roles):
    """
    Restringe el acceso a una ruta solo a los roles especificados.
    Los roles deben ser una lista o tupla, e.g., ['SUPERADMIN', 'ADMIN'].
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                # Flask-Login ya manejaría esto, pero es una capa de seguridad
                return login_manager.unauthorized()
            
            user_role = (current_user.role or 'LECTOR').upper()

            if user_role not in allowed_roles:
                flash(f"Acceso denegado: Se requiere uno de los siguientes roles: {', '.join(allowed_roles)}", 'danger')
                # 403 Forbidden para rutas API o abortar para vistas
                if request.path.startswith('/api/'):
                    return jsonify({"ok": False, "error": "Acceso denegado por permisos."}), 403
                else:
                    # Lo enviamos al dashboard o a la lista de clientes por defecto
                    return redirect(url_for('dashboard') or url_for('clientes_list'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# 🟢 DEFINICIÓN DE ROLES (Asegura consistencia)
ROLES_SUPERADMIN = ['SUPERADMIN'] 
ROLES_ADMIN = ['ADMIN']
ROLES_MODIFICACION = ['SUPERADMIN', 'ADMIN'] 
ROLES_LECTURA = ['SUPERADMIN', 'ADMIN', 'LECTOR']
ROLES_ADMIN_CRUD = ROLES_MODIFICACION # Usar ROLES_MODIFICACION para CRUD


# ===== Helpers de calendario / vigencia =====
def formatear_fecha_jinja(value):
    """Filtro Jinja para formatear objetos date o datetime."""
    if not value:
        return '—'
    
    # Intenta convertir a objeto date si es necesario (maneja cadenas si vienen)
    if isinstance(value, str):
        try:
            value = date.fromisoformat(value)
        except ValueError:
            return '—' # No es un formato de fecha válido
            
    # Formato deseado: día/mes/año (ej: 19/11/25)
    return value.strftime('%d/%m/%y')    

app.jinja_env.filters['formatearFecha'] = formatear_fecha_jinja

def primer_dia_mes(d: date) -> date:
    return d.replace(day=1)

def ultimo_dia_mes(d: date) -> date:
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.replace(day=last_day)

# 🛑 FUNCIÓN CRÍTICA DE CÁLCULO DE VIGENCIA (YA INCLUYE DEMO DE 15 DÍAS)
def calcular_fechas_vigencia(fecha_inicio: date, vigencia: str) -> tuple[date, date]:
    """
    Calcula fechas respetando las dos lógicas de negocio:
    1. DEMO: 15 días naturales exactos. Pago al día siguiente.
    2. PERIODOS (Mensual, Trimestral, etc): Alinear a FIN DE MES calendario.
    """
    vig = (vigencia or "").upper().strip()
    
    if not fecha_inicio:
        fecha_inicio = date.today()

    # --- CASO 1: DEMO (15 Días Naturales) ---
    # Prioridad absoluta: si dice DEMO, son 15 días.
    if "DEMO" in vig:
        vence_en = fecha_inicio + timedelta(days=15)
        proximo_pago = vence_en + timedelta(days=1)
        return vence_en, proximo_pago

    # --- CASO 2: PERIODOS ALINEADOS A FIN DE MES ---
    meses_map = {
        "MENSUAL": 1, 
        "TRIMESTRAL": 3, 
        "SEMESTRAL": 6, 
        "ANUAL": 12
    }
    duracion = meses_map.get(vig, 1)

    # Lógica: Mes Inicio + (Duración - 1) -> Fin de ese mes
    fecha_objetivo = fecha_inicio + relativedelta(months=duracion - 1)
    ultimo_dia_mes = calendar.monthrange(fecha_objetivo.year, fecha_objetivo.month)[1]
    
    vence_en = date(fecha_objetivo.year, fecha_objetivo.month, ultimo_dia_mes)
    proximo_pago = vence_en + timedelta(days=1)
    
    return vence_en, proximo_pago

# app.py (Junto a las funciones de calendario)

def recalcular_vigencia_cliente(cliente_id):
    """
    Reconstruye la historia del cliente ignorando pagos cancelados.
    """
    from datetime import date
    
    # 1. Identificar la entidad (usamos Suscripcion como fuente de la verdad)
    suscripcion = Suscripcion.query.filter_by(cliente_id=cliente_id).first()
    cliente = Cliente.query.get(cliente_id)

    if not suscripcion or not cliente:
        return False

    # 2. Obtener pagos activos ordenados
    all_pagos = Pago.query.filter_by(cliente_id=cliente_id).order_by(Pago.fecha_pago.asc()).all()
    pagos_activos = [p for p in all_pagos if (getattr(p, 'status', '') or '').upper() != 'CANCELADO']

    # 3. Variables para el recálculo
    acumulado_vence_en = None
    acumulado_proximo_pago = None
    
    ultimo_paquete = None
    ultima_vigencia = None
    
    if not pagos_activos:
        # Reset total si no hay pagos activos
        suscripcion.vence_en = None
        suscripcion.proximo_pago = None
        suscripcion.status = 'En prueba'
        cliente.fecha_pago = None
        db.session.commit()
        return True

    for pago in pagos_activos:
        fecha_pago_actual = pago.fecha_pago
        
        # Determinar inicio del periodo (Regla de negocio: continuidad vs hueco)
        if acumulado_vence_en is None:
            fecha_inicio_calculo = fecha_pago_actual
        else:
            if fecha_pago_actual > acumulado_vence_en:
                fecha_inicio_calculo = fecha_pago_actual
            else:
                fecha_inicio_calculo = acumulado_vence_en
        
        # 4. Calculamos y asignamos AMBOS valores
        if pago.vigencia:
            nuevo_vence, nuevo_proximo = calcular_fechas_vigencia(fecha_inicio_calculo, pago.vigencia)
        else:
            # Fallback si el pago no tiene vigencia (debería tenerla)
            nuevo_vence, nuevo_proximo = acumulado_vence_en or fecha_pago_actual, acumulado_proximo_pago or fecha_pago_actual
            
        acumulado_vence_en = nuevo_vence
        acumulado_proximo_pago = nuevo_proximo
        
        # Datos informativos del último pago
        ultimo_paquete = pago.paquete
        ultima_vigencia = pago.vigencia

    # 5. Asignar resultados a la entidad Suscripcion
    suscripcion.vence_en = acumulado_vence_en
    suscripcion.proximo_pago = acumulado_proximo_pago 
    
    if ultimo_paquete: suscripcion.paquete = ultimo_paquete
    if ultima_vigencia: suscripcion.vigencia = ultima_vigencia
    
    # 6. Status
    hoy = date.today()
    if acumulado_vence_en and acumulado_vence_en >= hoy:
        suscripcion.status = 'Activo'
    else:
        suscripcion.status = 'Suspendido'

    db.session.commit()
    return True

def calcular_status_pago(proximo_pago: date, today: date) -> dict:
    """
    Indica si la suscripción está VIGENTE, VENCIDA o SIN PAGO.
    """
    # 1. SIN PAGO
    if not proximo_pago:
        return {"status": "SIN PAGO", "color": "bg-secondary"}

    # 2. VIGENTE O VENCIDA
    dias = (proximo_pago - today).days

    if dias >= 0:
        # Vence hoy o después
        return {"status": "VIGENTE", "color": "bg-success"}
    else:
        # Fecha de próximo pago ya pasó
        return {"status": "VENCIDA", "color": "bg-danger"}
    

def get_status_principal_color(status: str) -> str:
    """ Devuelve la clase de Bootstrap para el status principal. """
    return {
        'Activo': 'bg-success',
        'Suspendido': 'bg-warning text-dark',
        'Eliminado': 'bg-danger',
        'En prueba': 'bg-info text-dark'
    }.get(status, 'bg-secondary')

# ===== Catálogos SAT (MX) =====
CATALOGO_REGIMEN = [
    ("601", "General de Ley Personas Morales"),
    ("603", "Personas Morales con Fines no Lucrativos"),
    ("605", "Sueldos y Salarios e Ingresos Asimilados a Salarios"),
    ("606", "Arrendamiento"),
    ("607", "Régimen de Enajenación o Adquisición de Bienes"),
    ("608", "Demás ingresos"),
    ("610", "Residentes en el extranjero (sin establecimiento permanente)"),
    ("611", "Ingresos por dividendos"),
    ("612", "Personas físicas con actividades profesionales"),
    ("614", "Ingresos por intereses"),
    ("615", "Ingresos por obtención de premios"),
    ("616", "Sin obligaciones fiscales"),
    ("620", "Sociedades cooperativas de producción que optan por diferir sus ingresos"),
    ("621", "Incorporación Fiscal"),
    ("622", "Actividades agrícolas, ganaderas, silvícolas y pesqueras"),
    ("623", "Opcional para grupos de sociedades"),
    ("624", "Coordinados"),
    ("625", "Actividades con plataformas tecnológicas"),
    ("626", "Régimen Simplificado de Confianza"),
]

CATALOGO_USO_CFDI = [
    ("G01", "Adquisición de mercancías"),
    ("G02", "Devoluciones, descuentos o bonificaciones"),
    ("G03", "Gastos en general"),
    ("I01", "Construcciones"),
    ("I02", "Mobiliario y equipo de oficina por inversiones"),
    ("I03", "Equipo de transporte"),
    ("I04", "Equipo de cómputo y accesorios"),
    ("I05", "Dados, troqueles, moldes, matrices y herramental"),
    ("I06", "Comunicaciones telefónicas"),
    ("I07", "Comunicaciones satelitales"),
    ("I08", "Otra maquinaria y equipo"),
    ("S01", "Sin efectos fiscales"),
    ("CP01", "Pagos"),
    ("CN01", "Nómina"),
]

def actualizar_suscripcion_cliente(cliente_id):
    """
    Recalcula las fechas de la Suscripcion del cliente basándose 
    en el último Pago con status='ACTIVO' (ignorando 'CANCELADO').
    """
    suscripcion = Suscripcion.query.filter_by(cliente_id=cliente_id).first()
    cliente = Cliente.query.get(cliente_id) # ⬅️ Necesitamos el objeto Cliente
    if not suscripcion or not cliente:
        return

    # 🛑 PASO 1: Encontrar el último pago ACTIVO
    ultimo_pago_valido = Pago.query.filter_by(
        cliente_id=cliente_id,
        status='ACTIVO' 
    ).order_by(Pago.fecha_pago.desc()).first()

    if ultimo_pago_valido:
        vigencia_str = ultimo_pago_valido.vigencia
        
        # ⚠️ LÓGICA DE CÁLCULO DE DÍAS (Ajustada para usar solo string)
        dias_a_sumar = 0
        if vigencia_str:
            if "MENSUAL" in vigencia_str.upper():
                dias_a_sumar = 30 
            elif "ANUAL" in vigencia_str.upper():
                dias_a_sumar = 365
        
        # Recalcular la fecha de vencimiento
        nueva_fecha_vence = ultimo_pago_valido.fecha_pago + timedelta(days=dias_a_sumar)
            
        # 🛑 ACTUALIZAR SUSCRIPCION (Fechas de vigencia)
        suscripcion.paquete = ultimo_pago_valido.paquete 
        suscripcion.proximo_pago = nueva_fecha_vence 
        suscripcion.vence_en = nueva_fecha_vence 
        suscripcion.status = 'Activo'
        cliente.fecha_pago = ultimo_pago_valido.fecha_pago 
            
    else:
        # Si no hay pagos ACTIVOS, se resetea todo a None/Suspendido
        suscripcion.proximo_pago = None
        suscripcion.vence_en = None
        suscripcion.paquete = None
        suscripcion.status = 'Suspendido'
        cliente.fecha_pago = None 

    db.session.commit()

@app.route('/paquetes_precios')
@login_required
@role_required(ROLES_SUPERADMIN)
def paquetes_precios_list():
    """Ruta para cargar la vista HTML de la tabla de paquetes y precios."""
    return render_template('paquetes_precios_list.html')


# app.py (Reemplaza la función api_paquetes_precios_dt)
@app.route('/api/paquetes_precios_dt')
@login_required
def api_paquetes_precios_dt():
    """Devuelve SOLO el registro de PaquetePrecio VIGENTE (más reciente) para cada combinación, excluyendo DEMO."""
    from sqlalchemy import func, distinct, case, desc, and_
    from datetime import date 

    try:
        # 1a. Subquery: Encuentra la fecha más reciente para cada grupo (País, Paquete, Vigencia)
        max_fecha_subquery = db.session.query(
            PaquetePrecio.pais,
            PaquetePrecio.paquete,
            PaquetePrecio.vigencia,
            func.max(PaquetePrecio.fecha_vigencia).label('max_fecha')
        ).filter(
            PaquetePrecio.vigencia.notilike('%DEMO%') # 🛑 EXCLUIR VIGENCIAS DEMO
        ).group_by(PaquetePrecio.pais, PaquetePrecio.paquete, PaquetePrecio.vigencia).subquery()
        
        # 1b. Consulta Principal: Encuentra el ID del registro que coincide con esa max_fecha
        registros_vigentes_sub = db.session.query(
            func.max(PaquetePrecio.id).label('id')
        ).join(
            max_fecha_subquery,
            and_(
                PaquetePrecio.pais == max_fecha_subquery.c.pais,
                PaquetePrecio.paquete == max_fecha_subquery.c.paquete,
                PaquetePrecio.vigencia == max_fecha_subquery.c.vigencia,
                PaquetePrecio.fecha_vigencia == max_fecha_subquery.c.max_fecha
            )
        ).group_by(PaquetePrecio.pais, PaquetePrecio.paquete, PaquetePrecio.vigencia).subquery()

        # 2. Consulta final: Obtener los datos completos solo de los IDs vigentes
        registros = db.session.query(PaquetePrecio).filter(
            PaquetePrecio.id.in_(registros_vigentes_sub)
        ).order_by(PaquetePrecio.pais.asc(), PaquetePrecio.paquete.asc()).all()

        data = [{
            'id': r.id,
            'pais': r.pais,
            'paquete': r.paquete,
            'vigencia': r.vigencia,
            'precio': r.precio,
            'moneda': r.moneda
        } for r in registros]
        
        return jsonify({"data": data})
    except Exception as e:
        return jsonify({"data": [], "error": str(e)}), 500


def render_template_cliente_form(is_public=False, editar=False, cliente=None, suscripcion=None, modo_edicion=False):
    from datetime import date 

    if editar:
        modo_edicion = True

    servidores = ["s14","s13","s12","s11","s10","s9","s8","s7","s6","s5","s4","s3","s2","Principal","Clínica","Colombia","Petyou 4","Petyou 3","Petyou 2","Petyou 1"]
    clientes_existentes = Cliente.query.all()
    
    # 1. Obtener y filtrar precios (Excluye DEMO de la DB)
    precios_db = PaquetePrecio.query.filter(PaquetePrecio.vigencia.notilike('%DEMO%')).all()
    
    # 2. Configuración de Países (Garantizar valores por defecto)
    paises_db_set = {p.pais for p in precios_db if p.pais}
    orden_paises_default = ["MÉXICO", "COLOMBIA", "LATAM"]
    paises = []
    
    for p in orden_paises_default:
        if p in paises_db_set:
            paises.append(p)
    for p in paises_db_set:
        if p not in paises: paises.append(p)
    
    if not paises:
        paises = orden_paises_default
    
    # 3. Vigencias
    vigencias_db_set = {p.vigencia for p in precios_db if p.vigencia}
    orden_vig = ["MENSUAL", "TRIMESTRAL", "SEMESTRAL", "ANUAL"] 
    vigencias = [v for v in orden_vig if v in vigencias_db_set]
    for v in vigencias_db_set:
        if v not in vigencias:
            vigencias.append(v)
    if not vigencias: vigencias = orden_vig

    # INYECCIÓN DE DEMO EN VIGENCIAS 
    if "DEMO" not in vigencias:
        vigencias.insert(0, "DEMO") 

    
    # 4. Llenar el catálogo con paquetes de pago
    paquetes_por_pais = {}
    for reg in precios_db:
        nombre = (reg.paquete or "")
        if "(Sucursal" in nombre or "Sucursal)" in nombre: continue
        if reg.pais:
            if reg.pais not in paquetes_por_pais:
                paquetes_por_pais[reg.pais] = []
            if nombre not in paquetes_por_pais[reg.pais]:
                paquetes_por_pais[reg.pais].append(nombre)
    
    # 5. Procesar inyección de DEMO
    final_paquetes_por_pais = {}

    for pais_key in paises:
        lista_paquetes_pago = paquetes_por_pais.get(pais_key, [])
        lista_paquetes_pago.sort()
        
        lista_final = []

        # 🛑 FIX: INYECCIÓN DIRECTA Y SIMPLE 🛑
        if not is_public:
            # 1. Añadimos Demo al inicio si estamos en Admin/Edición
            lista_final.append("Demo")
        
        # 2. Añadir todos los paquetes de pago, asegurando que no se duplique Demo
        for p in lista_paquetes_pago:
            if p != "Demo":
                lista_final.append(p)

        final_paquetes_por_pais[pais_key] = lista_final
        
    paquetes_por_pais = final_paquetes_por_pais

    # 6. Envío
    catalogo_regimen = dict(CATALOGO_REGIMEN)
    catalogo_uso_cfdi = dict(CATALOGO_USO_CFDI)

    return render_template(
        'cliente_form.html',
        is_public=is_public,
        modo_edicion=modo_edicion, 
        cliente=cliente,
        suscripcion=suscripcion,
        servidores=servidores,
        paises=paises,
        paquetes_por_pais=paquetes_por_pais, 
        vigencias=vigencias, 
        clientes_existentes=clientes_existentes,
        catalogo_regimen=catalogo_regimen,
        catalogo_uso_cfdi=catalogo_uso_cfdi,
        date=date
    )

@app.route('/api/paquetes_precios/nuevo', methods=['POST'])
@login_required
def api_paquetes_precios_nuevo():
    """Crea un nuevo registro de PaquetePrecio."""
    data = request.get_json()
    
    fecha_vigencia_str = data.get('fecha_vigencia')
    if not fecha_vigencia_str:
        return jsonify({"ok": False, "error": "La fecha de vigencia es obligatoria."}), 400
        
    try:
        nuevo = PaquetePrecio(
            pais=data['pais'].upper().strip(),
            paquete=data['paquete'].strip(),
            vigencia=data['vigencia'].upper().strip(),
            precio=float(data['precio']),
            moneda=data['moneda'].upper().strip(),
            fecha_vigencia=date.fromisoformat(fecha_vigencia_str)
        )
        db.session.add(nuevo)
        db.session.commit()
        return jsonify({"ok": True, "msg": "Paquete creado correctamente."})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": f"Error al crear: {e}"}), 500
    

@app.route('/api/paquetes_precios/<int:id>')
@login_required
def api_paquetes_precios_get(id):
    """Obtiene un registro por ID para edición."""
    registro = PaquetePrecio.query.get_or_404(id)
    data = {
        'id': registro.id,
        'pais': registro.pais,
        'paquete': registro.paquete,
        'vigencia': registro.vigencia,
        'precio': float(registro.precio),
        'moneda': registro.moneda,
        'fecha_vigencia': registro.fecha_vigencia.isoformat() if registro.fecha_vigencia else date.today().isoformat()
    }
    return jsonify({"ok": True, "data": data})


@app.route('/api/paquetes_precios/editar/<int:id>', methods=['POST'])
@login_required
def api_paquetes_precios_editar(id):
    """Edita un registro existente de PaquetePrecio."""
    registro = PaquetePrecio.query.get_or_404(id)
    data = request.get_json()
    
    fecha_vigencia_str = data.get('fecha_vigencia')
    if not fecha_vigencia_str:
        return jsonify({"ok": False, "error": "La fecha de vigencia es obligatoria."}), 400
        
    try:
        registro.pais = data['pais'].upper().strip()
        registro.paquete = data['paquete'].strip()
        registro.vigencia = data['vigencia'].upper().strip()
        registro.precio = float(data['precio'])
        registro.moneda = data['moneda'].upper().strip()
        registro.fecha_vigencia = date.fromisoformat(fecha_vigencia_str)
        
        db.session.commit()
        return jsonify({"ok": True, "msg": "Paquete actualizado correctamente."})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": f"Error al editar: {e}"}), 500




# ========== Login / Logout ==========
@login_manager.user_loader
def load_user(user_id):
    # Fix: Usar db.session.get() en lugar de User.query.get()
    return db.session.get(User, int(user_id)) 


@app.route('/login', methods=['GET', 'POST'])
def login():
    from flask_wtf import FlaskForm # Re-importar si es necesario
    from wtforms import StringField, PasswordField, SubmitField, validators
    
    # Definición de formulario para compatibilidad con Flask-WTF
    class TempLoginForm(FlaskForm):
        username = StringField('Usuario', validators=[validators.DataRequired()])
        password = PasswordField('Contraseña', validators=[validators.DataRequired()])
        submit = SubmitField('Iniciar Sesión')

    form = TempLoginForm()
    
    # Manejar caso de login si el HTML usa Flask-WTF (preferido)
    if form.validate_on_submit():
        username = form.username.data
        password = form.password.data
        usuario = User.query.filter_by(username=username).first()

        if usuario and usuario.check_password(password): # CRÍTICO: Usa el nuevo método
            login_user(usuario)
            flash('¡Bienvenido!', 'success')
            return redirect(url_for('clientes_list'))
        else:
            flash('Usuario o contraseña incorrectos.', 'danger')
            
    # Manejar caso de login si el HTML NO USA Flask-WTF (por compatibilidad con tu HTML anterior)
    if request.method == 'POST' and 'username' in request.form and 'password' in request.form:
          username = request.form['username']
          password = request.form['password']
          usuario = User.query.filter_by(username=username).first()
          
          # CRÍTICO: Usar el método check_password
          if usuario and usuario.check_password(password): 
              login_user(usuario)
              flash('¡Bienvenido!', 'success')
              return redirect(url_for('clientes_list'))
          else:
              flash('Usuario o contraseña incorrectos', 'danger')
              
    return render_template('login.html', form=form)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# ========== Ejemplos básicos ==========
@app.route('/')
@login_required
def index():
    registros = Pago.query.all()
    return render_template('index.html', registros=registros)

@app.route('/form', methods=['GET', 'POST'])
@login_required
def form():
    if request.method == 'POST':
        nombre = request.form['nombre']
        correo = request.form['correo']
        monto = request.form['monto']
        nuevo_pago = Pago(nombre=nombre, correo=correo, monto=float(monto))
        db.session.add(nuevo_pago)
        db.session.commit()
        flash("Registro guardado correctamente")
        return redirect(url_for('form'))
    return render_template('form.html')

@app.route('/form_privado', methods=['GET', 'POST'])
@login_required
def form_privado():
    if request.method == 'POST':
        nombre = request.form['nombre']
        correo = request.form['correo']
        monto = request.form['monto']
        nuevo_pago = Pago(nombre=nombre, correo=correo, monto=float(monto))
        db.session.add(nuevo_pago)
        db.session.commit()
        flash("Registro privado guardado correctamente")
        return redirect(url_for('form_privado'))
    return render_template('form.html')

@app.route('/descargar')
@login_required
def descargar():
    pagos = Pago.query.all()
    data = [{'Nombre': p.nombre, 'Correo': p.correo, 'Monto': p.monto} for p in pagos]
    df = pd.DataFrame(data)
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False, encoding='utf-8-sig')
    buffer.seek(0)
    return send_file(buffer, as_attachment=True, download_name='pagos.csv', mimetype='text/csv')

@app.route('/dashboard')
@login_required
@role_required(ROLES_LECTURA)
def dashboard():
    pagos = Pago.query.all()
    total = sum(p.monto for p in pagos)
    cantidad = len(pagos)
    now = datetime.now()
    return render_template('dashboard.html', total=total, cantidad=cantidad, pagos=pagos, now=now)



# =======================================================
# CRUD USUARIOS ADMINISTRADORES (User)
# =======================================================



@app.route('/usuarios')
@login_required
@role_required(ROLES_ADMIN_CRUD)
def usuarios_list():
    """Ruta para cargar la vista HTML de la lista de usuarios."""
    return render_template('usuarios_list.html')

@app.route('/api/usuarios_dt')
@login_required
@role_required(ROLES_ADMIN_CRUD)
def api_usuarios_dt():
    """Devuelve la lista de usuarios para DataTables."""
    usuarios = User.query.order_by(User.id.asc()).all()
    data = [{
        'id': u.id,
        'username': u.username,
        'full_name': u.full_name, # 🛑 Campo nuevo
        'email': u.email,         # 🛑 Campo nuevo
        'role': u.role            # 🛑 Campo nuevo
    } for u in usuarios]
    return jsonify({"data": data})

@app.route('/api/usuarios/nuevo', methods=['POST'])
@login_required
@role_required(ROLES_ADMIN_CRUD)
def api_usuario_nuevo():
    """Crea un nuevo usuario."""
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    full_name = data.get('full_name')
    email = data.get('email')
    role = data.get('role', 'LECTOR') # Valor por defecto

    if not username or not password or not full_name:
        return jsonify({"ok": False, "error": "Username, Nombre y Password son obligatorios."}), 400
    
    if User.query.filter_by(username=username).first():
        return jsonify({"ok": False, "error": f"El usuario '{username}' ya existe."}), 400

    try:
        nuevo_usuario = User(
            username=username,
            password_hash=generate_password_hash(password, method='pbkdf2:sha256'),
            full_name=full_name,
            email=email,
            role=role.upper()
        )
        db.session.add(nuevo_usuario)
        db.session.commit()
        return jsonify({"ok": True, "msg": "Usuario creado correctamente."})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": f"Error al crear usuario: {e}"}), 500

@app.route('/api/usuarios/editar/<int:id>', methods=['POST'])
@login_required
@role_required(ROLES_ADMIN_CRUD)
def api_usuario_editar(id):
    """Edita los datos o resetea el password de un usuario existente."""
    usuario = User.query.get_or_404(id)
    data = request.get_json()
    
    username = data.get('username')
    password = data.get('password')
    full_name = data.get('full_name')
    email = data.get('email')
    role = data.get('role')

    # Validación de usuario y rol
    if id == current_user.id and role and role.upper() != current_user.role.upper():
        return jsonify({"ok": False, "error": "No puedes cambiar tu propio rol en esta interfaz."}), 403
    
    if username and username != usuario.username:
        if User.query.filter(User.username == username, User.id != id).first():
            return jsonify({"ok": False, "error": f"El usuario '{username}' ya existe."}), 400
        usuario.username = username

    if full_name: usuario.full_name = full_name
    if email: usuario.email = email
    if role: usuario.role = role.upper()

    if password:
        usuario.password_hash = generate_password_hash(password, method='pbkdf2:sha256')

    try:
        db.session.commit()
        return jsonify({"ok": True, "msg": "Usuario actualizado correctamente."})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": f"Error al editar usuario: {e}"}), 500

@app.route('/api/usuarios/eliminar/<int:id>', methods=['POST'])
@login_required
@role_required(ROLES_ADMIN_CRUD)
def api_usuario_eliminar(id):
    """Elimina un usuario."""
    if id == current_user.id:
        return jsonify({"ok": False, "error": "No puedes eliminar tu propia cuenta mientras estás logueado."}), 403
        
    if User.query.count() <= 1:
        return jsonify({"ok": False, "error": "No se puede eliminar al único usuario del sistema."}), 400
        
    usuario = User.query.get_or_404(id)

    try:
        db.session.delete(usuario)
        db.session.commit()
        return jsonify({"ok": True, "msg": "Usuario eliminado correctamente."})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": f"Error al eliminar usuario: {e}"}), 500
    

# app.py (rutas de clientes)
@app.route('/clientes')
@login_required
@role_required(ROLES_LECTURA)
def clientes_list():
    from sqlalchemy import select, outerjoin
    from datetime import date

    stmt = (
        select(
            Cliente.id,
            Cliente.negocio,
            Cliente.nombre_contacto,
            Cliente.mail,
            Cliente.telefono,
            Cliente.telefono_secundario_1,
            Cliente.telefono_secundario_2,
            Cliente.telefono_secundario_3,
            Cliente.pais,
            Suscripcion.id_gumi,
            Suscripcion.status,
            Suscripcion.server,
            Suscripcion.paquete,
            Suscripcion.vigencia,
            Suscripcion.vence_en,
            Suscripcion.proximo_pago,
            Suscripcion.observaciones,
            Cliente.fecha_pago.label("ultimo_pago") # ✅ toma directamente del cliente_form
        )
        .outerjoin(Suscripcion, Suscripcion.cliente_id == Cliente.id)
        .order_by(Cliente.negocio.asc())
    )

    results = db.session.execute(stmt).all()
    today = date.today()

    clientes = []
    for r in results:
        (
            cid, negocio, nombre_contacto, mail, tel, tel1, tel2, tel3,
            pais, id_gumi, status_sus, server, paquete, vigencia,
            vence_en, proximo_pago, observaciones, ultimo_pago
        ) = r

        dias_restantes = (vence_en - today).days if vence_en else None
        status = status_sus or "Activo"

        clientes.append({
            "id": cid,
            "negocio": negocio,
            "nombre_contacto": nombre_contacto,
            "mail": mail,
            "telefono": tel,
            "tel_sec_1": tel1,
            "tel_sec_2": tel2,
            "tel_sec_3": tel3,
            "pais": pais,
            "status": status,
            "id_gumi": id_gumi,
            "server": server,
            "paquete": paquete,
            "vigencia": vigencia,
            "vence_en": vence_en,
            "proximo_pago": proximo_pago,
            "dias_restantes": dias_restantes,
            "observaciones": observaciones,
            "ultimo_pago": ultimo_pago
        })

    return render_template(
        'clientes_list.html',
        clientes=clientes,
        current_date=today
    )

# app.py

# ==========================================
# 1. RUTA PRINCIPAL: Clientes DEMO
# ==========================================
@app.route('/clientes/demo')
@login_required
def clientes_demo_list():
    # Reutilizamos la lógica para obtener países y paquetes para filtros
    precios_db = PaquetePrecio.query.all()
    paises_db_set = {p.pais for p in precios_db if p.pais}
    
    # Pre-cargar países para el filtro de la vista
    orden_paises = ["MÉXICO", "COLOMBIA", "LATAM"]
    paises = [p for p in orden_paises if p in paises_db_set]
    for p in paises_db_set:
        if p not in paises: paises.append(p)
    if not paises: paises = ["MÉXICO", "COLOMBIA", "LATAM"]

    return render_template(
        'clientes_demo_list.html', # ⬅️ Nuevo archivo HTML
        paises=paises
    )


@app.route('/api/clientes_demo_dt')
@login_required
def api_clientes_demo_dt():
    from datetime import date
    from sqlalchemy import select, outerjoin, or_, and_

    today = date.today()

    # 1. Consulta
    stmt = (
        select(
            Cliente.id,
            Cliente.negocio,
            Cliente.nombre_contacto,
            Cliente.mail,
            Cliente.pais,
            Cliente.status_cliente,
            Cliente.telefono,
            Cliente.telefono_secundario_1,
            Cliente.telefono_secundario_2,
            Cliente.telefono_secundario_3,
            Suscripcion.id_gumi,
            Suscripcion.server,
            Suscripcion.paquete,
            Suscripcion.status,
            Suscripcion.vence_en
        )
        .select_from(outerjoin(Cliente, Suscripcion, Cliente.id == Suscripcion.cliente_id))
        .where(
            or_(
                Suscripcion.paquete.ilike('%Demo%'),
                and_(
                    Suscripcion.id.is_(None),
                    Cliente.status_cliente.ilike('%En prueba%')
                )
            )
        )
        .order_by(Cliente.negocio.asc())
    )
    
    try:
        results = db.session.execute(stmt).all()
        
        # 2. Diccionario para meses en español
        meses_abbr = {
            1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
            7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
        }
        
        rows = []
        for row in results:
            (
                cid, negocio, nombre_contacto, mail, pais, status_cliente, tel, tel1, tel2, tel3, 
                id_gumi, server, paquete, status_sus, vence_en
            ) = row

            status = status_sus or status_cliente or "En prueba"
            paquete_display = paquete or "N/A (Lead)"

            # Formato de teléfonos
            telefonos_html = ''
            if tel: 
                telefonos_html += f'<div class="mb-1"><a href="https://wa.me/{tel}" target="_blank" class="fw-bold text-success text-decoration-none"><i class="fa-brands fa-whatsapp"></i> {tel}</a></div>'
            if tel1: telefonos_html += f'<div class="small text-muted">{tel1}</div>'
            if tel2: telefonos_html += f'<div class="small text-muted">{tel2}</div>'

            # 3. Formato de Fecha Amigable
            if vence_en:
                dia = vence_en.day
                mes = meses_abbr.get(vence_en.month, "")
                anio = vence_en.year
                vence_en_display = f"{dia} {mes} {anio}" # Ej: 30 nov 2025
                vence_en_orden = vence_en.isoformat()    # Ej: 2025-11-30 (para ordenar)
            else:
                vence_en_display = "—"
                vence_en_orden = "9999-12-31"

            rows.append({
                "id": cid,
                "id_gumi": id_gumi or "—",
                "server": server or "—",
                "pais": pais or "—",
                "status": status,
                "negocio": negocio,
                "nombre_contacto": nombre_contacto,
                "mail": mail or "—",
                "paquete": paquete_display,
                "telefonos_display": telefonos_html,
                
                # Enviamos ambas versiones:
                "vence_en": vence_en_display,         # Lo que ve el usuario
                "vence_en_orden": vence_en_orden         # Dato oculto para ordenar correctamente
            })

        return jsonify({"data": rows})

    except Exception as e:
        return jsonify({"data": [], "error": "Error interno"}), 500
    

# ==========================================
@app.route('/clientes/nuevo', methods=['GET', 'POST'])
def clientes_nuevo():
    # Determinamos quién es (si no hay usuario logueado, es público)
    is_public = not current_user.is_authenticated

    if request.method == 'POST':
        try:
            # --- A. DATOS DEL CLIENTE ---
            negocio = request.form['negocio']
            nombre_contacto = request.form['nombre_contacto']
            mail = request.form['mail']
            telefono = request.form['telefono']
            
            telefono_secundario_1 = request.form.get('telefono_secundario_1') or None
            telefono_secundario_2 = request.form.get('telefono_secundario_2') or None
            telefono_secundario_3 = request.form.get('telefono_secundario_3') or None
            pais = request.form['pais']
            localidad = request.form.get('localidad') or None

            requiere_factura = bool(request.form.get('requiere_factura'))
            razon_social = request.form.get('razon_social')
            rfc = request.form.get('rfc')
            codigo_postal = request.form.get('codigo_postal')
            regimen_fiscal = request.form.get('regimen_fiscal')
            uso_cfdi = request.form.get('uso_cfdi')
            mail_facturas = request.form.get('mail_facturas')

            cliente = Cliente(
                negocio=negocio, nombre_contacto=nombre_contacto, mail=mail, telefono=telefono,
                telefono_secundario_1=telefono_secundario_1, telefono_secundario_2=telefono_secundario_2,
                telefono_secundario_3=telefono_secundario_3, pais=pais, localidad=localidad,
                requiere_factura=requiere_factura, razon_social=razon_social, rfc=rfc,
                codigo_postal=codigo_postal, regimen_fiscal=regimen_fiscal, uso_cfdi=uso_cfdi,
                mail_facturas=mail_facturas
            )
            db.session.add(cliente)
            db.session.flush() # Generar ID

            # --- B. DATOS DE SUSCRIPCIÓN ---
            if is_public:
                # Registro Público: Datos Demo
                suscripcion = Suscripcion(
                    cliente_id=cliente.id,
                    status='En prueba',
                    server='Principal',
                    fecha_inicio=date.today(),
                    paquete='Demo',
                    vigencia='MENSUAL',
                    es_sucursal=False,
                    observaciones="Registro público"
                )
                vence, prox = calcular_fechas_vigencia(date.today(), 'MENSUAL')
                suscripcion.vence_en = vence
                suscripcion.proximo_pago = prox
                db.session.add(suscripcion)
            else:
                # Admin: Datos del Formulario
                id_gumi = request.form.get('id_gumi')
                status = request.form.get('status') or 'Activo'
                server = request.form.get('server')
                paquete = request.form.get('paquete')
                vigencia = request.form.get('vigencia')
                fecha_inicio = date.fromisoformat(request.form.get('fecha_inicio') or date.today().isoformat())
                observaciones = request.form.get('observaciones')
                
                es_sucursal = bool(request.form.get('es_sucursal'))
                matriz_id = None
                if es_sucursal:
                    matriz_nom = request.form.get('matriz_nombre')
                    if matriz_nom:
                        m = Cliente.query.filter_by(negocio=matriz_nom).first()
                        if m: matriz_id = m.id

                vence_en, proximo_pago = calcular_fechas_vigencia(fecha_inicio, vigencia)

                suscripcion = Suscripcion(
                    cliente_id=cliente.id, id_gumi=id_gumi, status=status, server=server,
                    fecha_inicio=fecha_inicio, paquete=paquete, vigencia=vigencia,
                    vence_en=vence_en, proximo_pago=proximo_pago,
                    es_sucursal=es_sucursal, matriz_id=matriz_id, observaciones=observaciones
                )
                db.session.add(suscripcion)

                # --- C. PRIMER PAGO (Solo Admin) ---
                monto = float(request.form.get('precio_paquete') or 0)
                fecha_pago = date.fromisoformat(request.form.get('fecha_pago') or date.today().isoformat())
                
                # 🛑 FIX CRÍTICO: Generar bank_transaction_id para pagos manuales
                from time import time
                timestamp_ms = int(time() * 1000) 
                manual_unique_id_fallback = -1 * timestamp_ms

                pago = Pago(
                    cliente_id=cliente.id,
                    nombre=cliente.nombre_contacto,
                    correo=cliente.mail,
                    numero_whatsapp=cliente.telefono,
                    monto=monto,
                    fecha_pago=fecha_pago,
                    bank_transaction_id=manual_unique_id_fallback, # Usamos el ID único negativo
                    metodo_pago=request.form.get('metodo_pago'),
                    otro_metodo_pago=request.form.get('otro_metodo_pago'),
                    factura_pago=(request.form.get('factura_pago') == 'on'),
                    numero_factura=request.form.get('numero_factura'),
                    motivo_descuento=request.form.get('motivo_descuento'),
                    paquete=paquete,
                    vigencia=vigencia,
                    moneda=request.form.get('moneda_paquete')
                )
                db.session.add(pago)

            db.session.commit()
            
            if is_public:
                flash('✅ Registro exitoso. Nos pondremos en contacto.', 'success')
                return redirect(url_for('login')) 
            else:
                flash('✅ Cliente registrado correctamente.', 'success')
                return redirect(url_for('clientes_list'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error al registrar: {str(e)}', 'danger')

    # Renderizado del formulario
    return render_template_cliente_form(
        is_public=is_public, 
        modo_edicion=False
    ) # ⬅️ ESTE PARÉNTESIS CERRADO ES VITAL



@app.route('/clientes/<int:id>/editar', methods=['GET', 'POST'])
@login_required
def clientes_editar(id):
    cliente = Cliente.query.get_or_404(id)
    # Intentamos obtener la suscripción, si no existe, será None
    suscripcion = Suscripcion.query.filter_by(cliente_id=id).first()

    # Si es un cliente antiguo o público sin suscripción, creamos una vacía
    if not suscripcion:
        suscripcion = Suscripcion(
            cliente_id=cliente.id, 
            status='En prueba', 
            server='Principal',
            fecha_inicio=date.today(),
            paquete='Sin Paquete',
            vigencia='Mensual'
        )
        db.session.add(suscripcion)

    if request.method == 'POST':
        try:
            # --- 1. Actualizar Cliente (Datos Generales) ---
            cliente.negocio = request.form.get('negocio')
            cliente.nombre_contacto = request.form.get('nombre_contacto')
            cliente.mail = request.form.get('mail')
            cliente.telefono = request.form.get('telefono')
            cliente.telefono_secundario_1 = request.form.get('telefono_secundario_1') or None
            cliente.telefono_secundario_2 = request.form.get('telefono_secundario_2') or None
            cliente.telefono_secundario_3 = request.form.get('telefono_secundario_3') or None
            cliente.pais = request.form.get('pais')
            cliente.localidad = request.form.get('localidad') or None

            # --- Fiscales ---
            cliente.requiere_factura = bool(request.form.get('requiere_factura'))
            if cliente.requiere_factura:
                cliente.razon_social = request.form.get('razon_social')
                cliente.rfc = request.form.get('rfc')
                cliente.codigo_postal = request.form.get('codigo_postal')
                cliente.regimen_fiscal = request.form.get('regimen_fiscal')
                cliente.uso_cfdi = request.form.get('uso_cfdi')
                cliente.mail_facturas = request.form.get('mail_facturas')

            # --- 2. Actualizar Suscripción (Configuración Técnica) ---
            suscripcion.id_gumi = request.form.get('id_gumi') or None
            suscripcion.status = request.form.get('status') # Importante: Aquí tomamos el status que el admin elija
            suscripcion.server = request.form.get('server')
            suscripcion.paquete = request.form.get('paquete')
            suscripcion.vigencia = request.form.get('vigencia')
            suscripcion.observaciones = request.form.get('observaciones')

            fecha_inicio_str = request.form.get('fecha_inicio')
            if fecha_inicio_str:
                suscripcion.fecha_inicio = date.fromisoformat(fecha_inicio_str)
            
            # Fechas calculadas
            vence_en_str = request.form.get('vence_en')
            if vence_en_str:
                suscripcion.vence_en = date.fromisoformat(vence_en_str)
            prox_pago_str = request.form.get('proximo_pago')
            if prox_pago_str:
                suscripcion.proximo_pago = date.fromisoformat(prox_pago_str)

            # Sucursal
            suscripcion.es_sucursal = bool(request.form.get('es_sucursal'))
            matriz_nombre = request.form.get('matriz_nombre')
            if suscripcion.es_sucursal and matriz_nombre:
                matriz = Cliente.query.filter_by(negocio=matriz_nombre).first()
                suscripcion.matriz_id = matriz.id if matriz else None
            else:
                suscripcion.matriz_id = None


            # --- 3. DETECCIÓN DE ACTIVACIÓN (REGISTRO DE PAGO) ---
            # Si el formulario envió un 'metodo_pago', significa que estamos en el escenario de "Activar Lead"
            metodo_pago_enviado = request.form.get('metodo_pago')
            
            if metodo_pago_enviado:
                monto = float(request.form.get('precio_paquete') or 0)
                fecha_pago_raw = request.form.get('fecha_pago') or date.today().isoformat()
                fecha_pago = date.fromisoformat(fecha_pago_raw)

                # 🛑 FIX CRÍTICO: Generar bank_transaction_id para pagos manuales
                from time import time
                timestamp_ms = int(time() * 1000) 
                manual_unique_id_fallback = -1 * timestamp_ms

                nuevo_pago = Pago(
                    cliente_id=cliente.id,
                    nombre=cliente.nombre_contacto,
                    correo=cliente.mail,
                    numero_whatsapp=cliente.telefono,
                    monto=monto,
                    fecha_pago=fecha_pago,
                    bank_transaction_id=manual_unique_id_fallback, # Usamos el ID único negativo
                    metodo_pago=metodo_pago_enviado,
                    otro_metodo_pago=request.form.get('otro_metodo_pago'),
                    factura_pago=(request.form.get('factura_pago') == 'on'),
                    numero_factura=request.form.get('numero_factura'),
                    motivo_descuento=request.form.get('motivo_descuento'),
                    paquete=suscripcion.paquete,    # Usamos el paquete recién guardado
                    vigencia=suscripcion.vigencia,  # Usamos la vigencia recién guardada
                    moneda=request.form.get('moneda_paquete')
                )
                db.session.add(nuevo_pago)
                
                # También actualizamos el "último pago" en la tabla Cliente para referencia rápida
                cliente.fecha_pago = fecha_pago

            db.session.commit()
            flash('✅ Cliente actualizado correctamente.', 'success')
            return redirect(url_for('clientes_list'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error al guardar: {str(e)}', 'danger')

    # ===== GET: Renderizar Vista =====
    return render_template_cliente_form(
        is_public=False,     
        modo_edicion=True,
        cliente=cliente, 
        suscripcion=suscripcion
    )


from werkzeug.utils import secure_filename 
# import numpy as np # Ya importado arriba

# =======================================================
# CARGA MASIVA DE CLIENTES (SUPERADMIN)
# =======================================================

@app.route('/clientes/importar', methods=['GET', 'POST'])
@login_required
@role_required(ROLES_SUPERADMIN)
def clientes_importar():
    """Ruta para la carga masiva de clientes desde un archivo CSV."""
    if request.method == 'POST':
        if 'archivo_csv' not in request.files:
            flash('No se encontró el archivo en la petición.', 'danger')
            return redirect(url_for('clientes_importar'))

        file = request.files['archivo_csv']

        if file.filename == '':
            flash('Seleccione un archivo.', 'danger')
            return redirect(url_for('clientes_importar'))

        if file and file.filename.endswith('.csv'):
            try:
                df = pd.read_csv(file, encoding='utf-8', dtype={
                    'TELEFONO_PRINCIPAL': str,
                    'TELEFONO_SECUNDARIO': str, # Asumiendo que esta columna existe si la tienes en el CSV
                    'TELEFONO_TERCIARIO': str,  # Asumiendo que esta columna existe si la tienes en el CSV
                    # Podrías agregar otras columnas como ID_GUMI o RFC si te dan problemas numéricos
                })
                
                conteo_total = len(df)
                conteo_exitoso = 0
                errores = []

                df.columns = df.columns.str.upper().str.strip()

                REQUIRED_COLS_MIN = ['NEGOCIO', 'CONTACTO', 'MAIL', 'TELEFONO_PRINCIPAL', 'PAIS', 'STATUS', 'SERVER', 'PAQUETE', 'VIGENCIA', 'FECHA_INICIO_SUSCRIPCION']
                
                if not all(col in df.columns for col in REQUIRED_COLS_MIN):
                    missing_cols = [col for col in REQUIRED_COLS_MIN if col not in df.columns]
                    raise ValueError(f"Faltan columnas requeridas en el CSV: {', '.join(missing_cols)}")

                db.session.begin_nested() 
                
                for index, row in df.iterrows():
                    try:
                        # --- 1. PROCESAMIENTO DE DATOS CRÍTICOS ---
                        paquete = str(row['PAQUETE']).strip()
                        vigencia = str(row['VIGENCIA']).strip()
                        status = str(row['STATUS']).strip().upper()
                        
                        # 🛑 FIX 1: Manejar NaN en FECHA_INICIO_SUSCRIPCION (que es requerida)
                        fecha_inicio_sus_raw = row['FECHA_INICIO_SUSCRIPCION']
                        if pd.isna(fecha_inicio_sus_raw) or str(fecha_inicio_sus_raw).strip().upper() == 'NAN':
                            raise ValueError("FECHA_INICIO_SUSCRIPCION es obligatoria y no puede estar vacía.")
                        fecha_inicio_sus = date.fromisoformat(str(fecha_inicio_sus_raw).strip())
                        
                        
                        # --- 2. PROCESAMIENTO DE PAGO ---
                        fecha_pago_cliente = None
                        monto_pago = 0.0
                        moneda_pago = 'MXN'
                        
                        if status == 'ACTIVO':
                            # 🛑 FIX 2: Manejar NaN en FECHA_ULTIMO_PAGO
                            fecha_pago_raw = row.get('FECHA_ULTIMO_PAGO')
                            if pd.isna(fecha_pago_raw) or str(fecha_pago_raw).strip().upper() == 'NAN':
                                raise ValueError("Falta FECHA_ULTIMO_PAGO para cliente ACTIVO.")
                            
                            fecha_pago_cliente = date.fromisoformat(str(fecha_pago_raw).strip())
                            
                            monto_pago = float(row.get('MONTO_PAGO', 0.0) or 0.0)
                            moneda_pago = str(row.get('MONEDA', 'MXN')).strip()
                            if moneda_pago not in ['MXN', 'COP', 'USD']:
                                moneda_pago = 'MXN'

                        # --- 3. PROCESAMIENTO DE CAMPOS OPCIONALES ---

                        # Función helper para limpiar NaN/vacío
                        def get_clean_value(val, default=None):
                            if pd.isna(val) or str(val).strip().upper() == 'NAN' or str(val).strip() == '':
                                return default
                            return str(val).strip()

                        # Teléfonos y Gumi ID
                        tel_sec_1 = get_clean_value(row.get('TELEFONO_SECUNDARIO'))
                        tel_sec_2 = get_clean_value(row.get('TELEFONO_TERCIARIO'))
                        id_gumi = get_clean_value(row.get('ID_GUMI'))

                        # Datos Fiscales y Localidad
                        razon_social_val = get_clean_value(row.get('RAZON_SOCIAL'))
                        rfc_val = get_clean_value(row.get('RFC_NIT'))
                        cp_val = get_clean_value(row.get('CODIGO_POSTAL'))
                        regimen_val = get_clean_value(row.get('REGIMEN_FISCAL'))
                        uso_cfdi_val = get_clean_value(row.get('USO_CFDI'))
                        mail_facturas_val = get_clean_value(row.get('MAIL_FACTURAS'))
                        localidad_val = get_clean_value(row.get('LOCALIDAD'))

                        # Determinar si requiere_factura
                        requiere_factura = bool(razon_social_val and rfc_val)
                        
                        # --- 4. CREAR CLIENTE ---
                        cliente = Cliente(
                            negocio=str(row['NEGOCIO']).strip(),
                            nombre_contacto=str(row['CONTACTO']).strip(),
                            mail=str(row['MAIL']).strip(),
                            telefono=str(row['TELEFONO_PRINCIPAL']).strip(),
                            pais=str(row['PAIS']).strip().upper(),
                            status_cliente=status,
                            fecha_pago=fecha_pago_cliente,
                            
                            telefono_secundario_1=tel_sec_1,
                            telefono_secundario_2=tel_sec_2,
                            telefono_secundario_3=None,
                            
                            requiere_factura=requiere_factura,
                            razon_social=razon_social_val,
                            rfc=rfc_val,
                            codigo_postal=cp_val,
                            regimen_fiscal=regimen_val,
                            uso_cfdi=uso_cfdi_val,
                            mail_facturas=mail_facturas_val,
                            localidad=localidad_val
                        )
                        db.session.add(cliente)
                        db.session.flush()

                        # --- 5. CREAR SUSCRIPCIÓN ---
                        vence_en, proximo_pago = calcular_fechas_vigencia(fecha_inicio_sus, vigencia)

                        suscripcion = Suscripcion(
                            cliente_id=cliente.id,
                            id_gumi=id_gumi,
                            status=status,
                            server=str(row['SERVER']).strip(),
                            fecha_inicio=fecha_inicio_sus,
                            paquete=paquete,
                            vigencia=vigencia,
                            vence_en=vence_en,
                            proximo_pago=proximo_pago
                        )
                        db.session.add(suscripcion)
                        
                        # --- 6. CREAR REGISTRO DE PAGO (Si es Activo) ---
                        if status == 'ACTIVO':
                              # 🛑 FIX: Generar bank_transaction_id para pagos manuales de carga masiva
                              from time import time
                              timestamp_ms = int(time() * 1000) 
                              manual_unique_id_fallback = -1 * (timestamp_ms + index) # Asegura unicidad

                              pago = Pago(
                                  cliente_id=cliente.id,
                                  nombre=cliente.nombre_contacto,
                                  correo=cliente.mail,
                                  numero_whatsapp=cliente.telefono,
                                  monto=monto_pago,
                                  fecha_pago=fecha_pago_cliente,
                                  bank_transaction_id=manual_unique_id_fallback,
                                  metodo_pago='Carga Masiva',
                                  paquete=paquete,
                                  vigencia=vigencia,
                                  moneda=moneda_pago,
                                  status='ACTIVO'
                              )
                              db.session.add(pago)

                        conteo_exitoso += 1

                    except Exception as e:
                        # Fila 2 corresponde al índice 0, por eso es index + 2
                        errores.append(f"Fila {index + 2}: {e}")
                        db.session.rollback()
                        db.session.begin_nested() 
                        continue
                
                db.session.commit()

                if not errores:
                    flash(f"✅ Éxito: Se cargaron {conteo_exitoso}/{conteo_total} clientes sin errores.", 'success')
                else:
                    flash(f"⚠️ Advertencia: Se cargaron {conteo_exitoso}/{conteo_total} clientes. Hubo {len(errores)} errores. Revise la lista.", 'warning')
                    for err in errores:
                        print(f"ERROR DE CARGA: {err}")

                return redirect(url_for('clientes_importar'))

            except Exception as e:
                db.session.rollback()
                error_msg = f"Error fatal al procesar el archivo. Revise el formato. Error: {e}"
                flash(error_msg, 'danger')
                
            return redirect(url_for('clientes_importar'))

    return render_template('importar_clientes.html')



# ========== Suscripciones ==========
@app.route('/suscripcion/nueva/<int:cliente_id>', methods=['GET', 'POST'])
@login_required
def suscripcion_nueva(cliente_id):
    cliente = Cliente.query.get_or_404(cliente_id)
    negocios = Cliente.query.filter_by(es_sucursal=False).all()

    if request.method == 'POST':
        id_gumi = request.form.get('id_gumi') or None
        status = request.form['status']
        server = request.form['server']
        paquete = request.form['paquete']
        vigencia = request.form['vigencia']
        observaciones = request.form.get('observaciones') or None

        fecha_inicio_str = request.form['fecha_inicio']
        fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()

        es_sucursal = bool(request.form.get('es_sucursal'))
        matriz_nombre = request.form.get('matriz_nombre') or None
        matriz_id = None
        if es_sucursal and matriz_nombre:
            matriz = Cliente.query.filter_by(negocio=matriz_nombre, es_sucursal=False).first()
            if matriz:
                matriz_id = matriz.id

        vence_en, proximo_pago = calcular_fechas_vigencia(fecha_inicio, vigencia)

        sus = cliente.suscripcion
        if sus is None:
            sus = Suscripcion(cliente_id=cliente.id)

        sus.id_gumi = id_gumi
        sus.status = status
        sus.server = server
        sus.fecha_inicio = fecha_inicio
        sus.paquete = paquete
        sus.vigencia = vigencia
        sus.vence_en = vence_en
        sus.proximo_pago = proximo_pago
        sus.es_sucursal = es_sucursal
        sus.matriz_id = matriz_id
        sus.observaciones = observaciones

        db.session.add(sus)
        db.session.commit()
        flash('Suscripción guardada correctamente.', 'success')
        return redirect(url_for('clientes_list'))

    return render_template(
        'suscripcion_form.html',
        cliente=cliente,
        suscripcion=cliente.suscripcion,
        negocios=negocios
    )

@app.route('/api/clientes/<int:cliente_id>/status', methods=['POST'])
@login_required
def api_cambiar_status(cliente_id):
    data = request.get_json()
    nuevo = data.get('status')

    if not nuevo:
        return jsonify({"error": "Falta el nuevo status"}), 400

    cliente = Cliente.query.get_or_404(cliente_id)
    suscripcion = Suscripcion.query.filter_by(cliente_id=cliente_id).first()

    # Actualiza ambos si existen
    cliente.status_cliente = nuevo
    if suscripcion:
        suscripcion.status = nuevo

    try:
        db.session.commit()
        return jsonify({"ok": True, "nuevo_status": nuevo})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500



# ========== APIs auxiliares ==========
# 🛑 FIX CRÍTICO: RUTA MODIFICADA PARA USAR POST (request.get_json())
@app.route('/api/calcular_fechas', methods=['POST']) 
@login_required
def api_calcular_fechas():
    from datetime import datetime
    from flask import request, jsonify, current_app
    
    data = request.get_json() or {}
    fecha_inicio_str = data.get('fecha_inicio')
    vigencia = data.get('vigencia')

    if not fecha_inicio_str or not vigencia:
        return jsonify({"ok": False, "error": "Faltan parámetros: fecha_inicio o vigencia."}), 400

    try:
        fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').date()
        
        # Usamos la función de negocio ya definida (que maneja DEMO 15 días)
        vence_en, proximo_pago = calcular_fechas_vigencia(fecha_inicio, vigencia)
        
        return jsonify({
            "ok": True,
            "vence_en": vence_en.isoformat(),
            "proximo_pago": proximo_pago.isoformat()
        })
        
    except Exception as e:
        current_app.logger.error(f"Error al calcular fechas: {e}")
        return jsonify({"ok": False, "error": f"Error interno al calcular las fechas: {str(e)}"}), 500


@app.route('/api/precio_paquete')
def api_precio_paquete():
    pais = request.args.get('pais')
    paquete = request.args.get('paquete')
    vigencia = request.args.get('vigencia')
    es_sucursal = request.args.get('es_sucursal', 'false').lower() == 'true'

    if not pais or not paquete or not vigencia:
        return jsonify({"error": "Faltan datos"}), 400

    registro = PaquetePrecio.query.filter_by(
        pais=pais,
        paquete=paquete,
        vigencia=vigencia
    ).first()

    if not registro:
        registro = PaquetePrecio.query.filter(
            PaquetePrecio.pais == pais,
            PaquetePrecio.paquete.ilike(f"%{paquete}%"),
            PaquetePrecio.vigencia == vigencia
        ).first()

    if not registro:
        return jsonify({"error": "No se encontró el precio"}), 404

    precio = registro.precio
    if es_sucursal:
        precio *= 0.8 # 20% descuento sucursal
    precio = round(precio / 10) * 10 # redondeo a decena

    return jsonify({"precio": precio, "moneda": registro.moneda})

@app.route('/enviar_whatsapp_cliente/<int:cliente_id>', methods=['POST', 'GET'])
@login_required
def enviar_whatsapp_cliente(cliente_id):
    cliente = Cliente.query.get_or_404(cliente_id)
    
    # Verificar si tiene número
    if not cliente.telefono:
        flash("El cliente no tiene número principal registrado.", "warning")
    return redirect(url_for('clientes_list'))

@app.route('/api/clientes_dt')
@login_required
def api_clientes_dt():
    from datetime import date
    from sqlalchemy import select, outerjoin

    today = date.today()

    # 🔹 Consulta combinada Cliente + Suscripcion
    stmt = (
        select(
            Cliente.id,
            Cliente.negocio,
            Cliente.nombre_contacto,
            Cliente.telefono,
            Cliente.telefono_secundario_1,
            Cliente.telefono_secundario_2,
            Cliente.telefono_secundario_3,
            Cliente.mail,
            Cliente.pais,
            Cliente.status_cliente,
            Cliente.fecha_pago,
            Suscripcion.id_gumi,
            Suscripcion.server,
            Suscripcion.paquete,
            Suscripcion.status,
            Suscripcion.proximo_pago,
            Suscripcion.vence_en
        )
        .select_from(outerjoin(Cliente, Suscripcion, Cliente.id == Suscripcion.cliente_id))
        .order_by(Cliente.negocio.asc())
    )

    results = db.session.execute(stmt).all()
    rows = []

    for row in results:
        (
            cid, negocio, nombre_contacto, tel, tel1, tel2, tel3, mail, pais,
            status_cliente, fecha_pago, id_gumi, server, paquete,
            status_sus, proximo_pago, vence_en
        ) = row

        # 🛑 LÓGICA SIMPLIFICADA VIGENTE/VENCIDA INTEGRADA 🛑
        
        status_pago_info = {"status": "SIN SUSCRIPCIÓN", "color": "bg-secondary"}
        
        if proximo_pago:
            dias = (proximo_pago - today).days
            
            if dias >= 0:
                # Si es hoy o futuro
                status_pago_info = {"status": "VIGENTE", "color": "bg-success"}
            else:
                # Si la fecha ya pasó
                status_pago_info = {"status": "VENCIDA", "color": "bg-danger"}
        
        # -------------------------------------------------------------
        
        status = status_sus or status_cliente or "Activo"
        is_eliminado = (status_sus or status_cliente) == 'Eliminado'

        rows.append({
            "id": cid,
            "id_gumi": id_gumi or "",
            "server": server or "",
            "pais": pais or "",
            "status": status,
            "negocio": negocio,
            "nombre_contacto": nombre_contacto,
            "telefono": tel or "",
            "tel_sec_1": tel1 or "",
            "tel_sec_2": tel2 or "",
            "tel_sec_3": tel3 or "",
            "mail": mail or "",
            "paquete": paquete or "",
            "ultimo_pago": fecha_pago.isoformat() if fecha_pago else "",
            "proximo_pago": proximo_pago.isoformat() if proximo_pago else "",
            "status_pago_info": status_pago_info,
        })

    return jsonify({"data": rows})


@app.route('/pagos')
@login_required
def pagos_list_global():
    from sqlalchemy import extract
    
    # Obtener años únicos de los pagos para el filtro
    years = db.session.query(db.extract('year', Pago.fecha_pago).distinct().label('year')) \
             .filter(Pago.fecha_pago.isnot(None)) \
             .order_by(db.desc('year')) \
             .all()
    
    unique_years = [r.year for r in years]
    
    # Mapeo de meses (para el frontend)
    months_map = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    
    return render_template('pagos_list.html', 
                           unique_years=unique_years,
                           months_map=months_map)



@app.route('/api/pagos_dt_global')
@login_required
def api_pagos_dt_global():
    from sqlalchemy import select, outerjoin, extract
    from flask import url_for 
    import traceback

    year_filter = request.args.get('year', type=int)
    month_filter = request.args.get('month', type=int)
    
    # MAPEO DE MONEDAS POR PAÍS (Ajusta esta lista según tus clientes)
    CURRENCY_MAP = {
        'MÉXICO': 'MXN',
        'COLOMBIA': 'COP',
        'PERÚ': 'PEN',
        'ESTADOS UNIDOS': 'USD',
        # Agrega otros países/monedas si es necesario
    }
    DEFAULT_CURRENCY = 'MXN' # Moneda de fallback si no se encuentra ninguna referencia

    try:
        # 1. Construir la consulta base
        stmt = (
            select(
                Pago.id, Pago.fecha_pago, Pago.paquete, Pago.vigencia, Pago.monto, Pago.moneda, Pago.metodo_pago, 
                Pago.factura_pago, Pago.numero_factura, Pago.motivo_descuento, Pago.status.label('status_pago'), 
                
                Cliente.id.label('cliente_id_for_link'),
                Cliente.negocio.label('cliente_negocio'),
                Cliente.pais.label('cliente_pais'), # Dato necesario para el fallback de moneda
                Cliente.nombre_contacto.label('cliente_contacto'),
                Suscripcion.id_gumi.label('suscripcion_id_gumi'),
                Suscripcion.server.label('server_info')
            )
            .outerjoin(Cliente, Pago.cliente_id == Cliente.id) 
            .outerjoin(Suscripcion, Pago.cliente_id == Suscripcion.cliente_id)
            .where(Pago.status == 'ACTIVO') 
            .order_by(Pago.fecha_pago.desc())
        )

        # 2. Filtros
        if year_filter:
            stmt = stmt.where(extract('year', Pago.fecha_pago) == year_filter)
        
        if month_filter:
            stmt = stmt.where(extract('month', Pago.fecha_pago) == month_filter)
        
        # 3. Ejecutar
        results = db.session.execute(stmt).all()

        # 4. Diccionario de meses en español
        meses_abbr = {
            1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
            7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
        }

        rows = []
        for r in results:
            # --- LÓGICA DE MONEDA: Jalar el dato o usar fallback ---
            moneda_final = r.moneda
            if not moneda_final:
                # Si Pago.moneda es NULL, intentamos inferir del País del Cliente
                pais_limpio = (r.cliente_pais or '').upper().strip()
                moneda_final = CURRENCY_MAP.get(pais_limpio, DEFAULT_CURRENCY)
            # --- FIN LÓGICA DE MONEDA ---


            # --- FORMATO DE FECHA AMIGABLE ---
            if r.fecha_pago:
                dia = r.fecha_pago.day
                mes = meses_abbr.get(r.fecha_pago.month, "")
                anio = r.fecha_pago.year
                fecha_display = f"{dia} {mes} {anio}" # Ej: 30 nov 2025
                fecha_iso = r.fecha_pago.strftime("%Y-%m-%d") # Ej: 2025-11-30
            else:
                fecha_display = ""
                fecha_iso = ""

            # Formatos auxiliares
            facturado = "Sí" if r.factura_pago or (r.numero_factura and str(r.numero_factura).strip()) else "No"
            monto_str = f"{r.monto:,.2f}" if r.monto is not None else "0.00"
            
            # Generar enlace al detalle del cliente
            cliente_id_link = r.cliente_id_for_link 
            cliente_negocio_display = r.cliente_negocio or "Cliente Eliminado"
            
            negocio_link = cliente_negocio_display 
            
            if cliente_id_link:
                url_detalle = url_for('cliente_detalle', cliente_id=cliente_id_link) 
                negocio_link = f'<a href="{url_detalle}">{cliente_negocio_display}</a>'
            
            rows.append({
                "id": r.id,
                
                "fecha_pago": fecha_display,
                "fecha_pago_orden": fecha_iso,

                "paquete": r.paquete or "—",
                "vigencia": r.vigencia or "—",
                "monto": monto_str,
                "moneda": moneda_final, # <--- Siempre será un valor de moneda válido
                "metodo_pago": r.metodo_pago or "—", 
                "motivo_descuento": r.motivo_descuento or "—",
                "num_factura": r.numero_factura or "—",
                "facturado_str": facturado,
                "id_gumi": r.suscripcion_id_gumi or "—",
                "server": r.server_info or "—",
                "pais_cliente": r.cliente_pais or "—", 
                "negocio_link": negocio_link, 
                "contacto": r.cliente_contacto or "—",
            })

        return jsonify({"data": rows})

    except Exception as e:
        # Es buena práctica registrar el error completo en el log
        print(f"Error en api_pagos_dt_global: {traceback.format_exc()}")
        return jsonify({"data": []}), 500

# ========== API: Agregar pago ==========
def _parse_monto(m):
    """Acepta strings tipo 'MXN 1,234.50' o '$1,234' y regresa float seguro."""
    if m is None:
        return 0.0
    if isinstance(m, (int, float)):
        return float(m)
    s = str(m)
    # Deja solo dígitos, puntos y comas
    s = ''.join(ch for ch in s if ch.isdigit() or ch in '.,')
    # Si usa coma como decimal (y sin punto), normaliza a punto
    if s.count(',') == 1 and s.count('.') == 0:
        s = s.replace(',', '.')
    # Quita miles (comas)
    s = s.replace(',', '')
    try:
        return float(s) if s else 0.0
    except:
        return 0.0


@app.route('/api/pagos/nuevo', methods=['POST'])
@login_required
def api_nuevo_pago():
    
    data = request.get_json() or {}
    cliente_id = data.get('cliente_id')
    if not cliente_id:
        return jsonify({"error": "Falta cliente_id"}), 400

    cliente = Cliente.query.get_or_404(cliente_id)

    # 1. Extraer TODOS los datos del modal
    fecha_pago_str   = data.get('fecha_pago')
    monto            = _parse_monto(data.get('monto'))
    metodo_pago      = data.get('metodo_pago') or None
    otro_metodo      = data.get('otro_metodo_pago') or None
    factura_pago     = bool(data.get('factura_pago', False))
    numero_factura   = data.get('numero_factura') or None
    motivo_descuento = data.get('motivo_descuento') or None
    
    ### CORRECCIÓN CLAVE: El frontend envía 'paquete_id', no 'paquete' ###
    paquete_precio_id = data.get('paquete_id') 
    
    # 🛑 FIX CRÍTICO: Generación de bank_transaction_id para pagos manuales
    provided_bank_id = data.get('bank_transaction_id')
    
    if provided_bank_id is None or provided_bank_id == '':
        # Caso: Pago manual. Generamos el ID negativo único buscando el mínimo.
        
        # 1. BUSCAR EL VALOR MÍNIMO (más negativo) de bank_transaction_id
        min_manual_id = db.session.query(
            func.min(Pago.bank_transaction_id)
        ).filter(Pago.bank_transaction_id < 0).scalar() # Buscamos el ID más negativo

        # 2. Asignar el nuevo ID. Si min_manual_id es None, empezamos en -1.
        if min_manual_id is None:
            final_bank_transaction_id = -1
        else:
            final_bank_transaction_id = int(min_manual_id) - 1 
    else:
        # Caso: Pago de conciliación.
        final_bank_transaction_id = int(provided_bank_id)

    try:
        fecha_pago = date.fromisoformat(fecha_pago_str)
    except Exception:
        fecha_pago = date.today()

    # 2. Buscar datos del paquete y la suscripción
    sus = Suscripcion.query.filter_by(cliente_id=cliente.id).first()
    
    # Variables para guardar en el PAGO
    pp = None
    paquete_nombre = None
    vigencia_nombre = None
    moneda_a_usar = None 
    paquete_encontrado_en_pp = False # Flag para saber si se usará el paquete nuevo o el de la suscripción anterior

    if paquete_precio_id:
        try:
            # 1. Buscamos el objeto PaquetePrecio
            pp = PaquetePrecio.query.get(int(paquete_precio_id))
            
            # 2. Si lo encontramos, asignamos todos los datos de una vez
            if pp:
                paquete_nombre = pp.paquete
                vigencia_nombre = pp.vigencia
                moneda_a_usar = pp.moneda
                paquete_encontrado_en_pp = True
                
        except Exception as e:
            # Manejamos errores de conversión (si el ID no es válido)
            print(f"Error al buscar PaquetePrecio con ID {paquete_precio_id}: {e}")
            pp = None
    
    ### CORRECCIÓN EN EL FALLBACK: Usar el paquete anterior SOLO si NO se encontró un paquete nuevo ###
    if not paquete_encontrado_en_pp and sus:
        paquete_nombre = sus.paquete
        vigencia_nombre = sus.vigencia
        # NOTA: No se actualiza la moneda si se usa el fallback
    
    # 3. CREAR EL PAGO
    pago = Pago(
        nombre=cliente.nombre_contacto,
        correo=cliente.mail,
        monto=monto,
        numero_whatsapp=cliente.telefono,
        cliente_id=cliente.id,
        fecha_pago=fecha_pago,
        
        bank_transaction_id=final_bank_transaction_id,
        
        metodo_pago=metodo_pago,
        otro_metodo_pago=otro_metodo,
        factura_pago=factura_pago,
        numero_factura=numero_factura,
        # Se guarda el nombre y vigencia que se hayan determinado (nuevo o fallback)
        paquete=paquete_nombre,
        vigencia=vigencia_nombre, 
        motivo_descuento=motivo_descuento,
        moneda=moneda_a_usar
    )
    db.session.add(pago)

    # 4. ACTUALIZAR AL CLIENTE (para "último pago")
    cliente.fecha_pago              = fecha_pago
    cliente.metodo_pago           = metodo_pago
    cliente.otro_metodo_pago      = otro_metodo
    cliente.factura_pago          = factura_pago
    cliente.numero_factura        = numero_factura
    cliente.motivo_descuento      = motivo_descuento

    # 5. ACTUALIZAR LA SUSCRIPCIÓN

    if sus and vigencia_nombre:
        
        hoy = date.today()

        # 1. Determinar la fecha de inicio del CÁLCULO (Regla: Histórico vs Próximo Pago)
        
        # Si la suscripción está expirada o es nueva, la base es la fecha del recibo (fecha_pago).
        if sus.proximo_pago and sus.proximo_pago > hoy:
            fecha_inicio_calculo = sus.proximo_pago
        else:
            fecha_inicio_calculo = fecha_pago 
        
        # 2. Aplicar la Regla de Negocio: Usar la fecha del recibo si es posterior (Pago Anticipado)
        if fecha_pago > fecha_inicio_calculo:
            fecha_inicio_calculo = fecha_pago

        # 3. Recalculamos las fechas!
        vence_en, proximo_pago = calcular_fechas_vigencia(fecha_inicio_calculo, vigencia_nombre)
        
        # 4. ¡Actualizamos la suscripción!
        sus.vence_en = vence_en
        sus.proximo_pago = proximo_pago
        
        # Si se seleccionó un paquete diferente (o el mismo, pero lo encontramos), actualizamos los detalles
        if pp:
            sus.paquete = pp.paquete
            sus.vigencia = pp.vigencia
        
        ### CORRECCIÓN NAN: Si proximo_pago es None (falló el cálculo) se debe manejar ###
        if not proximo_pago:
            # Aquí puedes lanzar un error o simplemente no actualizar
            print(f"ADVERTENCIA: Falló el cálculo de vigencia para {vigencia_nombre}. No se actualizará la fecha de próximo pago.")
            # Si quieres que el usuario vea un error:
            # raise Exception(f"No se pudo calcular la próxima vigencia con el valor: {vigencia_nombre}")
            
    # 6. Guardar todo
    try:
        db.session.commit()
        return jsonify({"ok": True, "msg": "Pago registrado correctamente"})
    except Exception as e:
        db.session.rollback()
        error_message = str(e)
        if hasattr(e, 'orig') and hasattr(e.orig, 'args') and e.orig.args:
            error_message = f"Error SQL: {e.orig.args[0]}"
            
        return jsonify({"ok": False, "error": f"Error al guardar el pago: {error_message}"}), 500

@app.route('/api/pagos/agregar/<int:cliente_id>', methods=['POST'])
@login_required
def api_agregar_pago(cliente_id):
    data = request.get_json()
    cliente = Cliente.query.get_or_404(cliente_id)

    nuevo_pago = Pago(
        nombre=cliente.nombre_contacto,
        correo=cliente.mail,
        monto=float(data.get('monto', 0)),
        numero_whatsapp=cliente.telefono,
        cliente_id=cliente.id,
        fecha_pago=date.fromisoformat(data.get('fecha_pago'))
    )
    db.session.add(nuevo_pago)
    db.session.commit()

    return jsonify({"ok": True})


@app.route('/api/cliente_pago/<int:cliente_id>')
@login_required
def api_cliente_pago(cliente_id):
    """Devuelve datos para el modal de AGREGAR PAGO:
        - negocio, país
        - paquete/vigencia actual (de Suscripcion)
        - lista de paquetes disponibles para el país (con precio y moneda)
    """
    cliente = Cliente.query.get_or_404(cliente_id)
    pais = (cliente.pais or "").upper().strip()

    sus = Suscripcion.query.filter_by(cliente_id=cliente_id).first()
    paquete_actual = (sus.paquete if sus else None)
    vigencia_actual = (sus.vigencia if sus else None)

    # Trae todos los paquetes del país; excluye variantes de sucursal
    registros = (
        PaquetePrecio.query
        .filter(PaquetePrecio.pais == pais)
        .order_by(PaquetePrecio.paquete.asc(), PaquetePrecio.vigencia.asc())
        .all()
    )

    paquetes = []
    paquete_id_actual = None
    for r in registros:
        nombre = r.paquete or ""
        # Excluir combos de sucursal si los hubiera
        if "(Sucursal" in nombre or "Sucursal)" in nombre:
            continue

        paquetes.append({
            "id": r.id,
            "nombre": nombre,
            "vigencia": r.vigencia,
            "precio": float(r.precio or 0),
            "moneda": r.moneda or "MXN",
        })

        if paquete_actual and vigencia_actual:
            if nombre == paquete_actual and r.vigencia == vigencia_actual:
                paquete_id_actual = r.id

    return jsonify({
        "ok": True,
        "data": {
            "negocio": cliente.negocio,
            "pais": pais,
            "paquete_actual": paquete_actual,
            "paquete_id_actual": paquete_id_actual,
            "paquetes": paquetes
        }
    })

# ========== VISTA: Detalle de cliente ==========
from datetime import date
# Asegúrate de que las funciones auxiliares (calcular_status_pago, get_status_principal_color)
# y la clase Suscripcion sean accesibles aquí.

@app.route('/clientes/<int:cliente_id>/detalle')
@login_required
def cliente_detalle(cliente_id):
    cliente = Cliente.query.get_or_404(cliente_id) 
    
    # Se busca la suscripción activa o más reciente
    sus = Suscripcion.query.filter_by(cliente_id=cliente_id).first() 

    hoy = date.today()
    status_principal_color = 'bg-secondary' # Default si no hay suscripción
    
    # Si existe una suscripción, calculamos y adjuntamos el info
    if sus:
        # 🛑 1. Adjuntar el STATUS PAGO INFO al objeto suscripcion
        # (Usamos el helper que ya tienes definido)
        sus.status_pago_info = calcular_status_pago(sus.proximo_pago if sus else None, hoy)
        
        # 🛑 2. FIX CRÍTICO: Adjuntar el ID del paquete para el modal de pago
        # Si el error persiste, la columna en tu modelo Suscripcion se llama diferente.
        # Podría ser `paquete_id` o `id_paquete_precio`.
        try:
            # Intenta usar el nombre que debería ser el correcto (paquete_precio_id)
            paquete_id_a_usar = sus.paquete_precio_id
        except AttributeError:
            # Si falla, intenta usar un nombre alternativo o None. 
            # Si tu FK se llama 'paquete_id' en Suscripcion, descomenta y usa la siguiente línea:
            # paquete_id_a_usar = sus.paquete_id 
            paquete_id_a_usar = None
            # Si el error persiste, NECESITAS revisar el nombre exacto de la columna en tu modelo Suscripcion.
        
        sus.paquete_precio_id_actual = paquete_id_a_usar
        
        # Obtener el color del status principal
        status_principal_color = get_status_principal_color(sus.status)
    
    return render_template(
        'cliente_detalle.html',
        cliente=cliente,
        suscripcion=sus,
        hoy=hoy,
        status_principal_color=status_principal_color
    )



@app.route('/api/pagos_cliente_v2/<int:cliente_id>')
@login_required
def api_pagos_cliente_v2(cliente_id):
    """
    Devuelve los pagos registrados del cliente en formato JSON (para DataTables).
    Lee la información de CADA pago individual.
    """
    cliente = Cliente.query.get_or_404(cliente_id)
    pagos = Pago.query.filter_by(
        cliente_id=cliente_id,
    ).order_by(Pago.fecha_pago.desc()).all()
    
    # Define una moneda de fallback por país (para el monto)
    pais = (cliente.pais or "MÉXICO").upper()
    moneda = "MXN" if pais == "MÉXICO" else ("COP" if pais == "COLOMBIA" else "USD")

    data = []
    for p in pagos: # 'p' es cada objeto Pago individual
        
        # Leemos los datos directamente de 'p' (el pago)
        facturado_pago = "Sí" if (p.factura_pago or (p.numero_factura and str(p.numero_factura).strip())) else "No"

        data.append({
            "id": p.id,
            "fecha_pago": p.fecha_pago.strftime("%Y-%m-%d") if p.fecha_pago else "",
            "paquete": p.paquete or "—",
            "vigencia": p.vigencia or "—",
            "monto": f"{p.monto:,.2f}",
            "motivo_descuento": p.motivo_descuento or "",
            "moneda": moneda, # La moneda sigue siendo general
            "metodo_pago": p.metodo_pago or "—",
            "facturado": facturado_pago,
            "numero_factura": p.numero_factura or "",
            "status": p.status
        })

    return jsonify({"data": data})


# TASA DE CONVERSIÓN FIJA PARA DASHBOARD (Para unificar a MXN)
TASA_USD_MXN = 18.0
TASA_COP_MXN = 0.0045 

def convertir_a_mxn(monto, moneda):
    """Convierte un monto dado a MXN usando tasas fijas."""
    moneda_upper = (moneda or 'MXN').upper()
    
    # 🛑 FIX: Si el monto es None o no convertible, retorna 0.0
    if monto is None:
        return 0.0
    try:
        monto = Decimal(monto)
    except Exception:
        return 0.0

    if moneda_upper == 'MXN':
        return float(monto)
    if moneda_upper == 'USD':
        return float(monto * Decimal(str(TASA_USD_MXN)))
    if moneda_upper == 'COP':
        return float(monto * Decimal(str(TASA_COP_MXN)))
    return float(monto)

@app.route('/api/dashboard_data')
@login_required
def api_dashboard_data():
    from sqlalchemy import func, extract, and_
    from datetime import date
    from decimal import Decimal
    from flask import current_app
    from dateutil.relativedelta import relativedelta # Asegúrate de que esta importación exista en app.py
    import traceback
    
    # 🛑 NOTA: Asumimos que convertir_a_mxn(monto, moneda) está definida globalmente
    # y maneja correctamente la conversión a Decimal de los montos a MXN.
    # Si la moneda es MXN, debe devolver el monto original.

    try:
        # --- 1. CAPTURAR Y PREPARAR FILTROS ---
        anio_filtro = request.args.get('anio', type=int)
        mes_filtro = request.args.get('mes', type=int)
        pais_filtro = request.args.get('pais')
        server_filtro = request.args.get('server')
        paquete_filtro = request.args.get('paquete')
        
        # --- 2. CONSULTA BASE DE PAGOS (Filtrada) ---
        q_pagos = db.session.query(Pago).join(Cliente, Pago.cliente_id == Cliente.id).outerjoin(Suscripcion, Pago.cliente_id == Suscripcion.cliente_id).filter(Pago.status == 'ACTIVO')
        
        if anio_filtro:
            q_pagos = q_pagos.filter(extract('year', Pago.fecha_pago) == anio_filtro)
        if mes_filtro:
            q_pagos = q_pagos.filter(extract('month', Pago.fecha_pago) == mes_filtro)
        
        # Aplicar filtros de Suscripcion/Cliente a los Pagos
        if pais_filtro:
            q_pagos = q_pagos.filter(Cliente.pais == pais_filtro)
        if server_filtro:
            q_pagos = q_pagos.filter(Suscripcion.server == server_filtro)
        if paquete_filtro:
            q_pagos = q_pagos.filter(Pago.paquete.ilike(f'%{paquete_filtro}%'))


        # --- 3. CONSULTA BASE DE SUSCRIPCIONES ACTIVAS (Filtrada) ---
        q_suscripciones_activas = db.session.query(Suscripcion).join(Cliente, Cliente.id == Suscripcion.cliente_id).filter(
            Suscripcion.status == 'Activo'
        )
        # Aplicar filtros de Suscripcion/Cliente a las Suscripciones
        if pais_filtro: q_suscripciones_activas = q_suscripciones_activas.filter(Cliente.pais == pais_filtro)
        if server_filtro: q_suscripciones_activas = q_suscripciones_activas.filter(Suscripcion.server == server_filtro)
        if paquete_filtro: q_suscripciones_activas = q_suscripciones_activas.filter(Suscripcion.paquete.ilike(f'%{paquete_filtro}%'))


        # --- 4. CÁLCULO DE KPIS NUMÉRICOS ---
        
        # A. Ingresos Totales y Segmentados (Suma y Conversión a MXN)
        total_ingresos_mxn = 0.0          # Ingresos Totales (Global)
        ingresos_mx_only = 0.0            # NUEVO: Ingresos solo en MXN
        ingresos_latam_mxn = 0.0          # NUEVO: Ingresos de otras monedas (COP, USD, etc.) convertidos a MXN
        pagos_filtrados = q_pagos.all()
        
        for pago in pagos_filtrados:
            monto_mxn = convertir_a_mxn(pago.monto, pago.moneda)
            total_ingresos_mxn += monto_mxn
            
            moneda_check = (pago.moneda or '').upper().strip()
            
            if moneda_check == 'MXN':
                ingresos_mx_only += monto_mxn
            else:
                # Todo lo que no sea MXN se considera LATAM/Otro (convertido a MXN)
                ingresos_latam_mxn += monto_mxn
        
        # NUEVO KPI 3: Número de Pagos
        total_payments_count = len(pagos_filtrados)

        # B. Clientes Activos y Suspendidos
        total_clientes_activos = q_suscripciones_activas.count()
        
        # Clientes Suspendidos (Aplicar los mismos filtros)
        q_suspendidos = db.session.query(Suscripcion).join(Cliente, Cliente.id == Suscripcion.cliente_id).filter(Suscripcion.status == 'Suspendido')
        if pais_filtro: q_suspendidos = q_suspendidos.filter(Cliente.pais == pais_filtro)
        if server_filtro: q_suspendidos = q_suspendidos.filter(Suscripcion.server == server_filtro)
        if paquete_filtro: q_suspendidos = q_suspendidos.filter(Suscripcion.paquete.ilike(f'%{paquete_filtro}%'))
        total_suspendidos = q_suspendidos.count()

        # NUEVO KPI 4: DEMOs Activos (Clientes 'En prueba' por PAQUETE)
        # 🛑 CORRECCIÓN CRÍTICA: Filtra por PAQUETE.ilike('%demo%') Y STATUS == 'Activo'
        q_demos_activos = db.session.query(Suscripcion).join(Cliente, Cliente.id == Suscripcion.cliente_id).filter(
            # Debe ser activo Y el nombre del paquete debe contener 'demo'
            and_(
                Suscripcion.status == 'Activo',
                Suscripcion.paquete.ilike('%demo%')
            )
        )
        # Aplicar filtros de localización/servidor
        if pais_filtro: q_demos_activos = q_demos_activos.filter(Cliente.pais == pais_filtro)
        if server_filtro: q_demos_activos = q_demos_activos.filter(Suscripcion.server == server_filtro)
        
        # Solo aplicar filtro de paquete si el filtro ES 'Demo' o similar,
        # pero como ya filtramos por '%demo%', solo nos interesa si el usuario NO ha filtrado por otro paquete.
        # Si se filtra por 'Clínica', la cuenta de demos debe ser 0.
        if paquete_filtro and 'demo' not in paquete_filtro.lower():
            # Si el usuario filtra por un paquete que NO es demo, el conteo de demos debe ser 0.
            total_demos_activos = 0
        else:
            total_demos_activos = q_demos_activos.count()


        # C. Antigüedad Promedio (Calculado en memoria con la query filtrada)
        total_antiguedad_dias = 0
        clientes_antiguedad = q_suscripciones_activas.all() 
        
        hoy = date.today()
        for sus in clientes_antiguedad:
            # 🛑 FIX: Manejar fecha_inicio nula
            if sus.fecha_inicio:
                dias_activo = (hoy - sus.fecha_inicio).days
                total_antiguedad_dias += dias_activo

        antiguedad_promedio_meses = 0
        if total_clientes_activos > 0:
            antiguedad_promedio_meses = round((total_antiguedad_dias / total_clientes_activos) / 30.44, 1)

        # D. Porcentaje Facturado
        total_pagos_conteo = len(pagos_filtrados) 
        total_pagos_facturados = sum(1 for p in pagos_filtrados if p.factura_pago)
        pct_facturado = 0
        if total_pagos_conteo > 0:
            pct_facturado = round((total_pagos_facturados / total_pagos_conteo) * 100)


        # --- 5. GRÁFICAS DE DISTRIBUCIÓN (Agrupaciones eficientes) ---

        # E. Top Vigencias (Pagos) - Usamos la lista 'pagos_filtrados' en memoria
        vigencias_temp = {}
        for pago in pagos_filtrados:
            vig = pago.vigencia or 'N/A'
            vigencias_temp[vig] = vigencias_temp.get(vig, 0) + 1
        vigencias_data = vigencias_temp

        # F. Top Paquetes (Suscripciones Activas) - Agrupación en DB (más eficiente)
        # NOTA: La lógica de aquí ya debe incluir los paquetes 'Demo' si su status es 'Activo'
        paquetes_raw = db.session.query(Suscripcion.paquete, func.count(Suscripcion.id)).filter(
            Suscripcion.id.in_(
                q_suscripciones_activas.with_entities(Suscripcion.id).subquery()
            )
        ).group_by(Suscripcion.paquete).all()

        paquetes_labels = ['Abeja', 'Clínica', 'Iguana', 'Chango', 'Elefante', 'Demo']
        paquetes_data = [0] * len(paquetes_labels)
        
        for nombre, count in paquetes_raw:
            if not nombre: continue
            nombre_clean = nombre.split(' ')[0].strip()
            # 🛑 Para la gráfica, cualquier paquete que contenga 'Demo' lo mapeamos al índice 'Demo'
            if 'demo' in nombre_clean.lower():
                nombre_clean = 'Demo'

            try:
                index = paquetes_labels.index(nombre_clean)
                paquetes_data[index] += count
            except ValueError:
                # Si es un paquete no mapeado, lo ignoramos de la gráfica de top paquetes
                pass 

        # G. Métodos de Pago - Usamos la lista 'pagos_filtrados' en memoria
        metodos_temp = {}
        for pago in pagos_filtrados:
            metodo = pago.metodo_pago or 'Otro'
            metodos_temp[metodo] = metodos_temp.get(metodo, 0) + 1
        metodos_data = metodos_temp

        # H. Carga por Servidor - Agrupación en DB (más eficiente)
        servidores_raw = db.session.query(Suscripcion.server, func.count(Suscripcion.id)).filter(
            Suscripcion.id.in_(
                q_suscripciones_activas.with_entities(Suscripcion.id).subquery()
            )
        ).group_by(Suscripcion.server).order_by(func.count(Suscripcion.id).desc()).all()
        servidores_data = {s[0] or 'N/A': s[1] for s in servidores_raw}

        # I. Tendencia de Ventas (Últimos 6 meses) - Usa la base 'q_pagos' para aplicar filtros
        tendencia = []
        meses_tendencia = []
        
        hoy = date.today()
        for i in range(6):
            target_date = hoy - relativedelta(months=5 - i)
            target_month = target_date.month
            target_year = target_date.year
            
            q_tendencia = db.session.query(Pago).filter(
                Pago.status == 'ACTIVO',
                extract('year', Pago.fecha_pago) == target_year,
                extract('month', Pago.fecha_pago) == target_month
            ).join(Cliente, Pago.cliente_id == Cliente.id).outerjoin(Suscripcion, Pago.cliente_id == Suscripcion.cliente_id)

            if pais_filtro: q_tendencia = q_tendencia.filter(Cliente.pais == pais_filtro)
            if server_filtro: q_tendencia = q_tendencia.filter(Suscripcion.server == server_filtro)
            if paquete_filtro: q_tendencia = q_tendencia.filter(Pago.paquete.ilike(f'%{paquete_filtro}%'))
            
            pagos_mes = q_tendencia.all()
            
            ingresos_mes = 0.0
            for pago in pagos_mes:
                ingresos_mes += convertir_a_mxn(pago.monto, pago.moneda)
            
            mes_str = target_date.strftime('%b')
            meses_tendencia.append(mes_str)
            tendencia.append(round(ingresos_mes))

        # --- 6. ARMADO DEL JSON FINAL ---
        final_data = {
            "kpi": {
                # KPIS EXISTENTES
                "ingresos": round(total_ingresos_mxn, 2), # Ingresos Globales (para la primera tarjeta)
                "clientes": total_clientes_activos,       # Clientes Activos (para la segunda tarjeta)
                "antiguedad": antiguedad_promedio_meses, # Antigüedad Promedio
                "pct_facturado": pct_facturado,          # % Facturado
                "suspendidos": total_suspendidos,        # Suspendidos (por si se necesita)
                
                # NUEVOS KPIS SEGMENTADOS
                "ingresos_mx": round(ingresos_mx_only, 2),
                "ingresos_latam": round(ingresos_latam_mxn, 2),
                "num_pagos": total_payments_count,
                "demos_activos": total_demos_activos
            },
            "ventas": {
                "labels": meses_tendencia,
                "data": tendencia
            },
            "vigencias": {
                "labels": list(vigencias_data.keys()),
                "data": list(vigencias_data.values())
            },
            "paquetes": {
                "labels": paquetes_labels,
                "data": paquetes_data
            },
            "metodos": {
                "labels": list(metodos_data.keys()),
                "data": list(metodos_data.values())
            },
            "facturacion": {
                "data": [total_pagos_facturados, total_pagos_conteo - total_pagos_facturados]
            },
            "servidores": {
                "labels": list(servidores_data.keys()),
                "data": list(servidores_data.values())
            }
        }
        
        return jsonify(final_data)

    except Exception as e:
        # 🛑 CRÍTICO: Capturamos la excepción, la logueamos y devolvemos un JSON de error
        current_app.logger.error(f"Error fatal en api_dashboard_data: {traceback.format_exc()}")
        # Devolvemos un JSON vacío para que el frontend no rompa
        return jsonify({
            "kpi": {
                "ingresos": 0, "clientes": 0, "antiguedad": 0, "pct_facturado": 0, "suspendidos": 0,
                "ingresos_mx": 0, "ingresos_latam": 0, "num_pagos": 0, "demos_activos": 0
            },
            "ventas": {"labels": [], "data": []},
            "vigencias": {"labels": [], "data": []},
            "paquetes": {"labels": [], "data": []},
            "metodos": {"labels": [], "data": []},
            "facturacion": {"data": [0, 0]},
            "servidores": {"labels": [], "data": []},
            "error": "Error interno al procesar los datos."
        }), 500


@app.route('/api/pago/<int:id_pago>')
@login_required
def api_get_pago(id_pago):
    """ Devuelve los detalles de un pago específico para el modal de edición. """
    pago = Pago.query.get_or_404(id_pago)
    cliente = Cliente.query.get_or_404(pago.cliente_id)

    # Buscar el ID del paquete/precio actual de este pago
    # Esto es complejo porque el nombre del paquete se guardó como string.
    # Hacemos una búsqueda "mejor esfuerzo"
    paquete_precio_id = None
    if cliente.pais and pago.paquete and pago.vigencia:
        pp = PaquetePrecio.query.filter_by(
            pais=cliente.pais,
            paquete=pago.paquete,
            vigencia=pago.vigencia
        ).first()
        if pp:
            paquete_precio_id = pp.id

    data = {
        "id": pago.id,
        "fecha_pago": pago.fecha_pago.isoformat() if pago.fecha_pago else "",
        "monto_str": f"{pago.moneda or 'MXN'} {pago.monto:,.2f}", # Formateamos monto
        "monto_num": pago.monto,
        "metodo_pago": pago.metodo_pago or "",
        "otro_metodo_pago": pago.otro_metodo_pago or "",
        "factura_pago": pago.factura_pago or False,
        "numero_factura": pago.numero_factura or "",
        "motivo_descuento": pago.motivo_descuento or "",
        "paquete_precio_id_actual": paquete_precio_id # El ID del <select>
    }
    return jsonify({"ok": True, "data": data})


@app.route('/api/pago/editar/<int:id_pago>', methods=['POST'])
@login_required
def api_editar_pago(id_pago):
    """ Actualiza un registro de pago existente. """
    pago = Pago.query.get_or_404(id_pago)
    data = request.get_json() or {}

    try:
        # Extraer datos del modal
        fecha_pago_str   = data.get('fecha_pago')
        monto            = _parse_monto(data.get('monto')) # Reusa la función
        metodo_pago      = data.get('metodo_pago') or None
        otro_metodo      = data.get('otro_metodo_pago') or None
        factura_pago     = bool(data.get('factura_pago', False))
        numero_factura   = data.get('numero_factura') or None
        motivo_descuento = data.get('motivo_descuento') or None
        paquete_precio_id = data.get('paquete') 

        # Actualizar campos del pago
        pago.fecha_pago = date.fromisoformat(fecha_pago_str) if fecha_pago_str else pago.fecha_pago
        pago.monto = monto
        pago.metodo_pago = metodo_pago
        pago.otro_metodo_pago = otro_metodo
        pago.factura_pago = factura_pago
        pago.numero_factura = numero_factura
        pago.motivo_descuento = motivo_descuento

        # Actualizar paquete y vigencia si cambiaron
        if paquete_precio_id:
            pp = PaquetePrecio.query.get(int(paquete_precio_id))
            if pp:
                pago.paquete = pp.paquete
                pago.vigencia = pp.vigencia

        db.session.commit()
        
        # IMPORTANTE: La edición de un pago NO recalcula la vigencia total.
        # Solo actualiza los datos de ese registro histórico.

        return jsonify({"ok": True, "msg": "Pago actualizado"})

    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    

# ========== API: Cambiar "facturado" (a nivel cliente; refleja en listados) ==========
@app.route('/api/pagos/<int:cliente_id>/factura', methods=['POST'])
@login_required
def api_toggle_factura_cliente(cliente_id):
    """
    Permite cambiar el estado 'facturado' y número de factura del cliente.
    (No existe facturación por pago individual en el modelo actual).
    """
    data = request.get_json() or {}
    cliente = Cliente.query.get_or_404(cliente_id)

    facturado = bool(data.get('facturado', False))
    numero = (data.get('numero_factura') or "").strip()

    cliente.factura_pago = facturado
    cliente.numero_factura = numero if facturado and numero else None

    try:
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500

# app.py (Nueva ruta para Soft Delete de Pago)

@app.route('/api/pagos/<int:pago_id>/soft_delete', methods=['POST'])
@login_required
def soft_delete_pago(pago_id):
    pago = Pago.query.get(pago_id)
    if not pago:
        return jsonify({"ok": False, "error": "Pago no encontrado"}), 404
    
    # Obtener el cliente ID antes de la eliminación lógica
    cliente_id = pago.cliente_id
    
    try:
        # 1. SOFT DELETE: Usamos el campo 'status' para marcar como CANCELADO
        pago.status = 'CANCELADO' 
        
        # IMPORTANTE: Guardamos el estado de cancelado ANTES de recalcular
        db.session.add(pago) 
        db.session.commit() 

        # 2. RECALCULAR TODO EL HISTORIAL (CORRECCIÓN AQUÍ)
        recalcular_vigencia_cliente(cliente_id)
        
        # 3. Respuesta
        return jsonify({"ok": True, "message": "Pago cancelado y fechas recalculadas correctamente."})

    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    

@app.route('/api/pago/<int:pago_id>/factura', methods=['POST'])
@login_required
def api_update_factura_pago(pago_id):
    """
    Endpoint para actualizar el número de factura de un pago in-line.
    """
    data = request.get_json()
    # Captura el nuevo valor. Si es una cadena vacía, se guardará como tal.
    new_factura_number = data.get('numero_factura') 

    pago = Pago.query.get(pago_id)
    if not pago:
        return jsonify({"ok": False, "error": "Pago no encontrado"}), 404

    try:
        # Aquí guardamos el nuevo número de factura
        pago.numero_factura = new_factura_number
        db.session.commit()
        return jsonify({"ok": True, "message": "Factura actualizada"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": "Error interno del servidor"}), 500
    

# app.py

@app.route('/clientes-por-vencer')
@login_required
@role_required(ROLES_LECTURA)
def clientes_por_vencer_list():
    """Muestra la lista de clientes 'Por Vencer' (Activos, No DEMO, Próximos 15 días)."""
    # Asume que tienes una función para obtener países y servidores, similar a la demo list
    servers = [s[0] for s in db.session.query(Suscripcion.server).distinct().all() if s[0]]
    paises = [p[0] for p in db.session.query(Cliente.pais).distinct().all() if p[0]]
    
    # Lista de meses para el filtro (Enero=1, Diciembre=12)
    meses = [
        (1, 'Enero'), (2, 'Febrero'), (3, 'Marzo'), (4, 'Abril'), (5, 'Mayo'), (6, 'Junio'),
        (7, 'Julio'), (8, 'Agosto'), (9, 'Septiembre'), (10, 'Octubre'), (11, 'Noviembre'), (12, 'Diciembre')
    ]

    return render_template(
        'clientes_por_vencer_list.html', 
        servers=servers, 
        paises=paises,
        meses=meses,
        current_year=date.today().year
    )

@app.route('/api/clientes_por_vencer_dt')
@login_required
def api_clientes_por_vencer_dt():
    
    try:
        from datetime import date, timedelta
        from sqlalchemy import or_, and_, extract
        
        # 1. Parámetros
        server_filtro = request.args.get('server')
        pais_filtro = request.args.get('pais')
        mes_vencimiento = request.args.get('month')

        # 2. Fechas
        hoy = date.today()
        fecha_limite = hoy + timedelta(days=15)

        # 3. Consulta Base
        base_query = db.session.query(Cliente, Suscripcion).outerjoin(Suscripcion, Cliente.id == Suscripcion.cliente_id).filter(
            Suscripcion.id.isnot(None)
        ).filter(
            and_(
                Suscripcion.status == 'Activo',
                ~Suscripcion.paquete.ilike('%DEMO%'),
                Suscripcion.vence_en <= fecha_limite
            )
        )

        # 4. Filtros Opcionales
        if server_filtro:
            base_query = base_query.filter(Suscripcion.server == server_filtro)
        if pais_filtro:
            base_query = base_query.filter(Cliente.pais == pais_filtro)
        if mes_vencimiento:
            try:
                mes = int(mes_vencimiento)
                base_query = base_query.filter(extract('month', Suscripcion.vence_en) == mes)
            except ValueError:
                pass 

        # 5. Ejecución
        results = base_query.all()
        
        # Diccionario manual para meses en español (Evita problemas de idioma del servidor)
        meses_abbr = {
            1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
            7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
        }

        # 6. Formato
        data = []
        for cliente, suscripcion in results:
            fecha_vencimiento = suscripcion.vence_en
            
            # Cálculo de días para color
            dias_restantes = (fecha_vencimiento - hoy).days if fecha_vencimiento else 999
            clase_vencimiento = 'fw-bold text-success'
            if dias_restantes < 0: clase_vencimiento = 'fw-bold text-danger'
            elif dias_restantes <= 7: clase_vencimiento = 'fw-bold text-warning'
            
            # Formato de fecha amigable (30 ago 2025)
            if fecha_vencimiento:
                dia = fecha_vencimiento.day
                mes = meses_abbr.get(fecha_vencimiento.month, "")
                anio = fecha_vencimiento.year
                fecha_display_str = f"{dia} {mes} {anio}"
            else:
                fecha_display_str = "N/A"

            # Teléfonos
            telefonos_display = ''
            lista_tels = [cliente.telefono, cliente.telefono_secundario_1, cliente.telefono_secundario_2, cliente.telefono_secundario_3]
            for tel in lista_tels:
                if tel:
                    num = ''.join(filter(str.isdigit, tel))
                    if num: 
                        # Usamos div para lista vertical
                        telefonos_display += f'<div class="mb-1"><a href="https://wa.me/{num}" target="_blank" class="text-decoration-none fw-bold text-success"><i class="fa-brands fa-whatsapp"></i> {tel.strip()}</a></div>'

            data.append({
                'id': cliente.id,
                'negocio': cliente.negocio,
                'pais': cliente.pais,
                'nombre_contacto': cliente.nombre_contacto,
                'telefonos_display': telefonos_display,
                'id_gumi': suscripcion.id_gumi or 'N/A',
                'server': suscripcion.server or 'N/A',
                'paquete_nombre': suscripcion.paquete, 
                'status': suscripcion.status, 
                
                # DATA CRÍTICA: 
                # 1. Para ORDENAR correctamente usamos ISO (YYYY-MM-DD)
                'vence_en_orden': fecha_vencimiento.isoformat() if fecha_vencimiento else '9999-12-31',
                
                # 2. Para MOSTRAR bonito usamos el formato español
                'vence_en_display': f'<span class="{clase_vencimiento}">{fecha_display_str}</span>',
            })

        return jsonify({"data": data})

    except Exception as e:
        return jsonify({"error": str(e), "data": []}), 500
    

@app.route('/conciliacion')
@login_required
def conciliacion_list():
    """Muestra la lista de transacciones bancarias pendientes de conciliar."""
    
    # 1. 🛑 Cálculo de Variables de Contexto 🛑
    
    try:
        # Obtener todos los años únicos de las transacciones
        # FIX: Ahora 'distinct' está importado globalmente
        unique_years = db.session.query(
            distinct(extract('year', BankTransaction.date))
        ).filter(BankTransaction.date != None).all()
        
        # unique_years será una lista de tuplas, la convertimos a una lista de enteros
        unique_years = sorted([int(y[0]) for y in unique_years if y[0] is not None], reverse=True)
        
    except Exception:
        # Si la tabla está vacía o hay un error, usamos los años recientes
        # 🛑 FIX 2: Usar 'datetime.now()' en lugar de 'datetime.datetime.now()'
        unique_years = list(range(datetime.now().year, datetime.now().year - 3, -1))

    # Definición del mapa de meses
    months_map = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
        7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }
    
    now = datetime.now() # FIX: Usar 'datetime.now()'

    # 2. 🛑 Pre-cálculo de URLs (FIX para el error de Jinja) 🛑
    url_transacciones = url_for("api_transacciones_pendientes_dt")
    url_clientes_search = url_for("api_clientes_search")
    url_cliente_pago_base = url_for("api_cliente_pago", cliente_id=0) # Base con marcador
    url_paquetes = url_for("api_paquetes_by_country")
    url_pago_registrar = url_for("pago_registrar")
    
    # Imprime las URLs para que veas si hay codificación extra en tu terminal
    print("-" * 50)
    print("Verificación de URLs (Salida de Python):")
    print(f"URL Transacciones: {url_transacciones}")
    print(f"URL Clientes Search: {url_clientes_search}")
    print(f"URL Cliente Pago BASE: {url_cliente_pago_base}")
    print(f"URL Paquetes: {url_paquetes}")
    print(f"URL Pago Registrar: {url_pago_registrar}")
    print("-" * 50)
    
    
    # 3. Renderizar el template con todas las variables
    return render_template('conciliacion_list.html',
        unique_years=unique_years,
        months_map=months_map,
        now=now,
        # Variables de URL
        url_transacciones=url_transacciones,
        url_clientes_search=url_clientes_search,
        url_cliente_pago_base=url_cliente_pago_base,
        url_paquetes=url_paquetes,
        url_pago_registrar=url_pago_registrar
    )



@app.route('/conciliacion/importar', methods=['GET', 'POST'])
@login_required
@role_required(ROLES_SUPERADMIN)
def conciliacion_importar():
    """Ruta para importar transacciones bancarias desde CSV."""
    import re 
    from decimal import Decimal

    # Función helper para limpiar y obtener Decimal o 0 (CON LOGS DE DEBUG)
    def parse_monto_csv(monto_str):
        
        if pd.isna(monto_str) or not str(monto_str).strip():
            logger.debug(f"CSV Parse: Input '{monto_str}' es vacío/NaN. Resultado 0.00.")
            return Decimal('0.00')

        s = str(monto_str)
        
        # 1. Limpieza inicial de caracteres no deseados ($, ", \t, \xa0, etc.)
        s_cleaned_initial = s.replace('$', '').replace('"', '').replace('\t', '').replace('\xa0', '').strip()
        
        # CRÍTICO: Eliminamos TODOS los espacios internos
        s_no_spaces = s_cleaned_initial.replace(' ', '')
        
        logger.debug(f"CSV Parse (1): Original='{s}' Limpieza Inicial='{s_no_spaces}'")

        # 2. FIX CRÍTICO: Si la cadena final es solo un guion, es un valor nulo (0.00)
        if s_no_spaces == '-':
            logger.debug("CSV Parse FIX: Cadena es solo '-'. Retorna 0.00.")
            return Decimal('0.00')

        # 3. Lógica de separadores (Basado en tu formato: Coma es miles, punto es decimal)
        s_final = s_no_spaces
        
        # Si hay comas y puntos, asumimos formato 1,000.00 y eliminamos la coma
        if s_no_spaces.count(',') > 0 and s_no_spaces.count('.') > 0:
            s_final = s_no_spaces.replace(',', '')
            
        elif s_no_spaces.count(',') == 1 and s_no_spaces.count('.') == 0:
            # Si solo hay una coma (ej: 1,00) asumimos que es separador decimal
            s_final = s_no_spaces.replace(',', '.')
        
        logger.debug(f"CSV Parse (2): Limpieza Separadores='{s_final}'")

        # 4. Intento de conversión y log de debug
        try:
            if not s_final:
                return Decimal('0.00')
            result = Decimal(s_final)
            logger.debug(f"CSV Parse SUCCESS: Resultado final: {result}")
            return result
            
        except Exception as e:
            logger.error(f"CSV Parse FAILURE: La cadena '{s_final}' falló la conversión a Decimal. Error: {e}")
            return Decimal('0.00')

    # --- INICIO DE LA LÓGICA DE LA RUTA ---
    if request.method == 'POST':
        
        # 1. VERIFICAR QUE EL ARCHIVO ESTÉ EN LA PETICIÓN
        if 'archivo_csv' not in request.files:
            flash('No se encontró el archivo en la petición.', 'danger')
            return redirect(url_for('conciliacion_importar'))

        file = request.files['archivo_csv']
        
        # 2. VERIFICAR NOMBRE DEL ARCHIVO
        if file.filename == '':
            flash('Seleccione un archivo.', 'danger')
            return redirect(url_for('conciliacion_importar'))

        # 3. VERIFICAR EXTENSIÓN
        if not file.filename.endswith('.csv'):
            flash('Formato de archivo no soportado. Por favor, sube un archivo CSV.', 'danger')
            return redirect(request.url)
            
        # --- AHORA SE GARANTIZA QUE 'file' ES UN CSV NO VACÍO ---
        try:
            # Lógica de lectura y parseo
            try:
                # Intenta leer el archivo
                df = pd.read_csv(file, encoding='utf-8', thousands=',')
            except UnicodeDecodeError:
                file.seek(0)
                df = pd.read_csv(file, encoding='latin1', thousands=',')
            
            
            df.columns = df.columns.str.upper().str.strip()

            REQUIRED_COLS = ['FECHA', 'CONCEPTO', 'EGRESO', 'INGRESO', 'TOTAL']
            if not all(col in df.columns for col in REQUIRED_COLS):
                missing = [col for col in REQUIRED_COLS if col not in df.columns]
                raise ValueError(f"Faltan columnas requeridas: {', '.join(missing)}. Debe tener: {', '.join(REQUIRED_COLS)}.")

            imported_count = 0
            ingreso_count = 0
            egreso_count = 0
            
            db.session.begin_nested() 

            for index, row in df.iterrows():
                try:
                    fecha_str = str(row['FECHA']).strip()
                    concept = str(row['CONCEPTO']).strip()
                    
                    try:
                        bank_date = datetime.strptime(fecha_str, '%d-%m-%Y').date()
                    except ValueError:
                        bank_date = datetime.strptime(fecha_str, '%d/%m/%Y').date()
                    except ValueError:
                        bank_date = datetime.strptime(fecha_str, '%Y-%m-%d').date()

                    debit_amount = parse_monto_csv(row['EGRESO'])
                    credit_amount = parse_monto_csv(row['INGRESO'])
                    total_balance = parse_monto_csv(row['TOTAL'])
                    
                    logger.debug(f"Fila {index+2}: Debit={debit_amount}, Credit={credit_amount}, Total={total_balance}")

                    # Solo guardar si hay un movimiento monetario o un balance total
                    if debit_amount != Decimal('0.00') or credit_amount != Decimal('0.00') or total_balance != Decimal('0.00'):
                        new_transaction = BankTransaction(
                            date=bank_date, 
                            concept=concept, 
                            credit=credit_amount, 
                            debit=debit_amount, 
                            total_balance=total_balance,
                            is_conciliated=False,
                            status='PENDIENTE' # Status por defecto
                        )
                        db.session.add(new_transaction)
                        imported_count += 1
                        if credit_amount > Decimal('0.00'):
                            ingreso_count += 1
                        elif debit_amount > Decimal('0.00'):
                            egreso_count += 1
                            
                except Exception as e:
                    logger.error(f"Error de procesamiento en fila {index+2}: {e}")
                    db.session.rollback()
                    db.session.begin_nested() 
                    continue
            
            db.session.commit()

            if not errores:
                flash(f"✅ Importación exitosa. Registrados: {ingreso_count} ingresos y {egreso_count} egresos (Total: {imported_count}).", 'success')
            else:
                 # Esta parte solo se ejecuta si el bloque try/except dentro del bucle falló
                 flash(f"⚠️ Advertencia: Se cargaron {imported_count} registros. Hubo errores internos, revise el log.", 'warning')

            return redirect(url_for('conciliacion_list'))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error al procesar el archivo CSV: {e}', 'danger')
            return redirect(request.url)
            
    return render_template('conciliacion_importar.html')


@app.route('/api/transacciones_pendientes_dt')
@login_required
def api_transacciones_pendientes_dt():
    """API para DataTables de transacciones bancarias (Pendientes y Conciliadas)."""
    
    # Función de formato manual simple: Decimal a string con 2 decimales y coma de miles
    def format_decimal_to_str(d):
        if d is None:
            return "0.00"
        try:
            # Uso de ABS para asegurar que el débito se muestre positivo en la columna EGRESO
            return f"{abs(Decimal(d)):,.2f}"
        except Exception:
             return "0.00"

    year = request.args.get('year', None, type=int)
    month = request.args.get('month', None, type=int)

    # 1. Consulta SQL
    try:
        query = db.session.query(
            BankTransaction.id,
            BankTransaction.date,
            BankTransaction.concept,
            BankTransaction.credit,
            BankTransaction.debit,
            BankTransaction.total_balance,
            BankTransaction.is_conciliated,
            BankTransaction.status,
            BankTransaction.negocio_conciliado,
            BankTransaction.num_factura_conciliado,
            Pago.id.label('pago_id'), 
            Pago.numero_factura.label('numero_factura'),  # <--- CORRECCIÓN CRÍTICA: Añadir la factura del Pago
            Cliente.negocio.label('cliente_negocio'),
            Cliente.rfc.label('rfc_nit') # RFC/NIT del cliente
        ).outerjoin(
            Pago, BankTransaction.id == Pago.bank_transaction_id
        ).outerjoin(
            Cliente, Pago.cliente_id == Cliente.id
        )
        
        if year:
            query = query.filter(db.extract('year', BankTransaction.date) == year)
        if month:
            query = query.filter(db.extract('month', BankTransaction.date) == month)
            
        transactions_data = query.order_by(db.desc(BankTransaction.date)).all()
        
    except Exception as e:
        logger.error(f"Error en la consulta de DataTables: {traceback.format_exc()}")
        return jsonify({'data': [], 'error': f'Error en consulta de DB: {e}'}), 500


    # 2. Procesamiento y Formato
    data = []
    
    for row in transactions_data:
        # Aseguramos que los valores sean Decimal para la comparación
        debit = row.debit if row.debit is not None else Decimal('0.00')
        credit = row.credit if row.credit is not None else Decimal('0.00')
        total_balance = row.total_balance if row.total_balance is not None else Decimal('0.00')
        
        is_ingreso = credit > Decimal('0.00')
        
        # Lógica de display para Negocio y Factura
        
        # 1. Intenta usar los datos directamente de la Transacción Bancaria (para Sucursal/Exportación)
        negocio_final = row.negocio_conciliado or 'N/A'
        
        # 🛑 INICIO DEL AJUSTE PARA QUITAR EL EMAIL 🛑
        if negocio_final != 'N/A':
            # La limpieza solo aplica si la información viene de 'negocio_conciliado'
            
            # Patrón para eliminar (Email) o [Email] o cualquier cosa entre paréntesis o corchetes
            # Esto cubre el caso de Select2 que guarda 'Nombre (email)'
            negocio_final = re.sub(r'\s*\(.*\)', '', negocio_final)
            negocio_final = re.sub(r'\s*\[.*\]', '', negocio_final)
            
            # Patrón para eliminar cualquier cosa que parezca un correo después de un espacio o coma
            # Esto cubre casos como 'Nombre, email@dominio.com'
            if ',' in negocio_final:
                 negocio_final = negocio_final.split(',')[0].strip()
                
            negocio_final = negocio_final.strip() # Limpiamos cualquier espacio extra
        # 🛑 FIN DEL AJUSTE PARA QUITAR EL EMAIL 🛑


        factura_final = row.num_factura_conciliado or 'N/A'
        
        # 2. Si hay un Pago (Conciliación Clásica), sobreescribe con el nombre limpio del cliente
        if row.pago_id and row.cliente_negocio:
             negocio_final = row.cliente_negocio # Este ya debe ser solo el nombre
             # ¡ESTA LÍNEA AHORA FUNCIONARÁ PORQUE 'numero_factura' FUE INCLUIDO EN EL SELECT!
             factura_final = row.numero_factura or row.num_factura_conciliado or 'N/A' # Prefiere la factura del Pago si existe
        
        # 3. RFC/NIT se toma solo si hay un Pago (conciliación clásica)
        rfc_final = row.rfc_nit or 'N/A' 


        data.append({
            'id': row.id,
            'fecha_banco': row.date.strftime('%d-%m-%Y'),
            'concepto': row.concept,
            
            # Datos de conciliación
            'status': row.status, # Enviamos el status real de la Transacción Bancaria
            'is_conciliated': row.is_conciliated,
            'pago_id': row.pago_id, 
            
            # Valores formateados (String)
            'egreso_str': format_decimal_to_str(debit),
            'ingreso_str': format_decimal_to_str(credit), 
            'total_str': format_decimal_to_str(total_balance),
            
            # Valores numéricos (Float para sort/export)
            'egreso_num': float(debit),
            'ingreso_num': float(credit),
            'total_num': float(total_balance),
            
            # Datos conciliados (o pre-conciliados)
            'negocio_conciliado': negocio_final, 
            'num_factura_conciliado': factura_final,
            'rfc_nit_conciliado': rfc_final, 
            'is_ingreso': is_ingreso 
        })
        
    return jsonify({'data': data})

# ----------------------------------------------------
# 🟢 NUEVA RUTA: ELIMINAR TRANSACCIÓN BANCARIA
# ----------------------------------------------------
@app.route('/api/transaccion/eliminar/<int:transaccion_id>', methods=['DELETE'])
@login_required
@role_required(ROLES_MODIFICACION) # ADMIN o SUPERADMIN
def transaccion_eliminar(transaccion_id):
    """Elimina una BankTransaction por ID. También elimina el Pago asociado si existe."""
    try:
        # Primero, buscar y eliminar el Pago asociado si existe
        pago = Pago.query.filter_by(bank_transaction_id=transaccion_id).first()
        if pago:
            # Si hay un pago asociado, marcamos el cliente para recalculo de vigencia
            cliente_id = pago.cliente_id
            
            db.session.delete(pago)
            db.session.commit()
            
            # Si se eliminó el pago, re-calculamos la vigencia del cliente
            recalcular_vigencia_cliente(cliente_id)

        # Luego, eliminar la BankTransaction
        transaccion = BankTransaction.query.get_or_404(transaccion_id)
        db.session.delete(transaccion)
        db.session.commit()
        
        return jsonify({'success': True, 'message': 'Transacción y pago asociado eliminados con éxito.'}), 200

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error al eliminar transacción {transaccion_id}: {traceback.format_exc()}")
        return jsonify({'success': False, 'message': 'Error interno al eliminar la transacción.'}), 500

# ----------------------------------------------------
# 🟢 RUTA CRÍTICA: REGISTRAR / ACTUALIZAR PAGO (Conciliación)
# ----------------------------------------------------
@app.route('/pagos/registrar', methods=['POST'])
@login_required
def pago_registrar():
    """Registra un nuevo Pago asociado a una BankTransaction o actualiza uno existente (Re-conciliación)."""
    from flask import current_app
    from decimal import Decimal
    from datetime import datetime # Aseguramos que datetime esté disponible
    
    try:
        data = request.form
        
        # Datos requeridos para la conciliación
        bank_transaction_id = data.get('bank_transaction_id', type=int)
        cliente_id = data.get('cliente_id', type=int)
        paquete_precio_id = data.get('paquete_id', type=int)
        
        # Datos del formulario
        fecha_pago_str = data.get('fecha_pago')
        monto_pago = Decimal(data.get('monto_pago', 0.0))
        num_factura = data.get('numero_factura')
        
        # 1. Validación inicial
        if not all([bank_transaction_id, cliente_id, paquete_precio_id, fecha_pago_str]):
            return jsonify(ok=False, message='Faltan datos críticos.'), 400

        # 2. Obtener objetos de la DB
        transaccion = db.session.get(BankTransaction, bank_transaction_id)
        cliente = db.session.get(Cliente, cliente_id)
        paquete_precio = db.session.get(PaquetePrecio, paquete_precio_id)
        
        if not transaccion or not cliente or not paquete_precio:
            return jsonify(ok=False, message='Transacción, Cliente o Paquete no válidos.'), 400

        # 🛑 FIX CRÍTICO: Conversión de fecha flexible. 
        # Intentamos primero el formato ISO (YYYY-MM-DD) que envían los inputs web.
        try:
            fecha_pago = datetime.strptime(fecha_pago_str, '%Y-%m-%d').date()
        except ValueError:
            # Fallback al formato DD-MM-YYYY (por si se envía desde otro lugar o se editó)
            try:
                fecha_pago = datetime.strptime(fecha_pago_str, '%d-%m-%Y').date()
            except ValueError:
                current_app.logger.error(f"Error crítico: Formato de fecha de pago '{fecha_pago_str}' es incorrecto.")
                return jsonify(ok=False, message=f'Formato de fecha de pago incorrecto: {fecha_pago_str} (Esperado YYYY-MM-DD o DD-MM-YYYY).'), 400
        
        # 3. Determinar si es un UPDATE (Re-conciliación) o INSERT (Nueva conciliación)
        pago_existente = Pago.query.filter_by(bank_transaction_id=bank_transaction_id).first()
        
        is_update = pago_existente is not None
        pago = pago_existente if is_update else Pago()

        # 4. Asignar/Actualizar campos del Pago
        pago.nombre = cliente.nombre_contacto
        pago.correo = cliente.mail
        pago.cliente_id = cliente_id
        pago.monto = monto_pago 
        pago.numero_factura = num_factura
        pago.fecha_pago = fecha_pago

        # Datos del paquete
        pago.paquete = paquete_precio.paquete
        pago.vigencia = paquete_precio.vigencia
        pago.moneda = paquete_precio.moneda
        pago.paquete_precio_id = paquete_precio_id
        
        # Vínculos
        pago.bank_transaction_id = bank_transaction_id
        pago.status = 'ACTIVO' # Siempre activo para una conciliación/actualización

        if not is_update:
            db.session.add(pago)

        # 5. Marcar la Transacción como conciliada y actualizar status
        transaccion.is_conciliated = True
        transaccion.status = 'CONCILIADO'
        transaccion.negocio_conciliado = cliente.negocio # Guardar el negocio en la transacción
        transaccion.num_factura_conciliado = num_factura # Guardar factura en la transacción

        # 6. Recalcular Vigencia 
        recalcular_vigencia_cliente(cliente_id)

        db.session.commit()
        
        action = "actualizado" if is_update else "registrado"
        return jsonify(ok=True, message=f'Conciliación {action} con éxito. Vigencia de cliente recalculada.'), 200

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error en pago_registrar/actualizar: {traceback.format_exc()}")
        return jsonify(ok=False, message=f'Error interno al procesar el pago: {e}'), 500


# ----------------------------------------------------
# 🟢 API: PAQUETES (Para Selects Simples)
# ----------------------------------------------------
@app.route('/api/paquetes_list')
@login_required
def api_paquetes_list():
    """Devuelve una lista simple de paquetes para ser usada en Select2 o modales.
    Utiliza el modelo PaquetePrecio para asegurar que solo se muestren los activos.
    """
    # Usamos PaquetePrecio para obtener una lista única de paquetes y vigencias
    # Excluimos demos para la venta.
    paquetes_raw = db.session.query(
        PaquetePrecio.id,
        PaquetePrecio.paquete,
        PaquetePrecio.vigencia,
        PaquetePrecio.moneda
    ).filter(
        PaquetePrecio.vigencia.notilike('%DEMO%')
    ).order_by(PaquetePrecio.paquete).all()

    data = []
    for p in paquetes_raw:
        data.append({
            'id': p.id,
            'nombre': p.paquete,
            'vigencia': p.vigencia,
            'moneda': p.moneda
        })
        
    return jsonify(data)


@app.route('/api/clientes/search')
@login_required
def api_clientes_search():
    """API para el Select2 en el modal de conciliación."""
    query = request.args.get('q', '', type=str)
    
    if len(query) < 3:
        return jsonify(results=[]) 
        
    # Buscar clientes por negocio, contacto o email
    clientes = Cliente.query.filter(
        or_(
            Cliente.negocio.ilike(f'%{query}%'), 
            Cliente.nombre_contacto.ilike(f'%{query}%'), 
            Cliente.mail.ilike(f'%{query}%')
        )
    ).limit(20).all()
    
    results = [{
        'id': c.id, 
        'text': f"{c.negocio} ({c.nombre_contacto}) - {c.mail}"
    } for c in clientes]
    
    return jsonify(results=results)



@app.route('/api/clientes/search_menu')
@login_required
def api_clientes_search_menu():
    """API para el Buscador Rápido en el menú principal (Devuelve ID y texto)."""
    query = request.args.get('q', '', type=str)
    
    if len(query) < 3:
        return jsonify([]) # Devuelve un array vacío si la consulta es demasiado corta
        
    # Buscar clientes por negocio, contacto, mail, o ID Gumi
    clientes = Cliente.query.filter(
        or_(
            Cliente.negocio.ilike(f'%{query}%'), 
            Cliente.nombre_contacto.ilike(f'%{query}%'), 
            Cliente.mail.ilike(f'%{query}%')
        )
    ).limit(10).all() # Limitar a 10 resultados para no sobrecargar el menú
    
    results = [{
        'id': c.id, 
        'negocio': c.negocio,
        'nombre_contacto': c.nombre_contacto
    } for c in clientes]
    
    return jsonify(results)


@app.route('/api/paquetes_by_country')
@login_required
def api_paquetes_by_country():
    """Devuelve la lista de PaquetePrecio activos filtrados por país y moneda.
    Se usa para llenar el Select2 en el modal de conciliación.
    """
    country = request.args.get('country', None, type=str)
    
    # 1. Construir la consulta base
    # Filtramos por el campo is_active que tienes en PaquetePrecio.
    # Si no tienes datos en is_active, todos los registros aparecerán.
    query = PaquetePrecio.query.filter(
        PaquetePrecio.is_active == True # Filtra solo los paquetes activos
    )

    # 2. Aplicar filtro por país (usando la columna 'pais' o 'moneda' de PaquetePrecio)
    if country:
        country_upper = country.upper().strip()
        
        # Filtramos por país.
        # CRÍTICO: Asumimos que si el país es MÉXICO, queremos MONEDA MXN
        if country_upper == 'MÉXICO':
            query = query.filter(PaquetePrecio.moneda == 'MXN')
        else:
            # Para cualquier otro país, filtramos por la columna 'pais'
            query = query.filter(PaquetePrecio.pais == country_upper)

    
    # 3. Ejecutar consulta y formatear resultados
    paquetes_precios = query.order_by(PaquetePrecio.paquete.asc()).all()
    
    results = []
    
    for pp in paquetes_precios:
        # Usamos el campo 'paquete' (nombre) y 'vigencia' de PaquetePrecio
        precio_str = f"{pp.moneda} {pp.precio:,.2f}"
        
        results.append({
            'id': pp.id, # ID del PaquetePrecio (CRÍTICO)
            'text': f"{pp.paquete} ({pp.vigencia}) - {precio_str}",
            'precio': float(pp.precio),
            'moneda': pp.moneda,
        })
        
    return jsonify({
        'ok': True,
        'paquetes': results
    })


@app.route('/api/conciliar_transaccion/<int:transaccion_id>', methods=['POST'])
@login_required
@role_required(ROLES_MODIFICACION)
def api_conciliar_transaccion(transaccion_id):
    """
    Realiza la conciliación: crea un Pago y marca la BankTransaction como conciliada.
    DEPRECATED: Esta ruta ya no se usa, reemplazada por pago_registrar.
    """
    return jsonify(ok=False, msg="Ruta obsoleta, use /pagos/registrar"), 410


# 🛑 RUTA DE PRE-CONCILIACIÓN SUCURSAL (CORREGIDA)
@app.route('/api/transaccion/preconciliar_sucursal/<int:transaccion_id>', methods=['POST'])
@login_required
@role_required(ROLES_MODIFICACION)
def api_preconciliar_sucursal(transaccion_id):
    try:
        # 1. Obtener la transacción
        transaccion = BankTransaction.query.get_or_404(transaccion_id)
        data = request.get_json()

        # 2. Extraer datos
        negocios_nombres = data.get('negocios_nombres', '')
        numero_factura = data.get('numero_factura')
        
        # 3. Actualizar el status de la BankTransaction
        transaccion.status = 'PRE-CONCILIADO (Sucursal)' 
        transaccion.negocio_conciliado = negocios_nombres # Guardamos la cadena de nombres de negocios
        transaccion.num_factura_conciliado = numero_factura
        transaccion.is_conciliated = False # No está conciliada a un pago Gumi todavía

        # 4. Eliminar Pago asociado (si existiera)
        # Esto es importante si alguien intenta "Pre-Conciliar" algo que ya estaba conciliado.
        pago_existente = Pago.query.filter_by(bank_transaction_id=transaccion_id).first()
        if pago_existente:
             db.session.delete(pago_existente)
             # No es necesario recalcular aquí, ya que el Pago se registrará manualmente después.

        db.session.commit()
        
        return jsonify({
            "ok": True, 
            "message": "Transacción marcada como PRE-CONCILIADA (Sucursal). Proceda a registrar pagos individuales."
        }), 200
        
    except Exception as e:
        db.session.rollback()
        error_message = str(e)
        current_app.logger.error(f"Error al pre-conciliar la sucursal: {traceback.format_exc()}")
        return jsonify({
            "ok": False, 
            "message": f"Error al pre-conciliar la sucursal: {error_message}"
        }), 500
    

# ========== Main ==========
if __name__ == '__main__':
    with app.app_context():
        # 1. Crear todas las tablas (Esto creará las nuevas tablas de Conciliación)
        db.create_all()
        
        # 2. Crear usuario de prueba con password HASH (CRÍTICO: Usar password_hash)
        if not User.query.filter_by(username='admin').first():
            user = User(
                username='admin',
                # CRÍTICO: Usar el campo correcto y hash
                password_hash=generate_password_hash('09876', method='pbkdf2:sha256'), 
                full_name='Super Administrador Inicial', 
                email='admin@gumi.com', 
                role='SUPERADMIN' 
            )
            db.session.add(user)
            db.session.commit()
            print(">>> USUARIO SUPERADMIN INICIAL CREADO: admin / 09876")
            
    # La aplicación se ejecuta FUERA del app_context
    app.run(debug=True, host='127.0.0.1', port=5001)

    # Version final de despliegue, reintento [2025-12-12]