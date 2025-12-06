import csv
from datetime import datetime, timedelta
from pymongo.errors import DuplicateKeyError
import json
import pydgraph
import random

from connect import (
    db,
    get_cassandra_session,
    create_client_stub,
    create_client,
    close_client_stub,
)

CSV_PATH = "data/data.csv"

# ---------- Generador de CSV ----------

def generar_csv_simple(archivo: str = CSV_PATH, filas: int = 50):

    encabezados = [
        "user_id", "expediente", "email", "password", "role",
        "ticket_id", "title", "description", "category", "status", "priority",
        "installation_id", "place_name", "object_name", "lost_status", "turno"
    ]
    titulos = [
        "Falla en equipo de computo",
        "Falla en proyector del salon",
        "Falla en red del edificio",
        "Falla en sistema de impresion",
        "Falla en bocinas del aula",
        "Daño en mobiliario",
        "Daño en pantalla del laboratorio",
        "Daño en cableado electrico",
        "Daño en equipo multimedia",
        "Daño en puerta del aula",
        "Reporte de incidencia",
        "Sistema no responde",
        "Solicitud de mantenimiento",
        "Incidente recurrente reportado",
        "Aviso de comportamiento anormal",
        "Error al iniciar sesion",
        "Equipo lento durante el uso",
        "Solicitud de verificación de red",
        "Advertencia de seguridad",
        "Actualización requerida en sistema",
        "Problema detectado durante la clase",
        "Revisión solicitada del equipo",
        "Inconveniente en área de estudio",
        "Petición de soporte técnico",
        "Reporte de comportamiento extraño",
        "Problema en laboratorio de computo",
        "Incidente en edificio de ingenierias",
        "Reporte de falla en iluminación",
        "Problema en red del edificio",
        "Mal funcionamiento en sala de lectura",
        "Incidente durante clase",
        "Profesor solicita revisión del equipo",
        "Equipo no responde durante sesión",
        "Interrupción en presentación del docente",
        "Objeto perdido en biblioteca",
        "Llaves extraviadas en cafetería",
        "Se encontró objeto sin dueño",
        "Reporte de mochila perdida",
        "Extravio de cartera en instalaciones",
        "Reporte general de mantenimiento",
        "Reporte de situación anómala",
        "Reporte de equipo defectuoso",
        "Reporte preliminar de incidente",
    ]

    roles = ["docente", "estudiante"]
    categorias = ["instalaciones", "docentes", "cosas_perdidas"]
    estados = ["abierto", "en_proceso", "cerrado"]
    prioridades = ["alta", "media", "baja"]
    installation_ids = [
        "biblioteca",
        "gimnasio",
        "domo",
        "lab_quimica",
        "lab_computo",
        "banos",
        "estacionamiento",
        "ingenierias",
        "humanidades",
        "negocios",
        "apoyo_estudiantil",
        "cafeteria",
        "taller_ingenieria",
        "cancha",
        "auditorio",
        "estudios",
        "administracion",
        "salon_multi",
    ]
    place_names = [
        "Biblioteca central",
        "Gimnasio principal",
        "Domo deportivo",
        "Laboratorio de quimica",
        "Laboratorio de computo",
        "Banos edificio A",
        "Estacionamiento principal",
        "Edificio ingenierias",
        "Edificio humanidades",
        "Edificio negocios",
        "Centro de apoyo estudiantil",
        "Cafeteria central",
        "Sala de lectura",
        "Taller de ingenieria",
        "Cancha techada",
        "Auditorio principal",
        "Centro de medios",
        "Edificio administrativo",
        "Salon multidisciplinario",
    ]
    turnos = ["manana", "tarde_noche"]
    descripciones = [
        "El problema se presenta desde la semana pasada.",
        "La falla ocurre solo en ciertos horarios.",
        "Se requiere revision por parte de soporte.",
        "El incidente afecta a varias personas del area.",
        "Situacion reportada previamente sin solucion.",
        "El equipo deja de responder aleatoriamente.",
        "Se detecto comportamiento inusual en el sistema.",
        "Se necesita revision fisica del equipo.",
        "El servicio funciona de forma intermitente.",
        "Error aparece despues de unos minutos de uso.",
        "El usuario no puede completar sus actividades.",
        "Se solicita atencion prioritaria al incidente.",
        "El problema ocurre en mas de un dispositivo.",
        "Sistema muestra mensajes de error al iniciar.",
        "El equipo tarda demasiado en responder.",
        "Configuracion no se guardo correctamente.",
        "Se sospecha un problema de conexion.",
        "El fallo se detecto durante la clase.",
        "Impide uso normal del espacio.",
        "Validar posible riesgo de seguridad.",
    ]

    with open(archivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(encabezados)

        ticket_counter = 0

        for i in range(1, filas + 1):
            user_id = f"U{str(i).zfill(3)}"
            expediente = random.randint(750000, 780000)
            email = f"user{i}@iteso.mx"

            password = "".join(str(random.randint(0, 9)) for _ in range(8))

            role = random.choice(roles)

            # 3 tickets por usuario
            for _ in range(3):
                ticket_counter += 1
                ticket_id = f"TK-{3000 + ticket_counter}"

                category = random.choice(categorias)
                status = random.choice(estados)
                priority = random.choice(prioridades)

                installation_id = random.choice(installation_ids)
                place_name = random.choice(place_names)

                title = random.choice(titulos)
                description = random.choice(descripciones)

                if category == "cosas_perdidas":
                    object_name = random.choice(["Cartera", "Llaves", "USB", "Mochila", "Guantes"])
                    lost_status = random.choice(["activo", "encontrado"])
                else:
                    object_name = ""
                    lost_status = ""

                turno = random.choice(turnos)

                writer.writerow([
                    user_id, expediente, email, password, role,
                    ticket_id, title, description, category, status, priority,
                    installation_id, place_name, object_name, lost_status, turno
                ])

    print(f"CSV generado: {archivo}")

# ---------- Mongo ----------

def ensure_mongo_indexes():
    db.users.create_index("email", unique=True)
    # tickets: installation_id + created_at
    db.tickets.create_index("installation_id")
    db.tickets.create_index([("created_at", -1)])


def insert_user(row):
    users = db.users

    user_doc = {
        "user_id": row["user_id"],
        "expediente": row["expediente"],
        "email": row["email"],
        "password": row["password"],
        "role": row["role"],
        "createdAt": datetime.utcnow(),
    }

    try:
        result = users.update_one(
            {"email": user_doc["email"]},
            {"$setOnInsert": user_doc},
            upsert=True,
        )

        if result.upserted_id is not None:
            print(f"Usuario creado: {user_doc['email']}")
        else:
            print(f"Usuario ya existia: {user_doc['email']}")

    except DuplicateKeyError:
        print(f"- Email duplicado, ignorado: {user_doc['email']}")


def insert_ticket(row):
    tickets = db.tickets

    ticket_doc = {
        "ticket_id": row["ticket_id"],
        "title": row["title"],
        "description": row["description"],
        "category": row["category"],
        "status": row["status"],
        "priority": row["priority"],
        "user_id": row["user_id"],
        "installation_id": row["installation_id"],
        "place_name": row.get("place_name"),
        "object_name": row.get("object_name"), 
        "lost_status": row.get("lost_status"),
        "turno": row.get("turno"),
        "created_at": datetime.utcnow(),
    }

    tickets.insert_one(ticket_doc)
    print(f"Ticket creado: {ticket_doc['ticket_id']} ({ticket_doc['title']})")


def populate_mongo(csv_file: str = CSV_PATH):
    print("=== Populate Mongo ===")
    ensure_mongo_indexes()

    with open(csv_file, newline="", encoding="utf-8") as fd:
        reader = csv.DictReader(fd)

        for row in reader:
            print(f"\nProcesando ticket {row['ticket_id']}...")
            insert_user(row)
            insert_ticket(row)


# ---------- Cassandra ----------


def _get_turno(created_at: datetime) -> str:
    hour = created_at.hour
    if 7 <= hour < 15:
        return "manana"
    return "tarde_noche"


def populate_cassandra():
    """
    Llena las tablas de Cassandra usando los tickets que ya estan en Mongo.
    Usa db.tickets y db.users como fuente de verdad.
    """
    from Cassandra import model as cass_model

    print("=== Populate Cassandra ===")
    session = get_cassandra_session()

    # Asegurar esquema
    cass_model.create_schema(session)

    # Limpiar datos previos para evitar duplicados
    tablas = [
        "alertas_tickets_vencidos",
        "historial_por_usuario",
        "conteo_tickets_por_categoria_dia",
        "tickets_por_profesor",
        "historial_ticket",
        "tickets_por_instalacion_fechas",
        "tickets_por_estado",
        "filtrado_tickets_por_fecha",
        "tickets_por_usuario_dia",
        "tickets_por_rol",
        "conteo_tickets_por_prioridad",
        "tickets_por_instalaciones",
        "tickets_por_turno",
    ]
    for nombre in tablas:
        session.execute(f"TRUNCATE {nombre}")

    insert_alerta = session.prepare(
        """
        INSERT INTO alertas_tickets_vencidos
        (dias_inactivos, ticket_id, fecha_ultimo_cambio, estado_actual)
        VALUES (?, ?, ?, ?)
        """
    )
    insert_hist_usuario = session.prepare(
        """
        INSERT INTO historial_por_usuario
        (user_id, fecha, ticket_id, categoria, estado)
        VALUES (?, ?, ?, ?, ?)
        """
    )
    insert_profesor = session.prepare(
        """
        INSERT INTO tickets_por_profesor
        (profesor_id, fecha_creacion, ticket_id, categoria, estado, descripcion)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    )
    insert_hist_ticket = session.prepare(
        """
        INSERT INTO historial_ticket
        (ticket_id, fecha, evento, usuario, estado_anterior, estado_nuevo)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    )
    insert_instalacion = session.prepare(
        """
        INSERT INTO tickets_por_instalacion_fechas
        (install_id, fecha, ticket_id, categoria, estado, prioridad, descripcion)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
    )
    insert_estado = session.prepare(
        """
        INSERT INTO tickets_por_estado
        (estado, fecha, ticket_id, categoria, user_id)
        VALUES (?, ?, ?, ?, ?)
        """
    )
    insert_tickets = session.prepare(
        """
        INSERT INTO filtrado_tickets_por_fecha
        (fecha, ticket_id, user_id, categoria, estado, prioridad)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    )
    insert_usuario_dia = session.prepare(
        """
        INSERT INTO tickets_por_usuario_dia
        (user_id, fecha, hora, ticket_id, categoria, estado, descripcion)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
    )
    insert_rol = session.prepare(
        """
        INSERT INTO tickets_por_rol
        (rol, fecha_creacion, ticket_id, user_id, categoria, estado)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    )
    insert_instalaciones = session.prepare(
        """
        INSERT INTO tickets_por_instalaciones
        (instalacion, fecha, ticket_id, estado, categoria)
        VALUES (?, ?, ?, ?, ?)
        """
    )

    update_cat_dia = session.prepare(
        """
        UPDATE conteo_tickets_por_categoria_dia
        SET total = total + 1
        WHERE fecha = ? AND categoria = ?
        """
    )
    update_prioridad = session.prepare(
        """
        UPDATE conteo_tickets_por_prioridad
        SET total = total + 1
        WHERE prioridad = ?
        """
    )
    update_turno = session.prepare(
        """
        UPDATE tickets_por_turno
        SET total_tickets = total_tickets + 1
        WHERE turno = ?
        """
    )

    # Cache de usuarios por user_id
    usuarios = {u["user_id"]: u for u in db.users.find()}

    now = datetime.utcnow()
    tickets_cursor = db.tickets.find()

    for ticket in tickets_cursor:
        ticket_id = ticket["ticket_id"]
        user_id = ticket["user_id"]
        categoria = ticket["category"]
        estado = ticket["status"]
        prioridad = ticket["priority"]
        install_id = ticket["installation_id"]

        #Fecha real de creacion en Mongo 
        _ = ticket.get("created_at") or now

        # Repartimos tickets en septiembre, octubre y noviembre de 2025
        try:
            num = int(ticket_id.split("-")[1])  
        except (IndexError, ValueError):
            num = 0

        mod = num % 3
        if mod == 0:
            month = 9   # septiembre
        elif mod == 1:
            month = 10  # octubre
        else:
            month = 11  # noviembre

        # Dia entre 1 y 28 para evitar problemas de fin de mes
        day = (num % 28) + 1
        #Hora simulada 
        hour = 8 + (num % 10)  

        created_at = datetime(2025, month, day, hour, 0, 0)
        fecha_dia = created_at.date()

        user = usuarios.get(user_id, {})
        rol = user.get("role")
        email = user.get("email", user_id)
        descripcion = ticket.get("description", "")

        #simulamos dias inactivos segun prioridad/estado
        base_por_prioridad = {"alta": 10, "media": 5, "baja": 2}
        dias_inactivos = base_por_prioridad.get(prioridad, 3)
        if estado == "cerrado":
            dias_inactivos += 2
        elif estado == "en_proceso":
            dias_inactivos = max(dias_inactivos - 1, 0)

        fecha_ultimo_cambio = now - timedelta(days=dias_inactivos)
        session.execute(
            insert_alerta,
            (dias_inactivos, ticket_id, fecha_ultimo_cambio, estado),
        )

        # 2) historial_por_usuario: un evento de creacion por ticket
        session.execute(
            insert_hist_usuario,
            (user_id, created_at, ticket_id, categoria, estado),
        )

        # 3) tickets_por_categoria_dia 
        session.execute(update_cat_dia, (fecha_dia, categoria))

        # 4) tickets_por_profesor: solo si es docente
        if rol == "docente":
            session.execute(
                insert_profesor,
                (user_id, created_at, ticket_id, categoria, estado, descripcion),
            )

        # 5) historial_ticket: solo un evento "creacion"
        session.execute(
            insert_hist_ticket,
            (ticket_id, created_at, "creacion", email, "N/A", estado),
        )

        # 6) tickets_por_instalacion_fechas
        session.execute(
            insert_instalacion,
            (install_id, created_at, ticket_id, categoria, estado, prioridad, descripcion),
        )

        # 7) tickets_por_estado
        session.execute(
            insert_estado,
            (estado, created_at, ticket_id, categoria, user_id),
        )

        # 8) tickets (timeline global)
        session.execute(
            insert_tickets,
            (created_at, ticket_id, user_id, categoria, estado, prioridad),
        )

        # 9) tickets_por_usuario_dia
        session.execute(
            insert_usuario_dia,
            (user_id, fecha_dia, created_at, ticket_id, categoria, estado, descripcion),
        )

        # 10) tickets_por_rol
        if rol is not None:
            session.execute(
                insert_rol,
                (rol, created_at, ticket_id, user_id, categoria, estado),
            )

        # 11) tickets_por_prioridad 
        session.execute(update_prioridad, (prioridad,))

        # 12) tickets_por_instalaciones 
        instalacion = install_id
        session.execute(
            insert_instalaciones,
            (instalacion, created_at, ticket_id, estado, categoria),
        )

        # 13) tickets_por_turno 
        turno = ticket.get("turno") or _get_turno(created_at)
        session.execute(update_turno, (turno,))

    print("Populate Cassandra: inserciones completadas.")


# ---------- Dgraph ----------


def populate_dgraph():
    """
    Llena Dgraph usando los tickets que ya estan en Mongo
    Crea nodos/relaciones segun el esquema RDF del doc.
    """
   
    print("=== Populate Dgraph ===")

    # Crear cliente de Dgraph
    stub = create_client_stub()
    client = create_client(stub)

  # Esquema RDF
    schema = """
    user_id: string @index(exact) .
    nombre: string @index(term) .
    email: string @index(exact) .
    rol: string @index(exact) .
    expediente: string @index(exact) .
    creo: [uid] @reverse .

    ticket_id: string @index(exact) .
    titulo: string @index(fulltext) .
    descripcion: string @index(fulltext) .
    estado: string  @index(exact) .
    prioridad: string @index(exact) .
    fecha_creacion: datetime @index(hour) .
    categoria: string @index(exact) .
    afecta: uid @reverse .
    tipo: uid .
    pertenece_a_categoria: uid .
    asignado_a: uid @reverse .
    escalado_a: uid @reverse .
    ocurre_en: uid @reverse .
    reporta_en: uid @reverse .
    contiene: [uid] .

    instal_id: string @index(exact) .
    tipo_instalacion: string @index(exact) .
    ubicacion: string .

    dept_id: string @index(exact) .

    tipo_id: string @index(exact) .

    categoria_id: string @index(exact) .

    palabra: string @index(exact, term) .

    agente_id: string @index(exact) .

    periodo_id: string @index(exact) .
    fecha_inicio: datetime .
    fecha_fin: datetime .

    horario_id: string @index(exact) .
    hora_inicio: string .
    hora_fin: string .
    periodo: string @index(exact) .

    type Usuario {
      user_id
      nombre
      email
      rol
      expediente
      creo
    }

    type Ticket {
      ticket_id
      titulo
      descripcion
      estado
      prioridad
      fecha_creacion
      afecta
      tipo
      pertenece_a_categoria
      asignado_a
      escalado_a
      ocurre_en
      reporta_en
      contiene
    }

    type Instalacion {
      instal_id
      nombre
      tipo_instalacion
      ubicacion
    }

    type TipoProblema {
      tipo_id
      descripcion
    }

    type Categoria {
      categoria_id
      nombre
      descripcion
    }

    type PalabraClave {
      palabra
    }

    type Agente {
      agente_id
      nombre
      email
    }

    type PeriodoTemporal {
      periodo_id
      descripcion
      fecha_inicio
      fecha_fin
    }

    type Horario {
      horario_id
      hora_inicio
      hora_fin
      periodo
    }
    """

    op = pydgraph.Operation(schema=schema)
    client.alter(op)

    # Datos desde Mongo
    usuarios_db = list(db.users.find())
    tickets_db = list(db.tickets.find())

    objetos = []

    # ---------- Agentes ----------
    agentes_info = [
        {"agente_id": "AG-001", "nombre": "Agente Soporte 1", "email": "agente1@iteso.mx", "dept_id": "DESI"},
        {"agente_id": "AG-002", "nombre": "Agente Soporte 2", "email": "agente2@iteso.mx", "dept_id": "DESI"},
        {"agente_id": "AG-003", "nombre": "Agente Soporte 3", "email": "agente3@iteso.mx", "dept_id": "DEGN"},
    ]

    agente_map = {}
    for a in agentes_info:
        uid = f"_:ag_{a['agente_id']}"
        agente_map[a["agente_id"]] = uid
        objetos.append(
            {
                "uid": uid,
                "dgraph.type": "Agente",
                "agente_id": a["agente_id"],
                "nombre": a["nombre"],
                "email": a["email"],
            }
        )

    # ---------- Periodos temporales ----------
    periodos_info = {
        9: {
            "periodo_id": "P-2025-09",
            "descripcion": "Septiembre 2025",
            "fecha_inicio": datetime(2025, 9, 1),
            "fecha_fin": datetime(2025, 9, 30),
        },
        10: {
            "periodo_id": "P-2025-10",
            "descripcion": "Octubre 2025",
            "fecha_inicio": datetime(2025, 10, 1),
            "fecha_fin": datetime(2025, 10, 31),
        },
        11: {
            "periodo_id": "P-2025-11",
            "descripcion": "Noviembre 2025",
            "fecha_inicio": datetime(2025, 11, 1),
            "fecha_fin": datetime(2025, 11, 30),
        },
    }

    periodo_map = {}
    for month, info in periodos_info.items():
        uid = f"_:per_{info['periodo_id']}"
        periodo_map[month] = uid
        objetos.append(
            {
                "uid": uid,
                "dgraph.type": "PeriodoTemporal",
                "periodo_id": info["periodo_id"],
                "descripcion": info["descripcion"],
                "fecha_inicio": info["fecha_inicio"].isoformat(),
                "fecha_fin": info["fecha_fin"].isoformat(),
            }
        )

    # ---------- Horarios ----------
    horarios_info = {
        "manana": {
            "horario_id": "H-MANANA",
            "hora_inicio": "07:00",
            "hora_fin": "14:59",
            "periodo": "manana",
        },
        "tarde_noche": {
            "horario_id": "H-TARDE-NOCHE",
            "hora_inicio": "15:00",
            "hora_fin": "22:00",
            "periodo": "tarde_noche",
        },
    }

    horario_map = {}
    for key, info in horarios_info.items():
        uid = f"_:hor_{key}"
        horario_map[key] = uid
        objetos.append(
            {
                "uid": uid,
                "dgraph.type": "Horario",
                "horario_id": info["horario_id"],
                "hora_inicio": info["hora_inicio"],
                "hora_fin": info["hora_fin"],
                "periodo": info["periodo"],
            }
        )

    # ---------- Tipos de problema ----------
    tipos_info = [
        {"tipo_id": "TP-01", "descripcion": "Falla electrica"},
        {"tipo_id": "TP-02", "descripcion": "Problema con equipo/docente"},
        {"tipo_id": "TP-03", "descripcion": "Perdida de objeto"},
    ]

    tipo_map = {}
    for tinfo in tipos_info:
        uid = f"_:tp_{tinfo['tipo_id']}"
        tipo_map[tinfo["tipo_id"]] = uid
        objetos.append(
            {
                "uid": uid,
                "dgraph.type": "TipoProblema",
                "tipo_id": tinfo["tipo_id"],
                "descripcion": tinfo["descripcion"],
            }
        )

    # ---------- Categorias ----------
    categorias_nombres = sorted(
        {t.get("category") for t in tickets_db if t.get("category")}
    )
    categoria_map = {}
    cat_index = 1
    for nombre_cat in categorias_nombres:
        cat_id = f"CAT-{cat_index:02d}"
        uid = f"_:cat_{nombre_cat}"
        categoria_map[nombre_cat] = uid
        objetos.append(
            {
                "uid": uid,
                "dgraph.type": "Categoria",
                "categoria_id": cat_id,
                "nombre": nombre_cat,
                "descripcion": f"Tickets de categoria {nombre_cat}",
            }
        )
        cat_index += 1

    # ---------- Instalaciones ----------
    instalaciones_vistas = {}
    for t in tickets_db:
        inst_id = t.get("installation_id")
        nombre = t.get("place_name") or inst_id
        if inst_id and inst_id not in instalaciones_vistas:
            instalaciones_vistas[inst_id] = nombre

    instal_map = {}
    for inst_id, nombre in instalaciones_vistas.items():
        uid = f"_:inst_{inst_id}"
        instal_map[inst_id] = uid
        objetos.append(
            {
                "uid": uid,
                "dgraph.type": "Instalacion",
                "instal_id": inst_id,
                "nombre": nombre,
                "tipo_instalacion": "instalacion",
                "ubicacion": "Campus ITESO",
            }
        )

    # ---------- Usuarios ----------
    usuarios_map = {}
    for u in usuarios_db:
        user_id = u.get("user_id")
        if not user_id:
            continue

        uid = f"_:u_{user_id}"
        expediente = str(u.get("expediente", ""))
        rol = u.get("role", "")
        email = u.get("email", "")

        user_obj = {
            "uid": uid,
            "dgraph.type": "Usuario",
            "user_id": user_id,
            "nombre": f"Usuario {user_id}",
            "email": email,
            "rol": rol,
            "expediente": expediente,
            "creo": [],
        }

        usuarios_map[user_id] = user_obj
        objetos.append(user_obj)

    # ---------- Palabras clave ----------
    palabra_map = {}

    def normalizar_palabra(w: str) -> str:
        return w.lower().strip(".,;:¡!¿?()[]{}\"'")

    stopwords = {
        "el", "la", "los", "las", "un", "una", "unos", "unas",
        "de", "del", "en", "y", "o", "por", "para", "con", "al",
        "se", "lo", "que", "es", "esta", "este", "son",
    }

    # ---------- Tickets ----------
    for t in tickets_db:
        ticket_id = t.get("ticket_id")
        if not ticket_id:
            continue

        try:
            num = int(ticket_id.split("-")[1])
        except (IndexError, ValueError):
            num = random.randint(3000, 4000)

        mod = num % 3
        if mod == 0:
            month = 9
        elif mod == 1:
            month = 10
        else:
            month = 11

        day = (num % 28) + 1
        hour = 8 + (num % 10)
        created_at = datetime(2025, month, day, hour, 0, 0)
        fecha_str = created_at.isoformat()

        ticket_uid = f"_:t_{ticket_id}"

        ticket_obj = {
            "uid": ticket_uid,
            "dgraph.type": "Ticket",
            "ticket_id": ticket_id,
            "titulo": t.get("title", ""),
            "descripcion": t.get("description", ""),
            "estado": t.get("status", ""),
            "prioridad": t.get("priority", ""),
            "fecha_creacion": fecha_str,
        }

        # afecta -> Instalacion
        inst_id = t.get("installation_id")
        if inst_id and inst_id in instal_map:
            ticket_obj["afecta"] = {"uid": instal_map[inst_id]}

        # categoria (string simple)
        categoria_nombre = t.get("category")
        if categoria_nombre:
            ticket_obj["categoria"] = categoria_nombre

        # pertenece_a_categoria
        if categoria_nombre and categoria_nombre in categoria_map:
            ticket_obj["pertenece_a_categoria"] = {"uid": categoria_map[categoria_nombre]}

        # tipo -> TipoProblema
        if categoria_nombre == "instalaciones":
            tipo_id = "TP-01"
        elif categoria_nombre == "docentes":
            tipo_id = "TP-02"
        else:
            tipo_id = "TP-03"
        ticket_obj["tipo"] = {"uid": tipo_map[tipo_id]}

        # asignado_a -> Agente
        agente_id = random.choice(list(agente_map.keys()))
        ticket_obj["asignado_a"] = {"uid": agente_map[agente_id]}

        # escalado_a -> Agente (algunos)
        if random.random() < 0.4:
            agentes_posibles = [aid for aid in agente_map.keys() if aid != agente_id]
            if agentes_posibles:
                agente_id_esc = random.choice(agentes_posibles)
                ticket_obj["escalado_a"] = {"uid": agente_map[agente_id_esc]}

        # ocurre_en -> PeriodoTemporal
        periodo_uid = periodo_map.get(month)
        if periodo_uid:
            ticket_obj["ocurre_en"] = {"uid": periodo_uid}

        # reporta_en -> Horario
        turno = t.get("turno")
        if not turno:
            turno = "manana" if hour < 15 else "tarde_noche"
        if turno not in horario_map:
            turno = "manana"
        ticket_obj["reporta_en"] = {"uid": horario_map[turno]}

        # contiene -> PalabraClave
        palabras_ticket = []
        texto = (t.get("title", "") + " " + t.get("description", "")).split()
        for raw in texto:
            w = normalizar_palabra(raw)
            if not w or len(w) < 4 or w in stopwords:
                continue
            palabras_ticket.append(w)

        palabras_ticket = list(dict.fromkeys(palabras_ticket))[:5]

        contiene_uids = []
        for w in palabras_ticket:
            if w not in palabra_map:
                p_uid = f"_:pal_{w}"
                palabra_map[w] = p_uid
                objetos.append(
                    {
                        "uid": p_uid,
                        "dgraph.type": "PalabraClave",
                        "palabra": w,
                    }
                )
            contiene_uids.append({"uid": palabra_map[w]})

        if contiene_uids:
            ticket_obj["contiene"] = contiene_uids

        # relacion creo: Usuario -> Ticket
        user_id = t.get("user_id")
        if user_id and user_id in usuarios_map:
            usuarios_map[user_id].setdefault("creo", []).append({"uid": ticket_uid})

        objetos.append(ticket_obj)

    # Enviar todo a Dgraph
    if objetos:
        txn = client.txn()
        try:
            data = json.dumps(objetos).encode("utf-8")
            mutation = pydgraph.Mutation()
            mutation.set_json = data
            txn.mutate(mutation)
            txn.commit()
            print(f"Populate Dgraph: {len(objetos)} objetos insertados.")
        finally:
            txn.discard()
    else:
        print("Populate Dgraph: no hay datos para insertar en Dgraph.")

    close_client_stub(stub)
    print("Populate Dgraph: completado.")


# ---------- MAIN GLOBAL ----------

def main():
    print("=== Populate: Mongo + Cassandra + Dgraph ===")

    generar_csv_simple(archivo=CSV_PATH, filas=100)
    populate_mongo()
    populate_cassandra()
    populate_dgraph()

    print("=== Populate completado ===")


if __name__ == "__main__":
    main()

def main():
    print("=== Populate: Mongo + Cassandra + Dgraph ===")

    generar_csv_simple(archivo=CSV_PATH, filas=100)

    populate_mongo()
    populate_cassandra()
    populate_dgraph() 
    print("=== Populate completado ===")


if __name__ == "__main__":
    main()
