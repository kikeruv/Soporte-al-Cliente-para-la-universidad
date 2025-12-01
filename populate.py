import csv
from datetime import datetime, timedelta
from pymongo.errors import DuplicateKeyError
import json
import pydgraph

from connect import (
    db,
    get_cassandra_session,
    create_client_stub,
    create_client,
    close_client_stub,
)

CSV_PATH = "data/data.csv"


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

    # Preparar sentencias normales
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
        (install_id, fecha, ticket_id, categoria, estado, prioridad)
        VALUES (?, ?, ?, ?, ?, ?)
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
        (user_id, fecha, hora, ticket_id, categoria, estado)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    )
    insert_rol = session.prepare(
        """
        INSERT INTO tickets_por_rol
        (rol, fecha_creacion, ticket_id, user_id, categoria, estado)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    )
    insert_departamento = session.prepare(
        """
        INSERT INTO tickets_por_instalaciones
        (departamento, fecha, ticket_id, estado, categoria)
        VALUES (?, ?, ?, ?, ?)
        """
    )

    # Sentencias para counters
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
            (install_id, created_at, ticket_id, categoria, estado, prioridad),
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
            (user_id, fecha_dia, created_at, ticket_id, categoria, estado),
        )

        # 10) tickets_por_rol
        if rol is not None:
            session.execute(
                insert_rol,
                (rol, created_at, ticket_id, user_id, categoria, estado),
            )

        # 11) tickets_por_prioridad 
        session.execute(update_prioridad, (prioridad,))

        # 12) tickets_por_instalaciones (usamos installation_id como departamento)
        departamento = install_id
        session.execute(
            insert_departamento,
            (departamento, created_at, ticket_id, estado, categoria),
        )

        # 13) tickets_por_turno 
        turno = ticket.get("turno") or _get_turno(created_at)
        session.execute(update_turno, (turno,))

    print("Populate Cassandra: inserciones completadas.")


# ---------- Dgraph ----------


def populate_dgraph():
    """
    Llena Dgraph usando los tickets que ya estan en Mongo
    Crea nodos usuario, instalacion y ticket y relaciones creo y afecta.
    """
   
    print("=== Populate Dgraph ===")

    # Crear cliente de Dgraph
    stub = create_client_stub()
    client = create_client(stub)

    # 1) Definimos esquema simple
    schema = """
    user_id: string @index(exact) .
    email: string @index(exact) .
    rol: string @index(exact) .

    ticket_id: string @index(exact) .
    titulo: string @index(term) .
    descripcion: string @index(fulltext) .
    estado: string @index(exact) .
    prioridad: string @index(exact) .
    fecha_creacion: datetime .

    instal_id: string @index(exact) .
    instal_nombre: string .

    creado_por: uid @reverse .
    afecta: uid @reverse .

    type Usuario {
      user_id
      email
      rol
    }

    type Ticket {
      ticket_id
      titulo
      descripcion
      estado
      prioridad
      fecha_creacion
      creado_por
      afecta
    }

    type Instalacion {
      instal_id
      instal_nombre
    }
    """

    op = pydgraph.Operation(schema=schema)
    client.alter(op)

    # 2) Leer datos de Mongo
    usuarios = list(db.users.find())
    tickets = list(db.tickets.find())

    objetos = []

    # 3) Crear nodos de Usuario
    for u in usuarios:
        user_id = u.get("user_id")
        if not user_id:
            continue

        obj = {
            "uid": f"_:u_{user_id}",
            "dgraph.type": "Usuario",
            "user_id": user_id,
        }

        email = u.get("email")
        if email:
            obj["email"] = email

        rol = u.get("role")
        if rol:
            obj["rol"] = rol

        objetos.append(obj)

    # 4) Crear nodos de Instalacion (a partir de tickets)
    instalaciones_vistas = set()

    for t in tickets:
        instal_id = t.get("installation_id")
        if not instal_id:
            continue
        if instal_id in instalaciones_vistas:
            continue

        instalaciones_vistas.add(instal_id)

        obj = {
            "uid": f"_:i_{instal_id}",
            "dgraph.type": "Instalacion",
            "instal_id": instal_id,
            "instal_nombre": instal_id,
        }
        objetos.append(obj)

    # 5) Crear nodos de Ticket y relaciones
    ahora = datetime.utcnow()

    for t in tickets:
        ticket_id = t.get("ticket_id")
        if not ticket_id:
            continue

        creado = t.get("created_at") or ahora
        if isinstance(creado, datetime):
            fecha_str = creado.isoformat()
        else:
            fecha_str = str(creado)

        ticket_obj = {
            "uid": f"_:t_{ticket_id}",
            "dgraph.type": "Ticket",
            "ticket_id": ticket_id,
            "titulo": t.get("title", ""),
            "descripcion": t.get("description", ""),
            "estado": t.get("status", ""),
            "prioridad": t.get("priority", ""),
            "fecha_creacion": fecha_str,
        }

        user_id = t.get("user_id")
        if user_id:
            ticket_obj["creado_por"] = {"uid": f"_:u_{user_id}"}

        instal_id = t.get("installation_id")
        if instal_id:
            ticket_obj["afecta"] = {"uid": f"_:i_{instal_id}"}

        objetos.append(ticket_obj)

        # 6) Enviar todo a Dgraph en una sola mutacion JSON
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

    


def main():
    print("=== Populate: Mongo + Cassandra + Dgraph ===")
    populate_mongo()
    populate_cassandra()
    populate_dgraph() 
    print("=== Populate completado ===")


if __name__ == "__main__":
    main()
