import csv
from datetime import datetime
from pymongo.errors import DuplicateKeyError

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
    if 15 <= hour < 22:
        return "tarde"
    return "noche"


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
        "tickets_por_instalacion",
        "tickets_por_estado",
        "filtrado_tickets_por_fecha",
        "tickets_por_usuario_dia",
        "tickets_por_rol",
        "conteo_tickets_por_prioridad",
        "tickets_por_departamento",
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
        INSERT INTO tickets_por_instalacion
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
        INSERT INTO tickets_por_departamento
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
        created_at = ticket.get("created_at") or now
        fecha_dia = created_at.date()

        user = usuarios.get(user_id, {})
        rol = user.get("role")
        email = user.get("email", user_id)
        descripcion = ticket.get("description", "")

        # 1) alertas_tickets_vencidos: dias inactivos = diferencia con hoy
        dias_inactivos = (now - created_at).days
        session.execute(
            insert_alerta,
            (dias_inactivos, ticket_id, created_at, estado),
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

        # 6) tickets_por_instalacion
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

        # 12) tickets_por_departamento (usamos installation_id como departamento)
        departamento = install_id
        session.execute(
            insert_departamento,
            (departamento, created_at, ticket_id, estado, categoria),
        )

        # 13) tickets_por_turno 
        turno = _get_turno(created_at)
        session.execute(update_turno, (turno,))

    print("Populate Cassandra: inserciones completadas.")


# ---------- Dgraph ----------


def populate_dgraph():
    """
    DESACTIVADO por ahora.
    Mas adelante aqui podemos leer de Mongo (db.tickets)
    y crear nodos/aristas en Dgraph.
    """
    print("Populate Dgraph desactivado (aun no implementado).")


def main():
    print("=== Populate: Mongo + Cassandra (Dgraph desactivado) ===")
    populate_mongo()
    populate_cassandra()
    # populate_dgraph()  # Lo activaremos cuando terminemos la parte de Dgraph
    print("=== Populate completado ===")


if __name__ == "__main__":
    main()
