import logging
from datetime import datetime, date

# Logger
log = logging.getLogger()


# Crear el keyspace si no existe
CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""


# 1. Alerta de tickets vencidos
CREATE_ALERTAS_TICKETS_VENCIDOS_TABLE = """
    CREATE TABLE IF NOT EXISTS alertas_tickets_vencidos (
        dias_inactivos int,
        ticket_id text,
        fecha_ultimo_cambio timestamp,
        estado_actual text,
        PRIMARY KEY (dias_inactivos, ticket_id)
    ) WITH CLUSTERING ORDER BY (ticket_id ASC);
"""


# 2. Historial por usuario
CREATE_HISTORIAL_POR_USUARIO_TABLE = """
    CREATE TABLE IF NOT EXISTS historial_por_usuario (
        user_id text,
        fecha timestamp,
        ticket_id text,
        categoria text,
        estado text,
        PRIMARY KEY (user_id, fecha, ticket_id)
    ) WITH CLUSTERING ORDER BY (fecha DESC, ticket_id ASC);
"""


# 3. Conteo de tickets por categoria y dia
CREATE_CONTEO_TICKETS_POR_CATEGORIA_DIA_TABLE = """
    CREATE TABLE IF NOT EXISTS conteo_tickets_por_categoria_dia (
        fecha date,
        categoria text,
        total counter,
        PRIMARY KEY (fecha, categoria)
    );
"""


# 4. Tickets por profesor
CREATE_TICKETS_POR_PROFESOR_TABLE = """
    CREATE TABLE IF NOT EXISTS tickets_por_profesor (
        profesor_id text,
        fecha_creacion timestamp,
        ticket_id text,
        categoria text,
        estado text,
        descripcion text,
        PRIMARY KEY (profesor_id, fecha_creacion, ticket_id)
    ) WITH CLUSTERING ORDER BY (fecha_creacion DESC, ticket_id ASC);
"""


# 5. Historial de un ticket
CREATE_HISTORIAL_TICKET_TABLE = """
    CREATE TABLE IF NOT EXISTS historial_ticket (
        ticket_id text,
        fecha timestamp,
        evento text,
        usuario text,
        estado_anterior text,
        estado_nuevo text,
        PRIMARY KEY (ticket_id, fecha)
    ) WITH CLUSTERING ORDER BY (fecha ASC);
"""


# 6. Tickets por instalacion (por rango de fechas)
CREATE_TICKETS_POR_INSTALACION_TABLE = """
    CREATE TABLE IF NOT EXISTS tickets_por_instalacion_fechas (
        install_id text,
        fecha timestamp,
        ticket_id text,
        categoria text,
        estado text,
        prioridad text,
        descripcion text,
        PRIMARY KEY (install_id, fecha, ticket_id)
    ) WITH CLUSTERING ORDER BY (fecha DESC, ticket_id ASC);
"""


# 7. Tickets por estado
CREATE_TICKETS_POR_ESTADO_TABLE = """
    CREATE TABLE IF NOT EXISTS tickets_por_estado (
        estado text,
        fecha timestamp,
        ticket_id text,
        categoria text,
        user_id text,
        PRIMARY KEY (estado, fecha, ticket_id)
    ) WITH CLUSTERING ORDER BY (fecha DESC, ticket_id ASC);
"""


# 8. Filtrado de tickets por fecha
CREATE_FILTRADO_TICKETS_POR_FECHA_TABLE = """
    CREATE TABLE IF NOT EXISTS filtrado_tickets_por_fecha (
        fecha timestamp,
        ticket_id text,
        user_id text,
        categoria text,
        estado text,
        prioridad text,
        PRIMARY KEY (fecha, ticket_id)
    );
"""


# 9. Tickets por usuario y dia
CREATE_TICKETS_POR_USUARIO_DIA_TABLE = """
    CREATE TABLE IF NOT EXISTS tickets_por_usuario_dia (
        user_id text,
        fecha date,
        hora timestamp,
        ticket_id text,
        categoria text,
        estado text,
        descripcion text,
        PRIMARY KEY ((user_id, fecha), hora, ticket_id)
    ) WITH CLUSTERING ORDER BY (hora DESC, ticket_id ASC);
"""


# 10. Tickets por rol de usuario
CREATE_TICKETS_POR_ROL_TABLE = """
    CREATE TABLE IF NOT EXISTS tickets_por_rol (
        rol text,
        fecha_creacion timestamp,
        ticket_id text,
        user_id text,
        categoria text,
        estado text,
        PRIMARY KEY (rol, fecha_creacion, ticket_id)
    ) WITH CLUSTERING ORDER BY (fecha_creacion DESC, ticket_id ASC);
"""


# 11. Conteo de tickets por prioridad
CREATE_CONTEO_TICKETS_POR_PRIORIDAD_TABLE = """
    CREATE TABLE IF NOT EXISTS conteo_tickets_por_prioridad (
        prioridad text,
        total counter,
        PRIMARY KEY (prioridad)
    );
"""


# 12. Tickets por instalaciones (usando install_id como instalacion)
CREATE_TICKETS_POR_INSTALACIONES_TABLE = """
    CREATE TABLE IF NOT EXISTS tickets_por_instalaciones (
        instalacion text,
        fecha timestamp,
        ticket_id text,
        estado text,
        categoria text,
        PRIMARY KEY (instalacion, fecha, ticket_id)
    ) WITH CLUSTERING ORDER BY (fecha DESC, ticket_id ASC);
"""


# 13. Tickets por turno
CREATE_TICKETS_POR_TURNO_TABLE = """
    CREATE TABLE IF NOT EXISTS tickets_por_turno (
        turno text,
        total_tickets counter,
        PRIMARY KEY (turno)
    );
"""


def create_keyspace(session, keyspace, replication_factor):
    """Crea el keyspace indicado si aun no existe."""
    log.info(
        f"Creando keyspace: {keyspace} con replication_factor={replication_factor}"
    )
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    """Crea todas las tablas del modelo de soporte al cliente."""
    log.info("Creando tablas de Cassandra para soporte al cliente")

    session.execute(CREATE_ALERTAS_TICKETS_VENCIDOS_TABLE)
    session.execute(CREATE_HISTORIAL_POR_USUARIO_TABLE)
    session.execute(CREATE_CONTEO_TICKETS_POR_CATEGORIA_DIA_TABLE)
    session.execute(CREATE_TICKETS_POR_PROFESOR_TABLE)
    session.execute(CREATE_HISTORIAL_TICKET_TABLE)
    session.execute(CREATE_TICKETS_POR_INSTALACION_TABLE)
    session.execute(CREATE_TICKETS_POR_ESTADO_TABLE)
    session.execute(CREATE_FILTRADO_TICKETS_POR_FECHA_TABLE)
    session.execute(CREATE_TICKETS_POR_USUARIO_DIA_TABLE)
    session.execute(CREATE_TICKETS_POR_ROL_TABLE)
    session.execute(CREATE_CONTEO_TICKETS_POR_PRIORIDAD_TABLE)
    session.execute(CREATE_TICKETS_POR_INSTALACIONES_TABLE)
    session.execute(CREATE_TICKETS_POR_TURNO_TABLE)


# =========================
# Funciones de consultas
# =========================


def _parse_date(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _parse_timestamp(ts_str: str) -> datetime:
    # Permite 'YYYY-MM-DD' o 'YYYY-MM-DD HH:MM'
    ts_str = ts_str.strip()
    if len(ts_str) == 10:
        return datetime.strptime(ts_str, "%Y-%m-%d")
    return datetime.fromisoformat(ts_str)


def alertas_tickets_vencidos(session, dias_minimos: int = 5):
    """
    1) Alerta de tickets vencidos:
       SELECT ticket_id, dias_inactivos
       FROM alertas_tickets_vencidos
       WHERE dias_inactivos > 5;
    """
    log.debug("Q1 - alertas_tickets_vencidos")
    stmt = session.prepare(
        """
        SELECT dias_inactivos, ticket_id, fecha_ultimo_cambio, estado_actual
        FROM alertas_tickets_vencidos
        WHERE dias_inactivos > ?
        ALLOW FILTERING
        """
    )
    rows = session.execute(stmt, (dias_minimos,))
    print(f"\n=== Tickets con mas de {dias_minimos} dias inactivos ===")
    for r in rows:
        print(
            f"- Ticket {r.ticket_id}: {r.dias_inactivos} dias, "
            f"ultimo cambio {r.fecha_ultimo_cambio}, estado {r.estado_actual}"
        )


def historial_por_usuario(session, user_id: str):
    """
    2) Historial por usuario:
       SELECT user_id, fecha, ticket_id, categoria, estado
       FROM historial_por_usuario
       WHERE user_id = 'U-001';
    """
    log.debug("Q2 - historial_por_usuario")
    stmt = session.prepare(
        """
        SELECT user_id, fecha, ticket_id, categoria, estado
        FROM historial_por_usuario
        WHERE user_id = ?
        """
    )
    rows = session.execute(stmt, (user_id,))
    print(f"\n=== Historial de usuario {user_id} ===")
    for r in rows:
        print(
            f"{r.fecha} - Ticket {r.ticket_id} "
            f"[{r.categoria}] estado={r.estado}"
        )


def tickets_por_categoria_dia(session, fecha_str: str):
    """
    3) Conteo de tickets por categoria y dia:
       SELECT fecha, categoria, total
       FROM conteo_tickets_por_categoria_dia
       WHERE fecha = '2025-10-01';
    """
    log.debug("Q3 - conteo_tickets_por_categoria_dia")
    fecha = _parse_date(fecha_str)
    stmt = session.prepare(
        """
        SELECT fecha, categoria, total
        FROM conteo_tickets_por_categoria_dia
        WHERE fecha = ?
        """
    )
    rows = session.execute(stmt, (fecha,))
    print(f"\n=== Tickets por categoria en fecha {fecha} ===")
    for r in rows:
        print(f"- Categoria {r.categoria}: total={r.total}")


def tickets_por_profesor(session, profesor_id: str):
    """
    4) Tickets por profesor:
       SELECT profesor_id, ticket_id, categoria, estado, fecha_creacion
       FROM tickets_por_profesor
       WHERE profesor_id = 'PR-01';
    """
    log.debug("Q4 - tickets_por_profesor")
    stmt = session.prepare(
        """
        SELECT profesor_id, fecha_creacion, ticket_id, categoria, estado, descripcion
        FROM tickets_por_profesor
        WHERE profesor_id = ?
        """
    )
    rows = session.execute(stmt, (profesor_id,))
    print(f"\n=== Tickets del profesor {profesor_id} ===")
    for r in rows:
        print(
            f"{r.fecha_creacion} - Ticket {r.ticket_id} "
            f"[{r.categoria}] estado={r.estado}"
        )


def historial_ticket(session, ticket_id: str):
    """
    5) Historial de un ticket:
       SELECT ticket_id, evento, fecha
       FROM historial_ticket
       WHERE ticket_id = 'TK-1001'
       ORDER BY fecha ASC;
    """
    log.debug("Q5 - historial_ticket")
    stmt = session.prepare(
        """
        SELECT ticket_id, fecha, evento, usuario, estado_anterior, estado_nuevo
        FROM historial_ticket
        WHERE ticket_id = ?
        """
    )
    rows = session.execute(stmt, (ticket_id,))
    print(f"\n=== Historial del ticket {ticket_id} ===")
    for r in rows:
        print(
            f"{r.fecha} - {r.evento} por {r.usuario}: "
            f"{r.estado_anterior} -> {r.estado_nuevo}"
        )


def tickets_por_instalacion_rango(
    session, install_id: str, fecha_inicio: str, fecha_fin: str
):
    """
    6) Tickets por instalacion (rango de fechas):
       SELECT install_id, fecha, ticket_id, categoria, estado
       FROM tickets_por_instalacion_fechas
       WHERE install_id = 'DESI'
         AND fecha >= '2025-10-01'
         AND fecha <= '2025-10-15';
    """
    log.debug("Q6 - tickets_por_instalacion_fechas")
    fi = _parse_timestamp(fecha_inicio)
    ff = _parse_timestamp(fecha_fin)
    stmt = session.prepare(
        """
        SELECT install_id, fecha, ticket_id, categoria, estado, prioridad, descripcion
        FROM tickets_por_instalacion_fechas
        WHERE install_id = ?
          AND fecha >= ?
          AND fecha <= ?
        """
    )
    rows = session.execute(stmt, (install_id, fi, ff))
    print(
        f"\n=== Tickets en instalacion {install_id} "
        f"entre {fi} y {ff} ==="
    )
    for r in rows:
        print(
            f"{r.fecha} - Ticket {r.ticket_id} "
            f"[{r.categoria}] estado={r.estado} prioridad={r.prioridad} "
            f"desc={getattr(r, 'descripcion', '')}"
        )


def tickets_por_estado(session, estado: str):
    """
    7) Tickets por estado:
       SELECT * 
       FROM tickets_por_estado
       WHERE estado = 'abierto';
    """
    log.debug("Q7 - tickets_por_estado")
    stmt = session.prepare(
        """
        SELECT estado, fecha, ticket_id, categoria, user_id
        FROM tickets_por_estado
        WHERE estado = ?
        """
    )
    rows = session.execute(stmt, (estado,))
    print(f"\n=== Tickets en estado {estado} ===")
    for r in rows:
        print(
            f"{r.fecha} - Ticket {r.ticket_id} "
            f"[{r.categoria}] usuario={r.user_id}"
        )


def tickets_por_fecha_rango(
    session, fecha_inicio: str, fecha_fin: str
):
    """
    8) Filtrado de tickets por fecha:
       SELECT *
       FROM filtrado_tickets_por_fecha
       WHERE fecha >= '2025-10-01'
         AND fecha <= '2025-10-15';
    """
    log.debug("Q8 - tickets por fecha (ALLOW FILTERING)")
    fi = _parse_timestamp(fecha_inicio)
    ff = _parse_timestamp(fecha_fin)
    stmt = session.prepare(
        """
        SELECT fecha, ticket_id, user_id, categoria, estado, prioridad
        FROM filtrado_tickets_por_fecha
        WHERE fecha >= ?
          AND fecha <= ?
        ALLOW FILTERING
        """
    )
    rows = session.execute(stmt, (fi, ff))
    print(f"\n=== Tickets entre {fi} y {ff} ===")
    for r in rows:
        print(
            f"{r.fecha} - Ticket {r.ticket_id} "
            f"[{r.categoria}] estado={r.estado} prioridad={r.prioridad}"
        )


def tickets_por_usuario_dia(
    session, user_id: str, fecha_str: str
):
    """
    9) Tickets por usuario y dia:
       SELECT user_id, fecha, ticket_id, categoria, estado
       FROM tickets_por_usuario_dia
       WHERE user_id = 'U-001'
         AND fecha = '2025-10-11';
    """
    log.debug("Q9 - tickets_por_usuario_dia")
    fecha = _parse_date(fecha_str)
    stmt = session.prepare(
        """
        SELECT user_id, fecha, hora, ticket_id, categoria, estado, descripcion
        FROM tickets_por_usuario_dia
        WHERE user_id = ? AND fecha = ?
        """
    )
    rows = session.execute(stmt, (user_id, fecha))
    print(f"\n=== Tickets del usuario {user_id} en {fecha} ===")
    for r in rows:
        print(
            f"{r.hora} - Ticket {r.ticket_id} "
            f"[{r.categoria}] estado={r.estado} "
            f"desc={getattr(r, 'descripcion', '')}"
        )


def tickets_por_rol(session, rol: str):
    """
    10) Tickets por rol de usuario:
        SELECT rol, fecha_creacion, ticket_id, user_id, categoria, estado
        FROM tickets_por_rol
        WHERE rol = 'docente';
    """
    log.debug("Q10 - tickets_por_rol")
    stmt = session.prepare(
        """
        SELECT rol, fecha_creacion, ticket_id, user_id, categoria, estado
        FROM tickets_por_rol
        WHERE rol = ?
        """
    )
    rows = session.execute(stmt, (rol,))
    print(f"\n=== Tickets para rol {rol} ===")
    for r in rows:
        print(
            f"{r.fecha_creacion} - Ticket {r.ticket_id} "
            f"[{r.categoria}] usuario={r.user_id} estado={r.estado}"
        )


def conteo_por_prioridad(session):
    """
    11) Conteo de tickets por prioridad:
        SELECT prioridad, total
        FROM conteo_tickets_por_prioridad;
    """
    log.debug("Q11 - conteo_tickets_por_prioridad")
    rows = session.execute(
        """
        SELECT prioridad, total
        FROM conteo_tickets_por_prioridad
        """
    )
    print("\n=== Conteo de tickets por prioridad ===")
    for r in rows:
        print(f"- {r.prioridad}: {r.total}")


def tickets_por_instalaciones(session, instalacion: str):
    """
    12) Tickets por instalaciones:
        SELECT instalacion, ticket_id, estado
        FROM tickets_por_instalaciones
        WHERE instalacion = 'DESI';
    """
    log.debug("Q12 - tickets_por_instalaciones")
    stmt = session.prepare(
        """
        SELECT instalacion, fecha, ticket_id, estado, categoria
        FROM tickets_por_instalaciones
        WHERE instalacion = ?
        """
    )
    rows = session.execute(stmt, (instalacion,))
    print(f"\n=== Tickets de la instalacion {instalacion} ===")
    for r in rows:
        print(f"Ticket {r.ticket_id} [{r.categoria}] estado={r.estado}")


def tickets_por_turno(session):
    """
    13) Tickets por turno:
        SELECT turno, total_tickets
        FROM tickets_por_turno;
    """
    log.debug("Q13 - tickets_por_turno")
    rows = session.execute(
        """
        SELECT turno, total_tickets
        FROM tickets_por_turno
        """
    )
    print("\n=== Conteo de tickets por turno ===")
    for r in rows:
        print(f"- {r.turno}: {r.total_tickets}")

