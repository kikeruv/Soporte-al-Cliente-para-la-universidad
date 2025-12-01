from connect import (
    get_cassandra_session,
    create_client_stub,
    create_client,
    close_client_stub,
    db,
)

from Mongo.client import (
    filtrar_por_categoria,
    resumen_estado,
    buscar_titulos_falla,
    lugares_con_mas_perdidas,
    instalaciones_con_mas_incidencias,
    buscar_por_texto,
    resumen_objetos_perdidos,
    tickets_cerrados_por_categoria,
    mostrar_usuarios,
)
from Cassandra import model as cass_model

import populate


# Helper para asegurar que el esquema de Cassandra se cree solo una vez
_cassandra_session = None
_cassandra_schema_creada = False


def get_cassandra_session_con_schema():
    """
    Obtiene una sesion de Cassandra y se asegura de que
    el esquema (tablas) se haya creado solo una vez.
    """
    global _cassandra_session, _cassandra_schema_creada

    if _cassandra_session is None:
        try:
            _cassandra_session = get_cassandra_session()
        except Exception as e:
            print("\nError al conectar con Cassandra:", e)
            return None

    if not _cassandra_schema_creada:
        cass_model.create_schema(_cassandra_session)
        _cassandra_schema_creada = True

    return _cassandra_session

##### Esto esta hecho con Chat ya que solo es para verificar si estan conectadas las bases de datos 
def test_connections():
    print("\nProbando conexiones a las 3 bases de datos")

       # Cassandra
    try:
        session = get_cassandra_session()
        print("✅ Cassandra conectada")
    except Exception as e:
        print("❌ Error en Cassandra:", e)

    # Mongo
    try:
        _ = db.list_collection_names()
        print("✅ MongoDB conectado")
    except Exception as e:
        print("❌ Error en Mongo:", e)

    # Dgraph
    stub = None
    try:
        stub = create_client_stub()
        client = create_client(stub)
        client.check_version()
        print("✅ Dgraph conectado")
    except Exception as e:
        print("❌ Error en Dgraph:", e)
    finally:
        if stub is not None:
            try:
                close_client_stub(stub)
            except Exception:
                pass


def borrar_datos():
    """
    Borra todos los datos de Mongo (users, tickets)
    y todas las tablas de Cassandra del keyspace de proyecto.
    """
    print(
        "\nADVERTENCIA: Esta opcion borrara TODOS los datos "
        "en Mongo (users, tickets) y Cassandra (tablas de proyecto)."
    )
    confirm = input("Escribe 'SI' para continuar: ").strip().upper()
    if confirm != "SI":
        print("Operacion cancelada.")
        return

    # Mongo
    try:
        db.users.delete_many({})
        db.tickets.delete_many({})
        print("Mongo: colecciones 'users' y 'tickets' vaciadas.")
    except Exception as e:
        print("Error al borrar datos en Mongo:", e)

    # Cassandra
    try:
        session = get_cassandra_session()
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
        print("Cassandra: tablas de proyecto truncadas.")
    except Exception as e:
        print("Error al borrar datos en Cassandra:", e)


##### Submenus por tipo de reporte #####
def menu_instalaciones():
    # Usaremos Mongo y Cassandra relacionados con instalaciones
    session = get_cassandra_session_con_schema()
    if session is None:
        return

    while True:
        print("\n=== Instalaciones M y C ===")
        print("1. Instalaciones con mas incidencias M")
        print("2. Tickets por instalacion y rango de fechas C")
        print("3. Tickets por departamento C")
        print("0. Volver al menu principal")

        try:
            op = int(input("Opcion: ").strip())
        except ValueError:
            print("Opcion invalida.")
            continue

        if op == 0:
            break
        elif op == 1:
            instalaciones_con_mas_incidencias()
        elif op == 2:
            install_id = input("install_id (ej. DESI): ").strip()
            f1 = input("Fecha inicio (YYYY-MM-DD): ").strip()
            f2 = input("Fecha fin (YYYY-MM-DD): ").strip()
            cass_model.tickets_por_instalacion_rango(session, install_id, f1, f2)
        elif op == 3:
            depto = input("Departamento (ej. DESI): ").strip()
            cass_model.tickets_por_departamento(session, depto)
        else:
            print("Opcion no valida.")


def menu_cosas_perdidas():
    while True:
        print("\n=== Cosas perdidas M ===")
        print("1. Resumen de objetos perdidos M")
        print("2. Lugares con mas reportes de perdidas M")
        print("0. Volver al menu principal")

        try:
            op = int(input("Opcion: ").strip())
        except ValueError:
            print("Opcion invalida.")
            continue

        if op == 0:
            break
        elif op == 1:
            resumen_objetos_perdidos()
        elif op == 2:
            lugares_con_mas_perdidas()
        else:
            print("Opcion no valida.")


def menu_tickets():
    # Mezcla de reportes de tickets en Mongo y Cassandra
    session = get_cassandra_session_con_schema()
    if session is None:
        return

    while True:
        print("\n=== Tickets M + C ===")
        print("1. Total de tickets por categoria M")
        print("2. Total de tickets por estado M")
        print("3. Titulos que empiezan con 'Falla' o 'Dano' M")
        print("4. Busqueda por texto en tickets M")
        print("5. Tickets cerrados por categoria M")
        print("6. Alertas de tickets vencidos C")
        print("7. Conteo de tickets por categoria y dia C")
        print("8. Historial de un ticket C")
        print("9. Tickets por estado C")
        print("10. Tickets por rango de fechas (global) C")
        print("11. Conteo de tickets por prioridad C")
        print("12. Tickets por turno C")
        print("0. Volver al menu principal")

        try:
            op = int(input("Opcion: ").strip())
        except ValueError:
            print("Opcion invalida.")
            continue

        if op == 0:
            break
        elif op == 1:
            filtrar_por_categoria()
        elif op == 2:
            resumen_estado()
        elif op == 3:
            buscar_titulos_falla()
        elif op == 4:
            buscar_por_texto()
        elif op == 5:
            tickets_cerrados_por_categoria()
        elif op == 6:
            dias = int(input("Dias inactivos mayores a (ej. 5): ").strip() or "5")
            cass_model.alertas_tickets_vencidos(session, dias)
        elif op == 7:
            fecha = input("Fecha (YYYY-MM-DD): ").strip()
            cass_model.tickets_por_categoria_dia(session, fecha)
        elif op == 8:
            ticket_id = input("ticket_id (ej. TK-2001): ").strip()
            cass_model.historial_ticket(session, ticket_id)
        elif op == 9:
            estado = input("Estado (abierto, en_proceso, cerrado): ").strip()
            cass_model.tickets_por_estado(session, estado)
        elif op == 10:
            f1 = input("Fecha inicio (YYYY-MM-DD): ").strip()
            f2 = input("Fecha fin (YYYY-MM-DD): ").strip()
            cass_model.tickets_por_fecha_rango(session, f1, f2)
        elif op == 11:
            cass_model.conteo_por_prioridad(session)
        elif op == 12:
            cass_model.tickets_por_turno(session)
        else:
            print("Opcion no valida.")


def menu_docentes():
    # Reportes academicos / docentes en Cassandra
    session = get_cassandra_session_con_schema()
    if session is None:
        return

    while True:
        print("\n=== Docentes / reportes academicos C ===")
        print("1. Tickets por profesor C")
        print("2. Tickets por rol de usuario C")
        print("0. Volver al menu principal")

        try:
            op = int(input("Opcion: ").strip())
        except ValueError:
            print("Opcion invalida.")
            continue

        if op == 0:
            break
        elif op == 1:
            profesor_id = input("profesor_id (user_id del docente): ").strip()
            cass_model.tickets_por_profesor(session, profesor_id)
        elif op == 2:
            rol = input("Rol (docente/estudiante): ").strip()
            cass_model.tickets_por_rol(session, rol)
        else:
            print("Opcion no valida.")


def menu_usuarios():
    # Usuarios en Mongo y actividad en Cassandra
    session = get_cassandra_session_con_schema()
    if session is None:
        return

    while True:
        print("\n=== Usuarios M + C ===")
        print("1. Mostrar todos los usuarios M")
        print("2. Historial de tickets por usuario C")
        print("3. Tickets por usuario y dia C")
        print("0. Volver al menu principal")

        try:
            op = int(input("Opcion: ").strip())
        except ValueError:
            print("Opcion invalida.")
            continue

        if op == 0:
            break
        elif op == 1:
            mostrar_usuarios()
        elif op == 2:
            user_id = input("user_id (ej. u-001): ").strip()
            cass_model.historial_por_usuario(session, user_id)
        elif op == 3:
            user_id = input("user_id: ").strip()
            fecha = input("Fecha (YYYY-MM-DD): ").strip()
            cass_model.tickets_por_usuario_dia(session, user_id, fecha)
        else:
            print("Opcion no valida.")


##### Menu principal #####
def print_menu_principal():
    print("\n=== Sistema de Soporte - Menu Principal ===")
    print("1. Cosas perdidas M")
    print("2. Instalaciones M + C")
    print("3. Docentes / reportes academicos C")
    print("4. Tickets M + C")
    print("5. Usuarios M + C")
    print("6. Probar conexiones M + C + D")
    print("7. Ejecutar populate M + C")
    print("8. Borrar TODOS los datos M + C")
    print("0. Salir")


def main():
    while True:
        print_menu_principal()

        try:
            opcion = int(input("Seleccione una opcion: ").strip())
        except ValueError:
            print("Opcion invalida. Debe ser un numero.\n")
            continue

        if opcion == 0:
            print("\nSaliendo del sistema")
            break

        # Reportes por tipo
        elif opcion == 1:
            menu_cosas_perdidas()
        elif opcion == 2:
            menu_instalaciones()
        elif opcion == 3:
            menu_docentes()
        elif opcion == 4:
            menu_tickets()
        elif opcion == 5:
            menu_usuarios()

        # Utilidades
        elif opcion == 6:
            test_connections()
        elif opcion == 7:
            print("\nEjecutando populate de M + C\n")
            populate.main()
            print()
        elif opcion == 8:
            borrar_datos()

        else:
            print("\nOpcion no valida.\n")


if __name__ == "__main__":
    main()