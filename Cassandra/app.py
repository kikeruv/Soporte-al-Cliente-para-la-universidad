#!/usr/bin/env python3
import logging
import os

from cassandra.cluster import Cluster

import model


# set logger
log = logging.getLogger()
log.setLevel("INFO")
handler = logging.FileHandler("proyecto.log")
handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
log.addHandler(handler)

CLUSTER_IPS = os.getenv("CASSANDRA_CLUSTER_IPS", "127.0.0.1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "proyecto")
REPLICATION_FACTOR = os.getenv("CASSANDRA_REPLICATION_FACTOR", "1")


def print_menu():
    opciones = {
        1: "Q1 - Alertas de tickets vencidos",
        2: "Q2 - Historial por usuario",
        3: "Q3 - Conteo por categoria y dia",
        4: "Q4 - Tickets por profesor",
        5: "Q5 - Historial de un ticket",
        6: "Q6 - tickets_por_instalacion_fechas",
        7: "Q7 - Tickets por estado",
        8: "Q8 - Tickets por rango de fechas (global)",
        9: "Q9 - Tickets por usuario y dia",
        10: "Q10 - Tickets por rol de usuario",
        11: "Q11 - Conteo de tickets por prioridad",
        12: "Q12 - Tickets por instalaciones",
        13: "Q13 - Tickets por turno",
        0: "Salir",
    }
    print("\n=== Menu de consultas Cassandra ===")
    for k in sorted(opciones.keys()):
        print(f"{k}. {opciones[k]}")


def main():
    log.info("Conectando a Cassandra")
    cluster = Cluster(CLUSTER_IPS.split(","))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    print(f"Cassandra listo. KEYSPACE en uso: {KEYSPACE}")

    while True:
        print_menu()
        try:
            opcion = int(input("\nSelecciona una opcion: ").strip())
        except ValueError:
            print("Opcion invalida, intenta de nuevo.")
            continue

        if opcion == 0:
            print("Saliendo de Cassandra CLI...")
            break

        elif opcion == 1:
            dias = int(input("Dias inactivos mayores a: ").strip() or "5")
            model.alertas_tickets_vencidos(session, dias)

        elif opcion == 2:
            user_id = input("user_id (ej. u-001): ").strip()
            model.historial_por_usuario(session, user_id)

        elif opcion == 3:
            fecha = input("Fecha (YYYY-MM-DD): ").strip()
            model.tickets_por_categoria_dia(session, fecha)

        elif opcion == 4:
            profesor_id = input("profesor_id (user_id del docente): ").strip()
            model.tickets_por_profesor(session, profesor_id)

        elif opcion == 5:
            ticket_id = input("ticket_id (ej. TK-2001): ").strip()
            model.historial_ticket(session, ticket_id)

        elif opcion == 6:
            install_id = input("install_id (ej. biblioteca): ").strip()
            f1 = input("Fecha inicio (YYYY-MM-DD): ").strip()
            f2 = input("Fecha fin (YYYY-MM-DD): ").strip()
            model.tickets_por_instalacion_rango(session, install_id, f1, f2)

        elif opcion == 7:
            estado = input("Estado (abierto, en_proceso, cerrado): ").strip()
            model.tickets_por_estado(session, estado)

        elif opcion == 8:
            f1 = input("Fecha inicio (YYYY-MM-DD): ").strip()
            f2 = input("Fecha fin (YYYY-MM-DD): ").strip()
            model.tickets_por_fecha_rango(session, f1, f2)

        elif opcion == 9:
            user_id = input("user_id: ").strip()
            fecha = input("Fecha (YYYY-MM-DD): ").strip()
            model.tickets_por_usuario_dia(session, user_id, fecha)

        elif opcion == 10:
            rol = input("Rol (docente/estudiante): ").strip()
            model.tickets_por_rol(session, rol)

        elif opcion == 11:
            model.conteo_por_prioridad(session)

        elif opcion == 12:
            depto = input("install_id / instalacion (ej. DESI): ").strip()
            model.tickets_por_instalaciones(session, depto)

        elif opcion == 13:
            model.tickets_por_turno(session)

        else:
            print("Opcion no valida.")


if __name__ == "__main__":
    main()
