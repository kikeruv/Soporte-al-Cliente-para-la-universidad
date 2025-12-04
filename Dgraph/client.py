import sys
import os
# Permite importar 'connect.py' desde la carpeta raíz del proyecto.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connect import create_client_stub, create_client, close_client_stub
import pydgraph

from datetime import datetime
import json


def get_client():
    """
    Crea y devuelve un cliente de Dgraph y su stub.
    """
    stub = create_client_stub()
    client = create_client(stub)
    return client, stub


def _normalizar_user_id(raw: str) -> str:
    """
    Adapta el valor ingresado al formato real de user_id usado en los datos (por ejemplo, 'U001').
    Acepta formas como 'U-001', 'U001', '001' y devuelve siempre 'U001'.
    """
    if not raw:
        return ""
    s = raw.strip().upper()
    s = s.replace("-", "").replace(" ", "")
    if s.startswith("U"):
        numeros = "".join(ch for ch in s[1:] if ch.isdigit())
    else:
        numeros = "".join(ch for ch in s if ch.isdigit())
    if not numeros:
        return ""
    return "U" + numeros.zfill(3)


# 1) Relacion usuario-ticket
#    Requerimiento: saber quien creó cada ticket y listar todos los tickets de un usuario.
def reporte_usuario_ticket():
    raw_id = input('user_id (ej. "U-001", vacío = todos): ').strip()
    user_id = _normalizar_user_id(raw_id)

    client, stub = get_client()
    try:
        if user_id:
            base_query = """
            {
              usuario(func: eq(user_id, "__USER__")) @filter(type(Usuario)) {
                user_id
                email
                creo {
                  ticket_id
                  titulo
                  estado
                  fecha_creacion
                }
              }
            }
            """
            query = base_query.replace("__USER__", user_id)
        else:
            # Todos los usuarios con tickets, ordenados por user_id
            query = """
            {
              usuario(func: type(Usuario), orderasc: user_id) {
                user_id
                email
                creo {
                  ticket_id
                  titulo
                  estado
                  fecha_creacion
                }
              }
            }
            """

        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)

        usuarios = data.get("usuario", [])

        print("\n=== Relación usuario–ticket (creó) ===")
        if not usuarios:
            print("No se encontraron usuarios en Dgraph.")
            return

        for u in usuarios:
            print(f"\nUsuario {u.get('user_id')} | {u.get('email')}")
            tickets = u.get("creo", [])
            if not tickets:
                print("  (Sin tickets creados)")
                continue
            for t in tickets:
                print(
                    f"  - {t.get('ticket_id')} | {t.get('titulo')} | "
                    f"estado: {t.get('estado')} | fecha: {t.get('fecha_creacion')}"
                )
    finally:
        close_client_stub(stub)
   
# 2) Historial de interacciones usuario - instalacion
#    Requerimiento: usuarios que reportan frecuentemente en las mismas instalaciones.
def historial_usuario_instalacion():
    raw_id = input('user_id (ej. "U-001"): ').strip()
    user_id = _normalizar_user_id(raw_id)
    if not user_id:
        print("Se requiere un user_id.")
        return

    client, stub = get_client()
    try:
        base_query = """
        {
          usuario(func: eq(user_id, "__USER__")) @filter(type(Usuario)) {
            user_id
            creo @filter(has(afecta)) {
              ticket_id
              titulo
              afecta {
                nombre
                instal_id
              }
            }
          }
        }
        """
        query = base_query.replace("__USER__", user_id)

        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        usuarios = data.get("usuario", [])

        print("\n=== Historial de interacciones usuario–instalación ===")
        if not usuarios:
            print("No se encontró el usuario en Dgraph.")
            return

        u = usuarios[0]
        print(f"Usuario: {u.get('user_id')}")
        tickets = u.get("creo", [])
        if not tickets:
            print("  (El usuario no tiene tickets que afecten instalaciones)")
            return

        for t in tickets:
            afecta = t.get("afecta") or {}
            print(
                f"  - Ticket {t.get('ticket_id')} | {t.get('titulo')} -> "
                f"Instalación: {afecta.get('nombre')} ({afecta.get('instal_id')})"
            )
    finally:
        close_client_stub(stub)

# 3) Detectar tickets relacionados por contexto
#    Requerimiento: problemas que se relacionan entre si.
def tickets_relacionados_por_contexto():
    client, stub = get_client()
    try:
        query = """
        {
          tickets(func: has(ticket_id)) @filter(type(Ticket)) {
            ticket_id
            titulo
            categoria
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        tickets = data.get("tickets", [])

        print("\n=== Tickets relacionados por contexto (categoría) ===")
        if not tickets:
            print("No se encontraron tickets.")
            return

        # Agrupar por categoria
        por_categoria = {}
        for t in tickets:
            cat = t.get("categoria") or "sin_categoria"
            por_categoria.setdefault(cat, []).append(t)

        hay_alguno = False
        for cat, lista in por_categoria.items():
            if len(lista) < 2:
                # Mostramos solo categorías con 2+ tickets
                continue
            hay_alguno = True
            print(f"\nCategoría: {cat}")
            for t in lista:
                print(f"  - {t.get('ticket_id')} | {t.get('titulo')}")

        if not hay_alguno:
            print("No hay categorías con más de un ticket (no hay contexto compartido).")
    finally:
        close_client_stub(stub)

# 4) Historial relacional del ticket
#    Requerimiento: ver todo el contexto del ticket
#    (quién lo reportó, instalación, categoría, tipo, agente asignado).
def historial_relacional_ticket():
    ticket_id = input('ticket_id (ej. "TK-3001"): ').strip()
    if not ticket_id:
        print("Se requiere un ticket_id.")
        return

    client, stub = get_client()
    try:
        base_query = """
        {
          ticket(func: eq(ticket_id, "__TICKET__")) @filter(type(Ticket)) {
            ticket_id
            titulo
            estado
            prioridad
            fecha_creacion

            ~creo {
              user_id
              nombre
              email
            }

            afecta {
              instal_id
              nombre
            }

            pertenece_a_categoria {
              nombre
              descripcion
            }

            tipo {
              tipo_id
              descripcion
            }

            asignado_a {
              agente_id
              nombre
              email
            }
          }
        }
        """
        query = base_query.replace("__TICKET__", ticket_id)

        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        tickets = data.get("ticket", [])

        print("\n=== Historial relacional del ticket ===")
        if not tickets:
            print("No se encontró el ticket en Dgraph.")
            return

        t = tickets[0]

        print(f"\nTicket: {t.get('ticket_id')} | {t.get('titulo')}")
        print(f"  Estado: {t.get('estado')} | Prioridad: {t.get('prioridad')}")
        print(f"  Fecha de creación: {t.get('fecha_creacion')}")

        # Quién lo reportó (~creo)
        creador = t.get("~creo", [])
        if creador:
            u = creador[0]
            print(
                f"\n  Reportado por: {u.get('nombre')} "
                f"({u.get('user_id')} - {u.get('email')})"
            )
        else:
            print("\n  Reportado por: (no encontrado)")

        # Instalación
        inst = t.get("afecta")
        if inst:
            print(
                f"  Instalación afectada: {inst.get('nombre')} "
                f"({inst.get('instal_id')})"
            )
        else:
            print("  Instalación afectada: (no registrada)")

        # Categoría
        cat = t.get("pertenece_a_categoria")
        if cat:
            print(
                f"  Categoría: {cat.get('nombre')} - "
                f"{cat.get('descripcion')}"
            )
        else:
            print("  Categoría: (no registrada)")

        # Tipo de problema
        tipo = t.get("tipo")
        if tipo:
            print(
                f"  Tipo de problema: {tipo.get('descripcion')} "
                f"({tipo.get('tipo_id')})"
            )
        else:
            print("  Tipo de problema: (no registrado)")

        # Agente asignado
        agente = t.get("asignado_a")
        if agente:
            print(
                f"  Asignado a: {agente.get('nombre')} "
                f"({agente.get('agente_id')} - {agente.get('email')})"
            )
        else:
            print("  Asignado a: (ningún agente)")
    finally:
        close_client_stub(stub)

    
    

# 11) Conexión entre usuarios y horarios de reporte
#     Requerimiento: horarios más frecuentes por usuario.

def conexion_usuarios_horarios():
    client, stub = get_client()
    try:
        query = """
        {
          usuarios(func: type(Usuario)) {
            user_id
            email
            creo {
              ticket_id
              fecha_creacion
            }
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        usuarios = data.get("usuarios", [])

        print("\n=== Conexión entre usuarios y horarios de reporte ===")
        if not usuarios:
            print("No se encontraron usuarios.")
            return

        def obtener_turno(dt: datetime) -> str:
            """
            Regresa 'mañana' o 'tarde_noche' según la hora.
            En este proyecto solo manejamos dos turnos.
            """
            h = dt.hour
            if 7 <= h < 15:
                return "mañana"
            return "tarde_noche"

        for u in usuarios:
            uid = u.get("user_id")
            email = u.get("email")
            tickets = u.get("creo", [])

            if not tickets:
                continue

            conteo_turnos = {"mañana": 0, "tarde_noche": 0}
            conteo_horas = [0] * 24  # 0..23

            for t in tickets:
                fecha_str = t.get("fecha_creacion")
                if not fecha_str:
                    continue
                try:
                    # Dgraph suele devolver datetime como 'YYYY-MM-DDTHH:MM:SSZ'.
                    if fecha_str.endswith("Z"):
                        fecha_str_norm = fecha_str[:-1] + "+00:00"
                    else:
                        fecha_str_norm = fecha_str
                    dt = datetime.fromisoformat(fecha_str_norm)
                except Exception:
                    # Si la fecha viene en otro formato la ignoramos
                    continue
                turno = obtener_turno(dt)
                conteo_turnos[turno] += 1

                hora = dt.hour
                if 0 <= hora < 24:
                    conteo_horas[hora] += 1

            print(f"\nUsuario {uid} ({email})")
            print(
                f"  Mañana: {conteo_turnos['mañana']} | "
                f"Tarde_noche: {conteo_turnos['tarde_noche']}"
            )

            # Mostrar solo las horas donde realmente hay tickets
            horas_con_tickets = [
                f"{h:02d}: {conteo_horas[h]}"
                for h in range(24)
                if conteo_horas[h] > 0
            ]
            if horas_con_tickets:
                print("  Horas con tickets (00-23):")
                print("   " + " | ".join(horas_con_tickets))
    finally:
        close_client_stub(stub)


# Menú para probar los requerimientos

def print_menu():
    print("\n=== Menu de consultas Dgraph ===")
    print("1.  Relación usuario–ticket")
    print("2.  Historial de interacciones usuario–instalación")
    print("3.  Tickets relacionados por contexto (categoría)")
    print("4.  Historial relacional del ticket")
    print("5.  Conexión entre usuarios y horarios de reporte")
    print("0. Salir")


def main():
    while True:
        print_menu()
        try:
            op = int(input("Opción: ").strip())
        except ValueError:
            print("Opción inválida.")
            continue

        if op == 0:
            print("Saliendo de Dgraph CLI...")
            break
        elif op == 1:
            reporte_usuario_ticket()
        elif op == 2:
            historial_usuario_instalacion()
        elif op == 3:
            tickets_relacionados_por_contexto()
        elif op == 4:
            conexion_usuarios_horarios()
        elif op==5:
            historial_relacional_ticket()
        else:
            print("Opción no válida.")

   


if __name__ == "__main__":
    main()
