import sys
import os
# Permite importar 'connect.py' desde la carpeta raíz del proyecto.
# Sin esto, Python no puede acceder a módulos fuera de /Dgraph/.
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
    user_id = input('user_id (ej. "U-001", vacío = todos): ').strip()
    user_id = _normalizar_user_id(user_id)

    client, stub = get_client()
    try:
        if user_id:
            base_query = """
            {
              usuario(func: eq(user_id, "__USER__")) @filter(type(Usuario)) {
                nombre
                email
                user_id
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
            query = """
            {
              usuario(func: type(Usuario), orderasc: user_id) {
                nombre
                email
                user_id
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
            # Mostramos solo una vez la palabra 'Usuario' para evitar duplicados visuales.
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
    user_id = input('user_id (ej. "U-001"): ').strip()
    user_id = _normalizar_user_id(user_id)
    if not user_id:
        print("Se requiere un user_id.")
        return

    client, stub = get_client()
    try:
        base_query = """
        {
          usuario(func: eq(user_id, "__USER__")) @filter(type(Usuario)) {
            nombre
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
        print(f"Usuario: {u.get('user_id')} | {u.get('nombre')}")
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

    
    

# 11) Conexión entre usuarios y horarios de reporte
#     Requerimiento: horarios más frecuentes por usuario.

def conexion_usuarios_horarios():
    client, stub = get_client()
    try:
        query = """
        {
          usuarios(func: has(user_id)) @filter(type(Usuario)) {
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
            h = dt.hour
            if 7 <= h < 15:
                return "mañana"
            if 15 <= h < 22:
                return "tarde"
            return "noche"

        for u in usuarios:
            uid = u.get("user_id")
            email = u.get("email")
            tickets = u.get("creo", [])

            if not tickets:
                continue

            conteo_turnos = {"mañana": 0, "tarde": 0, "noche": 0}

            for t in tickets:
                fecha_str = t.get("fecha_creacion")
                if not fecha_str:
                    continue
                try:
                    dt = datetime.fromisoformat(fecha_str)
                except Exception:
                    # Si la fecha viene en otro formato la ignoramos
                    continue
                turno = obtener_turno(dt)
                conteo_turnos[turno] += 1

            print(f"\nUsuario {uid} ({email})")
            print(
                f"  Mañana: {conteo_turnos['mañana']} | "
                f"Tarde: {conteo_turnos['tarde']} | "
                f"Noche: {conteo_turnos['noche']}"
            )
    finally:
        close_client_stub(stub)
   
   
#Menu para probar los requerimientos

def print_menu():
    print("\n=== Menu de consultas Dgraph ===")
    print("1.  Relación usuario–ticket")
    print("2.  Historial de interacciones usuario–instalación")
    print("3.  Tickets relacionados por contexto (categoría)")
    print("4.  Conexión entre usuarios y horarios de reporte")
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
        else:
            print("Opción no válida.")

   


if __name__ == "__main__":
    main()
