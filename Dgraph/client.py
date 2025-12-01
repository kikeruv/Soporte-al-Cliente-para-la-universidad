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


# 1) Relacion usuario-ticket
#    Requerimiento: saber quien creó cada ticket y listar todos los tickets de un usuario.
def reporte_usuario_ticket():
    client, stub = get_client()
    try:
        query = """
        {
          usuarios(func: has(user_id)) {
            user_id
            email
            rol
            tickets: ~creado_por {
              ticket_id
              titulo
              estado
              categoria
            }
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        usuarios = data.get("usuarios", [])

        print("\n=== Relacion usuario - tickets creados ===")
        if not usuarios:
            print("No se encontraron usuarios en Dgraph.")
            return

        for u in usuarios:
            print(f"\nUsuario: {u.get('user_id')} | {u.get('email')} | rol: {u.get('rol')}")
            tickets = u.get("tickets", [])
            if not tickets:
                print("  (Sin tickets creados)")
                continue
            for t in tickets:
                print(
                    f"  - {t.get('ticket_id')} | {t.get('titulo')} | "
                    f"estado: {t.get('estado')} | categoria: {t.get('categoria')}"
                )
    finally:
        close_client_stub(stub)


# 2) Deteccion de tickets duplicados 
#    Requerimiento: tickets similares por palabras clave en el titulo.
def detectar_tickets_duplicados():
    palabra = input("Palabra clave para buscar tickets similares: ").strip()
    if not palabra:
        print("No se ingreso ninguna palabra.")
        return

    client, stub = get_client()
    try:
        query = f"""
        {{
          tickets(func: anyofterms(titulo, "{palabra}")) {{
            ticket_id
            titulo
            categoria
            estado
          }}
        }}
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        tickets = data.get("tickets", [])

        print(f"\n=== Tickets que contienen la palabra '{palabra}' en el titulo ===")
        if not tickets:
            print("No se encontraron tickets.")
            return

        for t in tickets:
            print(
                f"- {t.get('ticket_id')} | {t.get('titulo')} | "
                f"categoria: {t.get('categoria')} | estado: {t.get('estado')}"
            )

        print(
            "\nNota: Tickets que comparten varias palabras clave en el titulo "
            "pueden considerarse potencialmente duplicados."
        )
    finally:
        close_client_stub(stub)


# 3) Historial de interacciones usuario - instalacion
#    Requerimiento: usuarios que reportan frecuentemente en las mismas instalaciones.
def historial_usuario_instalacion():
    client, stub = get_client()
    try:
        query = """
        {
          instalaciones(func: has(instal_id)) {
            instal_id
            instal_nombre
            tickets: ~afecta {
              ticket_id
              titulo
              creado_por {
                user_id
                email
              }
            }
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        instalaciones = data.get("instalaciones", [])

        print("\n=== Historial de interacciones usuario - instalacion ===")
        if not instalaciones:
            print("No se encontraron instalaciones.")
            return

        for inst in instalaciones:
            instal_id = inst.get("instal_id")
            nombre = inst.get("instal_nombre")
            tickets = inst.get("tickets", [])

            print(f"\nInstalacion: {instal_id} ({nombre})")

            if not tickets:
                print("  Sin tickets registrados.")
                continue

            # Contar cuantas veces cada usuario reporta en esta instalacion
            conteo_usuarios = {}
            for t in tickets:
                u = t.get("creado_por")
                if not u:
                    continue
                uid = u.get("user_id")
                email = u.get("email")
                if not uid:
                    continue
                if uid not in conteo_usuarios:
                    conteo_usuarios[uid] = {"email": email, "total": 0}
                conteo_usuarios[uid]["total"] += 1

            for uid, info in conteo_usuarios.items():
                print(
                    f"  Usuario {uid} ({info['email']}): "
                    f"{info['total']} tickets en esta instalacion"
                )
    finally:
        close_client_stub(stub)


# 4) Tickets relacionados por contexto (categoria)
#    Requerimiento: identificar tickets que comparten misma categoria/tipo.
def tickets_relacionados_por_contexto():
    client, stub = get_client()
    try:
        query = """
        {
          tickets(func: has(ticket_id)) {
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

        print("\n=== Tickets relacionados por categoria (contexto) ===")
        if not tickets:
            print("No se encontraron tickets.")
            return

        # Agrupar por categoria
        por_categoria = {}
        for t in tickets:
            cat = t.get("categoria") or "sin_categoria"
            if cat not in por_categoria:
                por_categoria[cat] = []
            por_categoria[cat].append(t)

        for cat, lista in por_categoria.items():
            if len(lista) < 2:
                # Mostramos solo categorias con 2 o mas tickets
                continue
            print(f"\nCategoria: {cat}")
            for t in lista:
                print(f"  - {t.get('ticket_id')} | {t.get('titulo')}")
    finally:
        close_client_stub(stub)


# 5) Usuario con mayor diversidad de reportes
#    Requerimiento: diversidad por categoria e instalacion.
def usuario_mayor_diversidad_reportes():
    client, stub = get_client()
    try:
        query = """
        {
          usuarios(func: has(user_id)) {
            user_id
            email
            tickets: ~creado_por {
              ticket_id
              categoria
              afecta {
                instal_id
              }
            }
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        usuarios = data.get("usuarios", [])

        print("\n=== Usuarios con mayor diversidad de reportes ===")
        if not usuarios:
            print("No se encontraron usuarios.")
            return

        resumen = []

        for u in usuarios:
            uid = u.get("user_id")
            email = u.get("email")
            tickets = u.get("tickets", [])

            categorias = set()
            instalaciones = set()

            for t in tickets:
                cat = t.get("categoria")
                if cat:
                    categorias.add(cat)

                inst = t.get("afecta")
                if inst and isinstance(inst, dict):
                    iid = inst.get("instal_id")
                    if iid:
                        instalaciones.add(iid)

            diversidad = len(categorias) + len(instalaciones)
            resumen.append(
                {
                    "user_id": uid,
                    "email": email,
                    "total_tickets": len(tickets),
                    "categorias": len(categorias),
                    "instalaciones": len(instalaciones),
                    "diversidad": diversidad,
                }
            )

        # Ordenar por diversidad (descendente)
        resumen.sort(key=lambda x: x["diversidad"], reverse=True)

        for r in resumen:
            print(
                f"\nUsuario {r['user_id']} ({r['email']})"
                f"\n  Total tickets: {r['total_tickets']}"
                f"\n  Categorias distintas: {r['categorias']}"
                f"\n  Instalaciones distintas: {r['instalaciones']}"
                f"\n  Indicador de diversidad: {r['diversidad']}"
            )
    finally:
        close_client_stub(stub)


# 6) Deteccion de problemas recurrentes por categoria y periodo
#    Requerimiento: problemas que se repiten en el tiempo.
def problemas_recurrentes():
    client, stub = get_client()
    try:
        query = """
        {
          tickets(func: has(ticket_id)) {
            ticket_id
            titulo
            categoria
            fecha_creacion
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        tickets = data.get("tickets", [])

        print("\n=== Problemas recurrentes por categoria y mes ===")
        if not tickets:
            print("No se encontraron tickets.")
            return

        # Agrupar por (categoria, periodo YYYY-MM)
        conteo = {}

        for t in tickets:
            cat = t.get("categoria") or "sin_categoria"
            fecha_str = t.get("fecha_creacion")
            if not fecha_str:
                continue
            try:
                dt = datetime.fromisoformat(fecha_str)
            except Exception:
                continue

            periodo = dt.strftime("%Y-%m")  # ejemplo: 2025-10
            clave = (cat, periodo)
            if clave not in conteo:
                conteo[clave] = 0
            conteo[clave] += 1

        # Mostrar resultados
        for (cat, periodo), total in sorted(conteo.items()):
            if total < 2:
                # Mostramos solo casos con 2 o mas tickets
                continue
            print(f"- Categoria '{cat}' en periodo {periodo}: {total} tickets")
    finally:
        close_client_stub(stub)


# 7) Conexion entre usuarios y horarios de reporte
#    Requerimiento: horarios mas frecuentes de reporte por usuario.
def horarios_reporte_por_usuario():
    client, stub = get_client()
    try:
        query = """
        {
          usuarios(func: has(user_id)) {
            user_id
            email
            tickets: ~creado_por {
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

        print("\n=== Horarios de reporte por usuario ===")
        if not usuarios:
            print("No se encontraron usuarios.")
            return

        def obtener_turno(dt: datetime) -> str:
            h = dt.hour
            if 7 <= h < 15:
                return "manana"
            if 15 <= h < 22:
                return "tarde"
            return "noche"

        for u in usuarios:
            uid = u.get("user_id")
            email = u.get("email")
            tickets = u.get("tickets", [])

            if not tickets:
                continue

            conteo_turnos = {"manana": 0, "tarde": 0, "noche": 0}

            for t in tickets:
                fecha_str = t.get("fecha_creacion")
                if not fecha_str:
                    continue
                try:
                    dt = datetime.fromisoformat(fecha_str)
                except Exception:
                    continue
                turno = obtener_turno(dt)
                conteo_turnos[turno] += 1

            print(f"\nUsuario {uid} ({email})")
            print(
                f"  Manana: {conteo_turnos['manana']} | "
                f"Tarde: {conteo_turnos['tarde']} | "
                f"Noche: {conteo_turnos['noche']}"
            )
    finally:
        close_client_stub(stub)


def print_menu():
    print("\n=== Menu de consultas Dgraph ===")
    print("1. Relacion usuario - tickets creados")
    print("2. Deteccion de tickets duplicados (por palabra en titulo)")
    print("3. Historial usuario - instalacion")
    print("4. Tickets relacionados por categoria (contexto)")
    print("5. Usuarios con mayor diversidad de reportes")
    print("6. Problemas recurrentes por categoria y mes")
    print("7. Horarios de reporte por usuario")
    print("0. Salir")


def main():
    while True:
        print_menu()
        try:
            op = int(input("Opcion: ").strip())
        except ValueError:
            print("Opcion invalida.")
            continue

        if op == 0:
            print("Saliendo de Dgraph CLI...")
            break
        elif op == 1:
            reporte_usuario_ticket()
        elif op == 2:
            detectar_tickets_duplicados()
        elif op == 3:
            historial_usuario_instalacion()
        elif op == 4:
            tickets_relacionados_por_contexto()
        elif op == 5:
            usuario_mayor_diversidad_reportes()
        elif op == 6:
            problemas_recurrentes()
        elif op == 7:
            horarios_reporte_por_usuario()
        else:
            print("Opcion no valida.")


if __name__ == "__main__":
    main()
