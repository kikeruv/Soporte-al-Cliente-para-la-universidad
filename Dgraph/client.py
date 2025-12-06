import sys
import os
# Permite importar 'connect.py' desde la carpeta raíz del proyecto.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from itertools import combinations
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


## 1) Relación usuario–ticket
#    Requerimiento: saber quién creó cada ticket y listar todos los tickets de un usuario.
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


# 2) Detección de Tickets Duplicados
#    Requerimiento: identificar automáticamente tickets similares usando PalabraClave (contiene).
def deteccion_tickets_duplicados():
    client, stub = get_client()
    try:
        # Traemos tickets con sus palabras clave (contiene -> PalabraClave.palabra)
        query = """
        {
          tickets(func: type(Ticket)) @filter(has(contiene)) {
            ticket_id
            titulo
            contiene {
              palabra
            }
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        tickets = data.get("tickets", [])

        print("\n=== Detección de tickets duplicados (por palabras clave) ===")

        if not tickets:
            print("No se encontraron tickets con palabras clave.")
            return

        # mapa_palabra -> lista de tickets que tienen esa palabra
        palabra_a_tickets = {}
        for t in tickets:
            tid = t.get("ticket_id")
            titulo = t.get("titulo")
            contiene = t.get("contiene") or []
            for p in contiene:
                w = p.get("palabra")
                if not w:
                    continue
                palabra_a_tickets.setdefault(w, []).append(
                    {"ticket_id": tid, "titulo": titulo}
                )

        # pares[(id1,id2)] = {"count": n_palabras_en_comun, "tickets": (t1, t2), "palabras": set()}
        pares = {}

        for palabra, lista_tickets in palabra_a_tickets.items():
            if len(lista_tickets) < 2:
                continue
            for t1, t2 in combinations(lista_tickets, 2):
                id1 = t1.get("ticket_id")
                id2 = t2.get("ticket_id")
                if not id1 or not id2 or id1 == id2:
                    continue
                key = tuple(sorted([id1, id2]))
                if key not in pares:
                    pares[key] = {
                        "count": 0,
                        "tickets": (t1, t2),
                        "palabras": set(),
                    }
                pares[key]["count"] += 1
                pares[key]["palabras"].add(palabra)

        if not pares:
            print("No se encontraron tickets potencialmente duplicados.")
            return

        # Ordenar por número de palabras en común (mayor a menor)
        pares_ordenados = sorted(
            pares.items(), key=lambda kv: kv[1]["count"], reverse=True
        )

        for (id1, id2), info in pares_ordenados:
            t1, t2 = info["tickets"]
            palabras_comunes = ", ".join(sorted(info["palabras"]))
            print(
                f"\nTickets potencialmente duplicados "
                f"(comparten {info['count']} palabras clave):"
            )
            print(f"  - {id1} | {t1.get('titulo')}")
            print(f"  - {id2} | {t2.get('titulo')}")
            print(f"    Palabras clave comunes: {palabras_comunes}")

    finally:
        close_client_stub(stub)


# 3) Historial de interacciones usuario - instalación
#    Requerimiento: rastrear qué usuarios reportan en las mismas instalaciones.
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


# 4) Historial relacional del ticket
#    Requerimiento: ver todo el contexto del ticket.
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


# 5) Tickets relacionados por contexto (categoría)
#    Requerimiento: identificar tickets que comparten el mismo contexto.
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


# 6) Usuario con mayor diversidad de reportes
#    Requerimiento: identificar usuarios con mayor diversidad (categorías / instalaciones).
def usuario_mayor_diversidad():
    client, stub = get_client()
    try:
        query = """
        {
          usuarios(func: type(Usuario)) {
            user_id
            nombre
            email
            creo {
              ticket_id
              pertenece_a_categoria {
                nombre
              }
              afecta {
                instal_id
                nombre
              }
            }
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        usuarios = data.get("usuarios", [])

        print("\n=== Usuario con mayor diversidad de reportes ===")

        if not usuarios:
            print("No se encontraron usuarios.")
            return

        resumen = []

        for u in usuarios:
            user_id = u.get("user_id")
            nombre = u.get("nombre")
            email = u.get("email")
            tickets = u.get("creo", []) or []

            categorias = set()
            instalaciones = set()

            for t in tickets:
                cat = t.get("pertenece_a_categoria")
                if isinstance(cat, dict):
                    nom_cat = cat.get("nombre")
                    if nom_cat:
                        categorias.add(nom_cat)

                inst = t.get("afecta")
                if isinstance(inst, dict):
                    inst_id = inst.get("instal_id") or inst.get("nombre")
                    if inst_id:
                        instalaciones.add(inst_id)

            diversidad = len(categorias) + len(instalaciones)
            resumen.append(
                {
                    "user_id": user_id,
                    "nombre": nombre,
                    "email": email,
                    "num_tickets": len(tickets),
                    "num_categorias": len(categorias),
                    "num_instalaciones": len(instalaciones),
                    "diversidad": diversidad,
                }
            )

        # Ordenar por diversidad (desc), luego por número de tickets
        resumen.sort(
            key=lambda r: (r["diversidad"], r["num_tickets"]), reverse=True
        )

        if not resumen:
            print("No hay datos de tickets para calcular diversidad.")
            return

        for r in resumen:
            print(
                f"\nUsuario {r['user_id']} | {r['nombre']} | {r['email']}"
                f"\n  Tickets: {r['num_tickets']}"
                f"\n  Categorías distintas: {r['num_categorias']}"
                f"\n  Instalaciones distintas: {r['num_instalaciones']}"
                f"\n  Puntaje de diversidad: {r['diversidad']}"
            )

    finally:
        close_client_stub(stub)


# 7) Detección de Problemas Recurrentes
#    Requerimiento: problemas que se repiten en ciertos periodos temporales.
def deteccion_problemas_recurrentes():
    client, stub = get_client()
    try:
        query = """
        {
          tickets(func: type(Ticket)) @filter(has(ocurre_en) AND has(tipo)) {
            ticket_id
            titulo
            tipo {
              tipo_id
              descripcion
            }
            ocurre_en {
              periodo_id
              descripcion
            }
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        tickets = data.get("tickets", [])

        print("\n=== Detección de problemas recurrentes (por tipo y periodo) ===")

        if not tickets:
            print("No se encontraron tickets con tipo y periodo.")
            return

        # clave: (tipo_desc, periodo_desc) -> lista de tickets
        agrupado = {}

        for t in tickets:
            tipo = t.get("tipo") or {}
            periodo = t.get("ocurre_en") or {}

            tipo_desc = tipo.get("descripcion") or "Tipo desconocido"
            periodo_desc = periodo.get("descripcion") or "Periodo desconocido"

            key = (tipo_desc, periodo_desc)
            agrupado.setdefault(key, []).append(t)

        hay_recurrentes = False
        for (tipo_desc, periodo_desc), lista in agrupado.items():
            if len(lista) < 2:
                continue  # solo mostramos problemas que se repiten

            hay_recurrentes = True
            print(
                f"\nTipo de problema: {tipo_desc} "
                f" | Periodo: {periodo_desc} "
                f" | Tickets: {len(lista)}"
            )
            for t in lista:
                print(f"  - {t.get('ticket_id')} | {t.get('titulo')}")

        if not hay_recurrentes:
            print("No se detectaron problemas recurrentes con más de un ticket.")
    finally:
        close_client_stub(stub)


# 8) Ruta de Atención de un Ticket
#    Requerimiento: flujo desde creación hasta escalamiento.
def ruta_atencion_ticket():
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

            asignado_a {
              agente_id
              nombre
              email
            }

            escalado_a {
              agente_id
              nombre
              email
            }

            tipo {
              descripcion
            }

            pertenece_a_categoria {
              nombre
            }
          }
        }
        """
        query = base_query.replace("__TICKET__", ticket_id)

        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        tickets = data.get("ticket", [])

        print("\n=== Ruta de atención del ticket ===")
        if not tickets:
            print("No se encontró el ticket en Dgraph.")
            return

        t = tickets[0]
        print(f"\nTicket: {t.get('ticket_id')} | {t.get('titulo')}")
        print(f"  Estado: {t.get('estado')} | Prioridad: {t.get('prioridad')}")
        print(f"  Fecha de creación: {t.get('fecha_creacion')}")

        # Usuario creador
        creadores = t.get("~creo", []) or []
        if creadores:
            u = creadores[0]
            print(
                f"\n  Creado por: {u.get('nombre')} "
                f"({u.get('user_id')} - {u.get('email')})"
            )
        else:
            print("\n  Creado por: (no registrado)")

        # Agente(s) asignado(s)
        asignado = t.get("asignado_a")
        if isinstance(asignado, list):
            asignados = asignado
        elif asignado:
            asignados = [asignado]
        else:
            asignados = []

        if asignados:
            print("\n  Agente(s) asignado(s):")
            for a in asignados:
                print(
                    f"    - {a.get('nombre')} "
                    f"({a.get('agente_id')} - {a.get('email')})"
                )
        else:
            print("\n  Agente asignado: (ninguno)")

        # Agente(s) a los que se ha escalado
        escalados = t.get("escalado_a")
        if isinstance(escalados, list):
            lista_escalados = escalados
        elif escalados:
            lista_escalados = [escalados]
        else:
            lista_escalados = []

        if lista_escalados:
            print("\n  Escalado a:")
            for a in lista_escalados:
                print(
                    f"    -> {a.get('nombre')} "
                    f"({a.get('agente_id')} - {a.get('email')})"
                )
        else:
            print("\n  Escalado a: (no hubo escalamiento)")

        # Contexto general
        tipo = t.get("tipo") or {}
        categoria = t.get("pertenece_a_categoria") or {}
        if tipo.get("descripcion") or categoria.get("nombre"):
            print("\n  Contexto:")
            if tipo.get("descripcion"):
                print(f"    Tipo de problema: {tipo.get('descripcion')}")
            if categoria.get("nombre"):
                print(f"    Categoría: {categoria.get('nombre')}")
    finally:
        close_client_stub(stub)


# 9) Red de Tickets Escalados
#    Requerimiento: visualizar tickets que han sido escalados entre agentes.
def red_tickets_escalados():
    client, stub = get_client()
    try:
        query = """
        {
          tickets_escalados(func: has(escalado_a)) @filter(type(Ticket)) {
            ticket_id
            titulo

            asignado_a {
              agente_id
              nombre
              email
            }

            escalado_a {
              agente_id
              nombre
              email
            }
          }
        }
        """
        txn = client.txn(read_only=True)
        res = txn.query(query)
        data = json.loads(res.json)
        tickets = data.get("tickets_escalados", [])

        print("\n=== Red de Tickets Escalados ===")

        if not tickets:
            print("No se encontraron tickets escalados.")
            return

        for t in tickets:
            print(f"\nTicket {t.get('ticket_id')} | {t.get('titulo')}")

            asignados = t.get("asignado_a")
            if isinstance(asignados, list):
                lista_asignados = asignados
            elif asignados:
                lista_asignados = [asignados]
            else:
                lista_asignados = []

            escalados = t.get("escalado_a")
            if isinstance(escalados, list):
                lista_escalados = escalados
            elif escalados:
                lista_escalados = [escalados]
            else:
                lista_escalados = []

            if lista_asignados:
                print("  Asignado a:")
                for a in lista_asignados:
                    print(
                        f"    - {a.get('nombre')} "
                        f"({a.get('agente_id')} - {a.get('email')})"
                    )
            else:
                print("  Asignado a: (ningún agente)")

            if lista_escalados:
                print("  Escalado a:")
                for a in lista_escalados:
                    print(
                        f"    -> {a.get('nombre')} "
                        f"({a.get('agente_id')} - {a.get('email')})"
                    )
            else:
                print("  Escalado a: (no hubo escalamiento)")

    finally:
        close_client_stub(stub)


# 10) Conexión entre usuarios y horarios de reporte
#     Requerimiento: ver horarios más frecuentes en que cada usuario genera tickets.
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
            tickets = u.get("creo", []) or []

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
    print("2.  Detección de tickets duplicados")
    print("3.  Historial de interacciones usuario–instalación")
    print("4.  Historial relacional del ticket")
    print("5.  Tickets relacionados por contexto (categoría)")
    print("6.  Usuario con mayor diversidad de reportes")
    print("7.  Detección de problemas recurrentes")
    print("8.  Ruta de atención de un ticket")
    print("9.  Red de tickets escalados")
    print("10. Conexión entre usuarios y horarios de reporte")
    print("0.  Salir")


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
            deteccion_tickets_duplicados()
        elif op == 3:
            historial_usuario_instalacion()
        elif op == 4:
            historial_relacional_ticket()
        elif op == 5:
            tickets_relacionados_por_contexto()
        elif op == 6:
            usuario_mayor_diversidad()
        elif op == 7:
            deteccion_problemas_recurrentes()
        elif op == 8:
            ruta_atencion_ticket()
        elif op == 9:
            red_tickets_escalados()
        elif op == 10:
            conexion_usuarios_horarios()
        else:
            print("Opción no válida.")


if __name__ == "__main__":
    main()