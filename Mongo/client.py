from connect import db
#############################
def filtrar_por_categoria():#

    tickets = list(db.tickets.find(
        {},
        {
            "_id": 0,
            "ticket_id": 1,
            "title": 1,
            "category": 1,
            "status": 1,
            "installation_id": 1,
            "created_at": 1
        }
    ))

    if not tickets:
        print("\nNo hay tickets.\n")
        return
    
    print("\nTickets por categoría")
    for t in tickets:
        print(
            f"{t['ticket_id']} | "
            f"{t['title']} | "
            f"categoria: {t['category']} | "
            f"estado: {t['status']} | "
            f"instalación: {t.get('installation_id')} | "
            f"{t.get('created_at')}"
        )

    pipeline = [
        {"$group": {"_id": "$category", "total_tickets": {"$sum": 1}}},
        {"$sort": {"total_tickets": -1}},
        {"$project": {"_id": 0, "category": "$_id", "total_tickets": 1}},
    ]

    resultados = list(db.tickets.aggregate(pipeline))

    if not resultados:
        print("\nNo hay tickets.\n")
        return

    print("\nTotal de tickets por categoría")
    for doc in resultados:
        print(f"- {doc['category']}: {doc['total_tickets']}")
######################
def resumen_estado():#

    tickets = list(db.tickets.find(
        {},
        {
            "_id": 0,
            "ticket_id": 1,
            "title": 1,
            "category": 1,
            "status": 1,
            "installation_id": 1,
            "created_at": 1
        }
    ))

    if not tickets:
        print("\nNo hay tickets.\n")
        return
    
    print("\nTickets por estado")
    for t in tickets:
        print(
            f"{t['ticket_id']} | "
            f"{t['title']} | "
            f"categoria: {t['category']} | "
            f"estado: {t['status']} | "
            f"instalación: {t.get('installation_id')} | "
            f"{t.get('created_at')}"
        )

    pipeline = [
        {"$group": {"_id": "$status", "total_tickets": {"$sum": 1}}},
        {"$sort": {"total_tickets": -1}},
        {"$project": {"_id": 0, "status": "$_id", "total_tickets": 1}},
    ]

    resultados = list(db.tickets.aggregate(pipeline))

    if not resultados:
        print("\nNo hay tickets.\n")
        return

    print("\nTotal de tickets por estado")
    for doc in resultados:
        print(f"- {doc['status']}: {doc['total_tickets']}")
############################
def buscar_titulos_falla():#
    # Crear índice si no existe
    db.tickets.create_index("title")

    filtro = { "title": { "$regex": r"^(Falla|Daño)", "$options": "i" } }  # Usamos regex

    proyeccion = {
        "_id": 0,
        "ticket_id": 1,
        "title": 1,
        "category": 1,
        "status": 1,
        "installation_id": 1,
        "created_at": 1
    }

    resultados = list(db.tickets.find(filtro, proyeccion))

    if not resultados:
        print("\nNo se encontraron tickets.\n")
        return

    print("\nTickets cuyo título inicia con 'Falla' o 'Daño'")
    for t in resultados:
        print(f"- {t['ticket_id']} | {t['title']} | {t['category']} | {t['status']} | {t.get('installation_id')} | {t.get('created_at')}")
################################
def lugares_con_mas_perdidas():#

    pipeline = [
        {"$match": {"category": "cosas_perdidas"}},
        {"$group": {"_id": "$place_name", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$project": {"_id": 0, "place_name": "$_id", "total": 1}},
    ]

    resultados = list(db.tickets.aggregate(pipeline))

    if not resultados:
        print("\nNo hay tickets.\n")
        return

    print("\nLugares con más reportes de pérdidas")
    for doc in resultados:
        print(f"- {doc['place_name']}: {doc['total']}")
#########################################
def instalaciones_con_mas_incidencias():#

    pipeline = [
        {"$group": {"_id": "$installation_id", "total_tickets": {"$sum": 1}}},
        {"$sort": {"total_tickets": -1}},
        {"$project": {"_id": 0, "installation_id": "$_id", "total_tickets": 1}},
    ]

    resultados = list(db.tickets.aggregate(pipeline))

    if not resultados:
        print("\nNo hay tickets.\n")
        return

    print("\nInstalaciones con más incidencias")
    for doc in resultados:
        print(f"- {doc['installation_id']}: {doc['total_tickets']}")
########################
def buscar_por_texto():#

    # Crear índice de texto si no existe
    db.tickets.create_index([
        ("title", "text"),
        ("description", "text"),
        ("object_name", "text")
    ])

    palabras = input("Escribe palabras clave para buscar: ")

    filtro = { "$text": { "$search": palabras } }

    proyeccion = {
        "_id": 0,
        "ticket_id": 1,
        "title": 1,
        "object_name": 1
    }

    resultados = list(db.tickets.find(filtro, proyeccion))

    if not resultados:
        print("\nNo se encontraron tickets.\n")
        return
    
    print("\nTickets encontrados por texto")
    for t in resultados:
        print(f"{t['ticket_id']} | {t['title']} | objeto: {t.get('object_name')} | categoría: {t.get('category')} | instalación: {t.get('installation_id')} | {t.get('created_at')}")

    print(f"Total de coincidencias: {len(resultados)}")
################################
def resumen_objetos_perdidos():#

    tickets = list(db.tickets.find(
        {"category": "cosas_perdidas"},
        {
            "_id": 0,
            "ticket_id": 1,
            "title": 1,
            "object_name": 1,
            "lost_status": 1,
            "installation_id": 1,
            "created_at": 1
        }
    ))

    if not tickets:
        print("\nNo hay objetos perdidos.\n")
        return
    
    print("\nObjetos perdidos registrados")
    for t in tickets:
        print(
            f"{t['ticket_id']} | {t['title']} | "
            f"objeto: {t.get('object_name')} | "
            f"estado: {t.get('lost_status')} | "
            f"instalación: {t.get('installation_id')} | "
            f"{t.get('created_at')}"
        )

    pipeline = [
        {"$match": {"category": "cosas_perdidas"}},
        {"$group": {"_id": "$lost_status", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$project": {"_id": 0, "lost_status": "$_id", "total": 1}},
    ]

    resultados = list(db.tickets.aggregate(pipeline))

    if not resultados:
        print("\nNo hay objetos perdidos.\n")
        return

    print("\nResumen de objetos perdidos")
    for doc in resultados:
        print(f"- {doc['lost_status']}: {doc['total']}")
######################################
def tickets_cerrados_por_categoria():#

    tickets = list(db.tickets.find(
        {"status": "cerrado"},
        {
            "_id": 0,
            "ticket_id": 1,
            "title": 1,
            "category": 1,
            "status": 1,
            "installation_id": 1,
            "created_at": 1
        }
    ))

    if not tickets:
        print("\nNo hay tickets cerrados.\n")
        return

    print("\nTickets cerrados encontrados")
    for t in tickets:
        print(f"{t['ticket_id']} | {t['title']} | {t['category']} | {t['installation_id']} | {t['created_at']}")

    pipeline = [
        {"$match": {"status": "cerrado"}},
        {"$group": {"_id": "$category", "total_closed": {"$sum": 1}}},
        {"$sort": {"total_closed": -1}},
        {"$project": {"_id": 0, "category": "$_id", "total_closed": 1}},
    ]

    resultados = list(db.tickets.aggregate(pipeline))

    if not resultados:
        print("\nNo se encontraron tickets cerrados.\n")
        return

    print("\nTickets cerrados por categoría")
    for doc in resultados:
        print(f"- {doc['category']}: {doc['total_closed']}")
########################
def mostrar_usuarios():#
    usuarios = list(db.users.find(
        {},
        {
            "_id": 0, 
            "user_id": 1, 
            "expediente": 1, 
            "email": 1, "role": 1
         }
    ))

    if not usuarios:
        print("\nNo hay usuarios registrados.\n")
        return

    print("\nLista de usuarios")
    for u in usuarios:
        print(f"{u['user_id']} | {u['expediente']} | {u['email']} | {u['role']}")

def tickets_recientes_por_instalacion():
    install_id = input("installation_id: ").strip()
    if not install_id:
        print("\ninstallation_id vacío.\n")
        return

    try:
        limite = int(input("¿Cuántos tickets recientes quieres ver? ").strip() or "10")
    except ValueError:
        limite = 10

    filtro = {"installation_id": install_id}

    proyeccion = {
        "_id": 0,
        "ticket_id": 1,
        "title": 1,
        "status": 1,
        "priority": 1,
        "installation_id": 1,
        "created_at": 1,
    }

    resultados = list(
        db.tickets
          .find(filtro, proyeccion)
          .sort("created_at", -1)   # usamos el índice { created_at: -1 }
          .limit(limite)
    )

    if not resultados:
        print("\nNo hay tickets para esa instalación.\n")
        return

    print(f"\nTickets recientes para instalación '{install_id}'")
    for t in resultados:
        print(f"{t['ticket_id']} | {t['title']} | {t['status']} | {t['priority']} | {t['created_at']}")

#####################################
def distribucion_categoria_estado():#
    pipeline = [
        {"$group": {"_id": {"category": "$category","status": "$status"},
        "total_tickets": {"$sum": 1}}},
        {"$project": {"_id": 0,"category": "$_id.category","status": "$_id.status",
        "total_tickets": 1}},
        {"$sort": {"category": 1,"status": 1}}
    ]

    resultados = list(db.tickets.aggregate(pipeline))

    print("\n=== Distribución de tickets por categoría y estado ===")
    if not resultados:
        print("No hay tickets para mostrar.\n")
    else:
        for doc in resultados:
            print(
                f"category: {doc['category']} | "
                f"status: {doc['status']} | "
                f"total_tickets: {doc['total_tickets']}"
            )
