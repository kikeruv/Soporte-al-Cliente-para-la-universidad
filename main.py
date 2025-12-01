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

import populate

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

def print_menu():
    #  CASSANDRA 
    print("1. Ver tickets por cliente")
    print("2. Ver tickets por fecha")
    print("3. Ver historial de soporte de un cliente")

    #  MONGODB 
    print("4. Total de tickets por categoría")
    print("5. Total de tickets por estado")
    print("6. Títulos que empiezan con 'Falla' o 'Daño'")
    print("7. Lugares con más reportes de pérdidas")
    print("8. Instalaciones con más incidencias")
    print("9. Búsqueda por texto en tickets")
    print("10. Resumen de objetos perdidos")
    print("11. Tickets cerrados por categoría")
    print("12. Mostar todos los usuarios")

    #  DGRAPH 
    print("13. Ver grafo de clientes y tickets")
    print("14. Ver relaciones de un cliente")
    print("15. Ver tickets asignados a un agente")

    #probar conexiones
    print("10. Probar conexiones a Cassandra, Mongo y Dgraph")
    print("11. Ejecutar populate (Mongo + Cassandra + Dgraph)")

    print("\n0. Salir\n")

def main():
    while True:
        print_menu()

        try:
            opcion = int(input("Seleccione una opción: "))
        except ValueError:
            print("Opción inválida. Debe ser un número.\n")
            continue

        # CASSANDRA (aún sin implementación real)
        if opcion == 1:
            print("\nVer tickets por cliente (Cassandra).\n")

        elif opcion == 2:
            print("\nVer tickets por fecha (Cassandra).\n")

        elif opcion == 3:
            print("\nVer historial de soporte de un cliente (Cassandra).\n")

        # MONGODB
        elif opcion == 4:
            filtrar_por_categoria()

        elif opcion == 5:
            resumen_estado()

        elif opcion == 6:
            buscar_titulos_falla()

        elif opcion == 7:
            lugares_con_mas_perdidas()

        elif opcion == 8:
            instalaciones_con_mas_incidencias()

        elif opcion == 9:
            buscar_por_texto()

        elif opcion == 10:
            resumen_objetos_perdidos()

        elif opcion == 11:
            print("\nEjecutando populate de Mongo + Cassandra + Dgraph\n")
            populate.main()
            print()

        elif opcion == 0:
            print("\nSaliendo del sistema")
            break

        else:
            print("\nOpción no válida.\n")


if __name__ == "__main__":
    main()