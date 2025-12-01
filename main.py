from connect import (
    get_cassandra_session,
    create_client_stub,
    create_client,
    close_client_stub,
    db,
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
    print("4. Listar clientes registrados")
    print("5. Listar tickets abiertos")
    print("6. Buscar ticket por ID")

    #  DGRAPH 
    print("7. Ver grafo de clientes y tickets")
    print("8. Ver relaciones de un cliente")
    print("9. Ver tickets asignados a un agente")

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

        # CASSANDRA
        if opcion == 1:
            print("\n Ver tickets por cliente.\n")

        elif opcion == 2:
            print("\n Ver tickets por fecha.\n")

        elif opcion == 3:
            print("\n Ver historial de soporte de un cliente.\n")

        # MONGODB
        elif opcion == 4:
            print("\nListar clientes registrados.\n")

        elif opcion == 5:
            print("\n Listar tickets abiertos.\n")

        elif opcion == 6:
            print("\n Buscar ticket por ID.\n")

        # DGRAPH
        elif opcion == 7:
            print("\n Ver grafo de clientes y tickets.\n")

        elif opcion == 8:
            print("\n Ver relaciones de un cliente.\n")

        elif opcion == 9:
            print("\n Ver tickets asignados a un agente.\n")

        elif opcion == 10:
            test_connections()

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
