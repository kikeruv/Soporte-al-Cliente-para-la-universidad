# main.py

def print_menu():
    #  CASSANDRA 
    print("1. [Cassandra] Ver tickets por cliente")
    print("2. [Cassandra] Ver tickets por fecha")
    print("3. [Cassandra] Ver historial de soporte de un cliente")

    #  MONGODB 
    print("4. [MongoDB] Listar clientes registrados")
    print("5. [MongoDB] Listar tickets abiertos")
    print("6. [MongoDB] Buscar ticket por ID")

    #  DGRAPH 
    print("7. [Dgraph] Ver grafo de clientes y tickets")
    print("8. [Dgraph] Ver relaciones de un cliente")
    print("9. [Dgraph] Ver tickets asignados a un agente")

    print("\n0. Salir\n")

def main():
    print_menu()

    try:
        opcion = int(input("Seleccione una opción: "))
    except ValueError:
        print("Opción inválida. Debe ser un número.")
        return

    # Respondemos según lo que elija el usuario (pero sin lógica real)
    if opcion == 1:
        print("\nVerás los tickets por cliente (Cassandra).")
    elif opcion == 2:
        print("\ncVerás los tickets por fecha (Cassandra).")
    elif opcion == 3:
        print("\nVerás el historial de soporte (Cassandra).")

    elif opcion == 4:
        print("\nListar clientes registrados (MongoDB).")
    elif opcion == 5:
        print("\nListar tickets abiertos (MongoDB).")
    elif opcion == 6:
        print("\nBuscar ticket por ID (MongoDB).")

    elif opcion == 7:
        print("\nVer grafo de clientes y tickets (Dgraph).")
    elif opcion == 8:
        print("\nVer relaciones de un cliente (Dgraph).")
    elif opcion == 9:
        print("\nVer tickets asignados a un agente (Dgraph).")

    elif opcion == 0:
        print("\nSaliendo del sistema...")
    else:
        print("\nOpción no válida.")


if __name__ == "__main__":
    main()
