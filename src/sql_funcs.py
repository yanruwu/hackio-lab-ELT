import pandas as pd
import psycopg2

def establecer_conn(database_name, postgres_pass, usuario, host="localhost"):
    """
    Establece una conexión a una base de datos de PostgreSQL.

    Params:
        - database_name (str): El nombre de la base de datos a la que conectarse.
        - postgres_pass (str): La contraseña del usuario de PostgreSQL.
        - usuario (str): El nombre del usuario de PostgreSQL.
        - host (str, opcional): La dirección del servidor PostgreSQL. Por defecto es "localhost".

    Returns:
        psycopg2.extensions.connection: La conexión establecida a la base de datos PostgreSQL.

    """

    # Crear la conexión a la base de datos PostgreSQL
    conn = psycopg2.connect(
        host=host,
        user=usuario,
        password=postgres_pass,
        database=database_name
    )

    # Establecer la conexión en modo autocommit
    conn.autocommit = True # No hace necesario el uso del commit al final de cada sentencia de insert, delete, etc.
    
    return conn


def crear_db(database_name):
    """Crea una base de datos en PostgreSQL si no existe.

    Args:
        database_name (str): El nombre de la base de datos a crear.

    Returns:
        None. Muestra un mensaje indicando si la base de datos fue creada o si ya existía.

    """
    # conexion a postgres
    conn = establecer_conn("postgres", "admin", "postgres") # Nos conectamos a la base de datos de postgres por defecto para poder crear la nueva base de datos
    
    # creamos un cursor con la conexion que acabamos de crear
    cur = conn.cursor()
    
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
    
    # Almacenamos en una variable el resultado del fetchone. Si existe tendrá una fila sino será None
    bbdd_existe = cur.fetchone()
    
    # Si bbdd_existe es None, creamos la base de datos
    if not bbdd_existe:
        cur.execute(f"CREATE DATABASE {database_name};")
        print(f"Base de datos {database_name} creada con éxito")
    else:
        print(f"La base de datos ya existe")
        
    # Cerramos el cursor y la conexion
    cur.close()
    conn.close()


def query_fetch(connection, query_text):
    """Ejecuta una consulta SQL y retorna todos los resultados.

    Args:
        connection (psycopg2.connection): La conexión a la base de datos.
        query_text (str): La consulta SQL a ejecutar.

    Returns:
        list: Los resultados de la consulta en formato de lista de tuplas.

    """
    cursor = connection.cursor()
    cursor.execute(query_text)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result


def query_commit(connection, query_text, *valores):
    """Ejecuta una consulta SQL de modificación y confirma los cambios en la base de datos.

    Args:
        connection (psycopg2.connection): La conexión a la base de datos.
        query_text (str): La consulta SQL a ejecutar.
        *valores: Valores opcionales a pasar a la consulta SQL.

    Returns:
        None. Imprime un mensaje indicando que la operación se completó.

    """
    cursor = connection.cursor()
    cursor.execute(query_text, *valores)
    connection.commit()
    cursor.close()
    connection.close()
    return print("Done!")


def query_commit_many(connection, query_text, *valores):
    """Ejecuta una consulta SQL de modificación para múltiples registros y confirma los cambios.

    Args:
        connection (psycopg2.connection): La conexión a la base de datos.
        query_text (str): La consulta SQL a ejecutar.
        *valores: Lista de tuplas con los valores para cada ejecución de la consulta SQL.

    Returns:
        None. Imprime un mensaje indicando que la operación se completó.

    """
    cursor = connection.cursor()
    cursor.executemany(query_text, *valores)
    connection.commit()
    cursor.close()
    connection.close()
    return print("Done!")


def mapeo(df, columna):
    """Asigna identificadores numéricos únicos a cada valor distinto en una columna de un DataFrame.

    Args:
        df (pandas.DataFrame): El DataFrame a procesar.
        columna (str): El nombre de la columna a mapear.

    Returns:
        dict: Un diccionario que mapea los valores originales a sus nuevos identificadores.

    """
    mapper = {v: i for i, v in enumerate(df[columna].unique(), start=1)}
    df[columna] = df[columna].map(mapper)
    return mapper
