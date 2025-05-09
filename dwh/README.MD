# Script para Carga Dinámica de Datos Parquet desde S3 a Redshift

Este script de Python automatiza la carga de archivos Parquet desde Amazon S3 a una base de datos Amazon Redshift. La ruta de los archivos en S3 se construye dinámicamente utilizando la fecha actual de ejecución (año, mes y día). El script está diseñado para cargar datos en tres tablas específicas de Redshift: `co_clientes`, `co_proveedores`, y `co_transacciones`.

## Prerrequisitos

Antes de ejecutar el script, asegúrate de tener lo siguiente configurado:

* **Python 3.6 o superior** instalado en tu sistema.
* Las siguientes librerías de Python instaladas:
    ```bash
    pip install python-dotenv sqlalchemy psycopg2-binary
    ```
* Un archivo `.env` en el mismo directorio del script con las siguientes variables de entorno configuradas:
    ```
    REDSHIFT_HOST="your_redshift_host"
    REDSHIFT_PORT="your_redshift_port"
    REDSHIFT_DBNAME="your_redshift_dbname"
    REDSHIFT_USER="your_redshift_user"
    REDSHIFT_PASSWORD="your_redshift_password"
    S3_BUCKET_NAME="your_s3_bucket_name"
    REDSHIFT_SCHEMA="your_redshift_schema" # Opcional, por defecto es 'public'
    ```
    Reemplaza los valores entre comillas dobles con la información real de tu entorno.
* Un módulo `extra/utils.py` (en una subcarpeta llamada `extra`) que contenga las funciones:
    * `create_redshift_engine(host, port, dbname, user, password)`: para establecer la conexión a la base de datos Redshift utilizando SQLAlchemy.
    * `load_parquet_from_s3_to_redshift(engine, s3_path, redshift_table, redshift_schema)`: para cargar los archivos Parquet desde la ruta de S3 especificada a la tabla de Redshift utilizando la función `COPY`.
* Permisos de AWS configurados correctamente para que la instancia de Redshift pueda leer los archivos desde el bucket de S3 especificado. Esto puede configurarse mediante roles de IAM asociados al clúster de Redshift (recomendado) o mediante el uso de claves de acceso en la cadena de conexión (menos seguro).

## Estructura de Carpetas

Asegúrate de tener la siguiente estructura de carpetas:
```
dwh/
├── load_redshift.py
├── extra/
│   └── utils.py
└── .env
```
## Funcionamiento del Script

1.  **Carga de Variables de Entorno:** El script utiliza la librería `python-dotenv` para cargar las variables de configuración desde el archivo `.env`.
2.  **Definición de Tablas y Prefijos S3:** Se define un diccionario `tables_config` que mapea los nombres de las tablas de Redshift con sus correspondientes prefijos en S3. Los prefijos utilizan placeholders para el año, mes y día.
3.  **Obtención de la Fecha Actual:** Se utiliza la librería `datetime` para obtener la fecha actual de ejecución. El año, mes (con dos dígitos) y día (con dos dígitos) se extraen de esta fecha.
4.  **Construcción Dinámica de Rutas S3:** Para cada tabla definida en `tables_config`, se construye dinámicamente la ruta completa en S3 utilizando el `s3_bucket_name`, el prefijo específico de la tabla (formateado con la fecha actual), y un comodín `*` para incluir todos los archivos Parquet dentro de ese directorio.
5.  **Conexión a Redshift:** Se llama a la función `create_redshift_engine` del módulo `extra.utils` para establecer una conexión a la base de datos Redshift utilizando los parámetros cargados desde el `.env`.
6.  **Carga de Datos:** Para cada tabla, se llama a la función `load_parquet_from_s3_to_redshift` del módulo `extra.utils`, pasando el engine de Redshift, la ruta de S3 construida dinámicamente, el nombre de la tabla y el esquema.
7.  **Logging:** El script utiliza la librería `logging` para proporcionar información sobre el proceso de conexión y carga.

## Ejecución del Script

Para ejecutar el script, simplemente navega al directorio donde se encuentra `load_redshift.py` en tu terminal y ejecuta:

```bash
python load_redshift.py