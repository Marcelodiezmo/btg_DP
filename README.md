#  Proyecto btg_DP - Pipeline de Procesamiento para Comercializadora de Energía

## Fecha de ejecución
**Mayo 2025**

---

##  Descripción general

Este proyecto implementa un **pipeline de procesamiento de datos en AWS** para una compañía comercializadora de energía. El sistema administra la información de **clientes, proveedores y transacciones** exportada desde sistemas internos en formato CSV.

Este pipeline realiza las siguientes acciones:

- Almacena los datos en un **Data Lake en S3**, con **particiones por año, mes y día**.
- Aplica **transformaciones básicas** utilizando **AWS Glue (PySpark)**.
- Guarda la información transformada en formato **Parquet** en la zona procesada.
- Registra los datos en el **Glue Data Catalog**.
- Ejecuta **consultas SQL desde Athena usando Python**.

---

##  Estructura del repositorio

```plaintext
btg_DP/
│
├── extra/                       # Funciones auxiliares para el pipeline
│   ├── __init__.py              # Inicializador del módulo extra
│   ├── load.py                  # Contiene la función save_table para guardar datos en Glue
│   ├── read.py                  # Funciones para lectura de datos
│   ├── transform_data.py        # Transformaciones simples de los datos
│   └── utils.py                 # Funciones utilitarias (como obtener fecha de ejecución)
│
├── job/
│   ├── __init__.py              # Inicializador del módulo job
│   ├── co_clientes.py           # ETL para datos de clientes
│   ├── co_proveedores.py        # ETL para datos de proveedores
│   ├── co_transacciones.py      # ETL para datos de transacciones
│   └── read_athena.py           # Lectura de datos desde Athena para el ETL
```

###  Dependencias principales

Asegúrate de tener instalado lo siguiente en tu entorno de desarrollo de AWS Glue:

- `pyspark >= 3.3.0`  
- `boto3 >= 1.26.0`  
-`pytest`


- Tener **AWS CLI configurado correctamente** (`aws configure`)  
- Tener las **credenciales y permisos adecuados** para conectarte a los servicios de AWS como Glue, S3 y Athena  

>  **Recomendación:** Verifica la conectividad ejecutando un comando como `aws s3 ls` para validar el acceso desde tu entorno.

##  Configuración necesaria en AWS

###  Permisos de IAM

**El rol de AWS Glue debe tener permisos para:**

- Leer y escribir en los buckets S3 utilizados  
- Acceder al Glue Data Catalog   
- Ejecutar trabajos de Glue  

**El usuario o rol que ejecuta Athena desde Python debe tener permisos para:**

- Consultar en los buckets particionados  
- Ejecutar consultas en Glue Catalog (Athena)  

---

###  Políticas recomendadas

- `AmazonS3FullAccess` *(ajustar a menor privilegio en entornos productivos)*  
- `AWSGlueServiceRole`  
- `AmazonAthenaFullAccess`  
- `AWSGlueConsoleFullAccess`


---

##  Automatización del pipeline

Este pipeline puede ejecutarse automáticamente mediante:

- **AWS Glue Triggers**: Para ejecutar cargas periódicas (basadas en tiempo o evento)  
- **AWS Step Functions**: Para orquestar los pasos del pipeline completo *(opcional)*  

---

##  Ejecución local

Para pruebas locales, colócate en la raíz del proyecto y ejecuta:

```bash
python3 -m btg_DP.job.co_transacciones

```

# Pruebas de Transformaciones de Datos

Este repositorio incluye varias funciones de transformación de datos y sus respectivas pruebas unitarias utilizando `pytest` y `PySpark`. A continuación, se describe el conjunto de pruebas implementadas.

## Funciones Probadas

### 1. **`convertir_fecha(df, nombre_columna, nueva_columna)`**
Convierte una columna de tipo `timestamp` a `date`.

#### Pruebas:
- **test_convertir_fecha_sobrescribe_columna**: Verifica que la columna original sea sobrescrita correctamente con el formato de fecha.
- **test_convertir_fecha_nueva_columna**: Verifica que se cree una nueva columna con el formato de fecha, manteniendo la columna original.
- **test_convertir_fecha_columna_inexistente**: Verifica que se lance una excepción si la columna especificada no existe en el DataFrame.

### 2. **`quitar_tildes(df, nombre_columna, nueva_columna=None)`**
Elimina las tildes (acentos) de una columna de texto.

#### Pruebas:
- **test_quitar_tildes_sobrescribe_columna**: Verifica que las tildes se eliminen correctamente y se sobrescriba la columna original.
- **test_quitar_tildes_nueva_columna**: Verifica que se cree una nueva columna con los valores sin tildes, manteniendo la columna original.

### 3. **`reemplazar_nulos(df, nombre_columna)`**
Reemplaza los valores nulos o vacíos en una columna con 0.

#### Pruebas:
- **test_reemplazar_nulos**: Verifica que los valores nulos o vacíos en la columna especificada sean reemplazados por 0.
- **test_columna_no_existente**: Verifica que se lance un error si la columna especificada no existe en el DataFrame.

### 4. **`filtrar_emails_validos(df, columna="email")`**
Filtra los registros que contienen correos electrónicos válidos en la columna especificada.

#### Pruebas:
- **test_filtrar_emails_validos**: Verifica que solo los correos electrónicos válidos se mantengan en el DataFrame.
- **test_columna_no_existente**: Verifica que se lance un error si la columna especificada no existe en el DataFrame.

## Ejecución de las Pruebas

Para ejecutar las pruebas, asegúrate de tener las dependencias necesarias instaladas:

```
Python 3.7+

PySpark

pytest
```


# AWS Glue - Despliegue Automatizado con CloudFormation

Este proyecto define una infraestructura modular para la ejecución de AWS Glue Jobs utilizando CloudFormation. Todos los trabajos comparten una misma plantilla (`glue-job.yaml`), pero se diferencian por medio de parámetros que permiten reutilizar el mismo código IAC con distintos scripts y nombres.

---

## Requisitos Previos

Antes de ejecutar el despliegue:

Sube los scripts `.py` y la librería `.whl` al bucket de S3 correspondiente.

### Scripts Python:

- co_transacciones.py
- co_clientes.py
- co_proveedores.py

### Librería personalizada:

- extra-0.1.0-py3-none-any.whl

Ejecuta los siguientes comandos para subir los archivos:

```bash
aws s3 cp btg_DP/job/co_transacciones.py s3://aws-glue-assets-913404505393-us-east-1/scripts/

aws s3 cp btg_DP/job/co_clientes.py s3://aws-glue-assets-913404505393-us-east-1/scripts/

aws s3 cp btg_DP/job/co_proveedores.py s3://aws-glue-assets-913404505393-us-east-1/scripts/


aws s3 cp btg_DP/extra/extra-0.1.0-py3-none-any.whl s3://aws-glue-assets-913404505393-us-east-1/scripts/extra/

```

Comandos para Desplegar los Glue Jobs
Cada Glue Job se despliega individualmente utilizando el mismo archivo glue-job.yaml, pasando los parámetros ScriptS3Path y JobName.

### Glue Job: Transacciones

```bash
aws cloudformation deploy \
  --template-file glue-job.yaml \
  --stack-name glue-job-transacciones \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ScriptS3Path=s3://aws-glue-assets-913404505393-us-east-1/scripts/co_transacciones.py \
    JobName=glue-job-transacciones
```
### Glue Job : Clientes
```bash
aws cloudformation deploy \
  --template-file glue-job.yaml \
  --stack-name glue-job-clientes \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ScriptS3Path=s3://aws-glue-assets-913404505393-us-east-1/scripts/co_clientes.py \
    JobName=glue-job-clientes
```
Glue Job: Proveedores

```
aws cloudformation deploy \
  --template-file glue-job.yaml \
  --stack-name glue-job-proveedores \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ScriptS3Path=s3://aws-glue-assets-913404505393-us-east-1/scripts/co_proveedores.py \
    JobName=glue-job-proveedores
```

### Notas
Si usas librerías adicionales como archivos .whl, asegúrate de subirlas a una ruta válida en S3 y configurar el parámetro --extra-py-files dentro de los DefaultArguments en el archivo glue-job.yaml.

Los scripts .py deben existir en el bucket de S3 antes de ejecutar el despliegue con CloudFormation.