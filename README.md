# ETL de cubos de Istac

Esta ETL realiza la carga de cubos de datos de la API de e-cubos de Istac.

## Prerrequisitos

Para poder ejecutar el proceso de ETL en un equipo es necesario disponer del siguiente software:

| Software | Versión |
| :------: | :-----: |
|  Python  | >= 3.8  |
|   pip    |   \*    |

El fichero [requirements.txt](./requirements.txt) contiene las dependencias que necesita el script para funcionar adecuadamente. Es recomendable crear un entorno virtual de Python[¹ ²](#referencias) (`virtualenv`), acceder al mismo e instalar dichas  dependencias mediante el comando `pip install -r requirements.txt` (recuerde ubicarse en su shell dentro del directorio `istac_cubos`).

Ejemplo:

```bash
  $ cd customerexperience-vertical/scripts/istac_cubos
  $ python3 -m venv venv
  $ source venv/bin/activate
  (venv)$ python -m pip install --upgrade pip
  (venv)$ python -m pip install -r requirements.txt
```

## Configuración

### Selección de cubos

Para efectuar la carga de los datos de un cubo, la ETL necesita un **id de cubo** y una **URL de descarga**. Ambos datos se encuentran a través de la aplicación **jaxi** del istac, accesible en la URL https://datos.canarias.es. Una vez que se localiza un conjunto de datos de interés, se debe hacer click sobre el enlace al cubo estadístico en formato PC-Axis:

![seleccion de cubo](img/seleccion_cubo.png)

Esto conduce a la página donde se puede recuperar el ID y el enlace de descarga del cubo estadístico en particular, por ejemplo:

![ID y URL de descarga](img/datos_cubo.png)

La lista completa de IDs de cubo y URLs de descarga a obtener debe volcarse a un fichero CSV, separado por comas, con codificación de caracteres **utf-8**. Este CSV debe tener al menos dos columnas:

- **id**: Se usará como ID de la entidad `IstacCube` en la que se almacenen los datos de este recurso
- **download**: URL de descarga del recurso pc-axis, típicamente del formato `http://www.gobiernodecanarias.org/istac/jaxi-istac/descarga.do?uripx=urn:uuid:...`

El CSV puede tener otras columnas, que la ETL ignorará. Esas columnas adicionales pueden usarse como referencia, por ejemplo para identificar qué departamento del cliente solicita la integración de ese recurso, o qué información quiere obtener de él.

En este repositorio se incluye un CSV de ejemplo, [cubos.csv](./cubos.csv), con los 325 cubos solicitados por el proyecto de Mallorca. Cada proyecto podrá personalizar su selección de cubos.

También se incluye en este repositorio un playbook de ejemplo, [spyder.ipynb](./spyder.ipynb), con el código que se utilizó para obtener esta información a partir de los requisitos dados por el Consell de Mallorca.

### Variables de configuración

Además del CSV de cubos a cargar, la ETL requiere de varias variables de configuración, que pueden proporcionarse bien en un fichero de configuración, o bien como variables de entorno:

- Sección **environment**:

  - *endpoint_cb (variable de entorno ETL_ISTAC_ENDPOINT_CB)*: URL del CB, por ejemplo `http://iot.lab.urbo2.es:1026`
  - *endpoint_keystone (variable de entorno ETL_ISTAC_ENDPOINT_KEYSTONE)*: URL del CB, por ejemplo `http://iot.lab.urbo2.es:5001`
  - *service (variable de entorno ETL_ISTAC_SERVICE)*: Nombre del servicio, por ejemplo `alcobendas`
  - *subservice (variable de entorno ETL_ISTAC_SUBSERVICE)*: Nombre del subservicio, por ejemplo `/customerexperience`
  - *user (variable de entorno ETL_ISTAC_USER)*: Nombre de usuario de keystone
  - *password (variable de entorno ETL_ISTAC_PASSWORD)*: Password de keystone

- Sección **settings**:

  - *sleep_send_batch (variable de entorno ETL_ISTAC_SLEEP_SEND_BATCH)*: Tiempo de espera (segundos) entre envío de batches consecutivos a la plataforma. Por defecto, 2
  - *timeout (variable de entorno ETL_ISTAC_TIMEOUT)*: Timeout de peticiones a plataforma (segundos). Por defecto, 10
  - *post_retry_connect (variable de entorno ETL_ISTAC_POST_RETRY_CONNECT)*: Reintentos de petición a plataforma. Por defecto, 3
  - *post_retry_backoff_factor (variable de entorno ETL_ISTAC_POST_RETRY_BACKOFF_FACTOR)*: Tiempo entre reintentos. Por defecto, 5
  - *batch_size (variable de entorno ETL_ISTAC_BATCH_SIZE)*: Tamaño del batch de entidades a guardar cada vez. Por defecto, 50
  - *csv_file (variable de entorno ETL_ISTAC_CSV_FILE)*: Ruta al fichero CSV con los ID y URLS de descarga de los cubos. No tiene valor por defecto.
  - *sql_file (variable de entorno ETL_ISTAC_SQL_FILE)*: Si se especifica, en lugar de salvar los datos directamente al context broker, la ETL crea este fichero y escribe en él todas las sentencias SQL necesarias para insertar los datos directamente en la base de datos, sin pasar por la API de Orion. Esto puede utilizarse para reducir la carga en el context broker o mejorar el tiempo de proceso de la ETL, ya que hacer la inserción directamente contra postgres es mucho más rápido que hacerlo a través del context broker. La carga en postgres del fichero generado debe hacerse manualmente, con un comando como `psql -h <host> -p <puerto> -d <database> -1 -f <sql_file>`.

El fichero [config.example.cfg](./config.example.cfg) contiene un ejemplo de fichero de configuración, por si quiere utilizarse este mecanismo en lugar de (o además de) variables de entorno.

En caso de que una variable esté definida tanto en el fichero de configuración como en el entorno (*ETL_ISTAC_...*), **el valor de la variable de entorno tiene preferencia**.

## Ejecución

### Lanzamiento del proceso

Para iniciar el proceso se ha de ejecutar desde el virtualenv el comando:

```bash
(venv)$ python etl.py
```

Si se está usando un fichero de configuración, se debe especificar la ruta al mismo con la opción `-c` (`--config`):

```bash
(venv)$ python etl.py -c <ruta/al/fichero/config.cfg>
```

El fichero de config no es necesario si todas las variables de ejecución se configuran como variables de entorno (*ETL_ISTAC_...*).

### Modelo resultante

El proceso de ETL se encarga de leer y enviar al _Context Broker_ la siguiente información:
- Metadatos asociados a cada uno de los cubos enumerados en el fichero csv, a través de entidades de tipo `IstacCubeMetadata`.
- Cada una de las coordenadas del cubo leido, a través de entidades del tipo `IstacCube`.

### Recarga de datos

Cuando la ETL completa la carga de un recurso en el context broker, crea una o dos entidades de tipo `IstacCubeBookmark` en el Context Broker, para marcar la fecha del dato más reciente cargado:

- La fecha del dato anual más reciente de un cubo se almacenará en una entidad `IstacCubeBookmark` con ID `{id del cubo}:yearly`.
- La fecha del dato mensual más reciente de un cubo (si tiene datos de granularidad mensual) se almacenará en una entidad `IstacCubeBookmark` con ID `{id del cubo}:monthly`.

En la próxima ejecución de la ETL, el script comprobará estos valores para **no volver a cargar datos de ese cubo para la misma fecha, o anteriores**.

En el caso excepcional de que se quisiera recargar desde la ETL un recurso completo, y no solo de manera incremental, sería necesario seguir estos pasos:

- Eliminar de la tabla de base de datos `cx_istaccube` los registros con el `entityid` a recargar.
- Eliminar del Context Broker las entidades `IstacCubeBookmark` con id `{id de recurso}:yearly` e `{id de recurso}:monthly`
- Volver a lanzar la ETL.

## Referencias

1. Tutorial de virtualenv, en inglés
   https://docs.python-guide.org/dev/virtualenvs/#lower-level-virtualenv

2. Tutorial de virtualenv, en español
   https://rukbottoland.com/blog/tutorial-de-python-virtualenv/
