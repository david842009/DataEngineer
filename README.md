# 📌 Prueba Tecnica

## 📖 Descripción
Este desarrollo realiza el procesamiento de datos para la API de lanzamientos espaciales, su arquitectura esta basada en un motor de procesamiento Apache Spark, el orquestador de pipelines por Apache Airflow y un almacenamiento en Delta Lake. 

## 🚀 Descripcion de Características
**Apache Spark:** es un motor de procesamiento de datos de código abierto diseñado para manejar grandes volúmenes de información de manera distribuida y eficiente. Es ampliamente utilizado en el análisis de datos, aprendizaje automático y procesamiento de datos en tiempo real. Sus principales características incluyen:
    - Procesamiento en memoria para mayor velocidad.
    - Soporte para múltiples lenguajes como Python, Scala y Java.
    - Integración con diversas fuentes de datos, como HDFS, S3, Cassandra y MongoDB.
    - Compatible con procesamiento en lotes y en tiempo real mediante Spark Streaming.

**Apache Airflow:** es una plataforma de orquestación de flujos de trabajo que permite la programación, ejecución y monitoreo de tareas complejas en procesos de datos. Es ampliamente utilizado en la gestión de ETLs, pipelines de datos y automatización de procesos empresariales. Sus principales características incluyen:
    - Definición de flujos de trabajo mediante código en Python.
    - Planificación y ejecución programada de tareas.
    - Integración con múltiples herramientas y servicios en la nube.
    - Visualización de la ejecución de flujos de trabajo mediante su interfaz web.
    - Manejo de dependencias entre tareas de forma flexible.

**Delta Lake** es una capa de almacenamiento open-source que mejora la confiabilidad y el rendimiento de los datos almacenados en formato **Parquet**, proporcionando transacciones ACID, versionado de datos y manejo eficiente de esquemas. 

**Databricks** implementa Delta Lake bajo un enfoque de **Esquema Medallion**, que organiza los datos en tres capas:
- **Bronze Layer:** Contiene los datos crudos tal como fueron ingeridos desde las fuentes, sin transformaciones. Sirve como respaldo en caso de errores en el procesamiento.
- **Silver Layer:** Contiene datos limpios y procesados, con una estructura más organizada y enriquecida. Aquí se eliminan duplicados y se aplican reglas de calidad de datos.
- **Gold Layer:** Es la capa de datos altamente optimizados y listos para análisis y consumo en reportes y modelos de machine learning. Generalmente, los datos en esta capa están agregados y optimizados para rendimiento.

El uso del esquema **Medallion** permite una trazabilidad clara del linaje de datos, facilitando la gobernanza y el versionado de datos dentro de **Databricks**.


## 📦 Instalación
### Requisitos previos
Asegúrate de tener instalado:
- **Docker**(4.38.0)
- **VisualStudioCode** (>= 1.98.0)
- **Github**

### Clonar el repositorio
```bash
git clone https://github.com/tu-repo.git
cd tu-repo
```

### Configuración del Backend (FastAPI)
1. Crear un entorno virtual:
```bash
python -m venv env
source env/bin/activate  # Linux/macOS
env\Scripts\activate  # Windows
```
Despliegue con Docker
Para ejecutar la aplicación con Docker Compose:
```bash
docker-compose up --build
```

### 🚢 Despliegue de Imágenes de Docker en el Ambiente
Para desplegar las imágenes de Docker dentro del ambiente, sigue estos pasos:

1. **Construir las imágenes de Docker**
```bash
docker-compose build
```

2. **Verificar que las imágenes han sido creadas**
```bash
docker images
```

3. **Etiquetar y subir las imágenes a un registro de contenedores (opcional)**
Si deseas almacenar las imágenes en un **Docker Registry** como Docker Hub o Azure Container Registry:
```bash
docker tag nombre-imagen usuario/nombre-imagen:latest
docker push usuario/nombre-imagen:latest
```

4. **Levantar los contenedores en el servidor de producción**
```bash
docker-compose up -d
```

5. **Verificar que los contenedores están en ejecución**
```bash
docker ps
```

6. **Acceder a los logs en caso de errores**
```bash
docker logs -f nombre-contenedor
```

7. **Detener y eliminar los contenedores si es necesario**
```bash
docker-compose down
```

## 🐳 Ejecución con Docker
Para ejecutar la aplicación con Docker Compose:
```bash
docker-compose up --build
```
