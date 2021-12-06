# Trabajo Final - Seminario Intensivo de Tópicos Avanzados

Alumno: Nicolás Marcantonio

## Objetivo

El objetivo de este trabajo es recolectar información climática de ciertos lugares de interés (en este caso algunas ciudades argentinas), 
analizar información relevante y visualizarla en un dashboard.

Para ello se utilizarán las siguientes herramientas/tecnologías:
- Docker
- Airflow
- Spark
- Superset


## Ambiente

Clonar el repositorio

```shell
https://github.com/nicomarcan/weather-analysis.git

cd weather-analysis

./control-env.sh start
```
Una vez levantado, proceder a realizar lo siguiente en las distintas herramientas:

## Airflow

- Ingresar a http://localhost:9090/ 
- Poner en on el switch que se encuentra en off en la fila del DAG weather (Ver imagen)
- Hacer click en el botón de play (Ver imagen)

![](https://github.com/nicomarcan/weather-analysis/blob/main/screenshots_readme/Captura%20de%20Pantalla%202021-11-29%20a%20la(s)%2023.50.57.png?raw=true)


## Predicción de ciudad en base a características climáticas y fecha

- Recuperación de la informacion de la base de datos previamente guardada por el DAG de airflow.


- Enriquecimiento del dataframe con los campos día, mes, año y estación (ésta última se ignora dado que la API usada sólo permite rescatar datos de este mes por lo que siempre es primavera)

- Preprocessing de los datos para transformarlo en un dataframe compatible con las librerías de ML de Spark. Utilizando One Hot encoder, string indexer y Vector assembler.

- Se probó utilizar regresión lineal y un árbol de decisión (ambos resultaron poco precisos dado a la poca cantidad de datos que se pudieron recolectar en el plan gratuito)


## Superset


## Crear conexión con base de datos de info climática

~~~
Ver paso a paso en las siguientes imágenes
~~~

![](https://github.com/nicomarcan/weather-analysis/blob/main/screenshots_readme/Captura%20de%20Pantalla%202021-11-29%20a%20la(s)%2018.36.59.png?raw=true)
![](https://github.com/nicomarcan/weather-analysis/blob/main/screenshots_readme/Captura%20de%20Pantalla%202021-11-29%20a%20la(s)%2018.37.35.png?raw=true)


## Importar dashboard.json  (superset/conf/dashboard.json)
![](https://github.com/nicomarcan/weather-analysis/blob/main/screenshots_readme/Captura%20de%20Pantalla%202021-11-29%20a%20la(s)%2018.37.49.png?raw=true)

![](https://github.com/nicomarcan/weather-analysis/blob/main/superset/conf/dashboard.jpg?raw=true)

