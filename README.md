## Importación de librerias
Se importan las librerías:Flask,Pandas,Google,werkzeug.
Y se llama al archivo credentials.py que contiene la información de credenciales para conexión con GCP (Bigquery)
```python

from flask import Flask,request
from flask_restful import Resource,Api, reqparse
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import credentials
```
## Importación de credenciales, conexión con API de Bigquery, Flask

A partir del archivo de service account se genera las credenciales que permitirá interactuar con la API de Bigquery (Bigquery.Client)

```python

credentials = service_account.Credentials.from_service_account_file(
    credentials.path_to_service_account_key_file, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
 
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
```

Creación del objeto de la APP de Flask y seteo del api
```python
app = Flask(__name__)
api = Api(app)
```
## SECCION 1: API
Hace referencia a la generación del API de la sección 1. Esta clase permitirá la inserción  de data en bigquery, la data a insertar será la contenida en un csv. Se toma como parámetro el nombre de la tabla destino.

```python
class Insertbatch(Resource):
```    
Método archivo_permitido permite identificar si el archivo a enviar es de extensión CSV
```python
    def archivo_permitido(self, filename):
        ALLOWED_EXTENSIONS = {'csv'}
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
```
Método Post
```python
    def post(self):
```
Se setea el argumento que será pasado por el método Post. El argumento a pasar es el nombre de la tabla en donde se realizará la inserción de la data
```python
        parser = reqparse.RequestParser()          
        parser.add_argument('table',type=str, required=True,location='args')
        args = parser.parse_args()  
```
La variable Table_id contiene la información del proyecto mas el dataset y la tabla (la cual se obtiene del argumento mencionado en el punto anterior)
```python
        table_id="proyectoglobant2905.proyecto."+str(args['table'])
```
En caso se enviar la consulta y en ella nos e encuentre el archivo, el mensaje será "Archivo no encontrado"
```python
        if 'file' not in request.files:
            return 'Archivo no encontrado'
```   
Se captura  el archivo csv
```python     
        file = request.files.get("file")

        
```   
Se configura la carga del archivo csv a bigquery en modo WRITE_APPEND (se inserta data)
```python      
        if file and self.archivo_permitido(file.filename):          
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0, autodetect=False,field_delimiter=",",
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
```
Se procede con la carga del archivo a bigquery, se toma como parámetro el table_id.
```python
            job = client.load_table_from_file(file.stream, table_id, job_config=job_config)
 ```   
Se obtiene resultado del job
```python            
            job.result()  
             
            client.get_table(table_id) 
  ```   
Se envía mensaje de inserción exitosa
```python          
            mensaje=    "Se insertaron los valores correctamente en la tabla  {}".format(
                     table_id
                )
             
            

        return mensaje
```   
## SECCION 2: Primer Requerimiento
Hace referencia al primer requerimiento de la Sección 2. End Point --> Requerimiento1
```python
class Requerimiento1(Resource):
```
Método get:
```python     
    def get(self):
```
Se asigna la query al client para su ejecución en Bigquery
```python 
        query_job = client.query(
                """
                select
                    department,
                    job,
                    sum(Q1) as Q1,
                    sum(Q2) as Q2,
                    sum(Q3) as Q3,
                    sum(Q4) as Q4
                    from (
                        select
                        department,
                        job,
                        case when Quarter=1 then cantidad else 0 end as Q1,
                        case when Quarter=2 then cantidad else 0 end as Q2,
                        case when Quarter=3 then cantidad else 0 end as Q3,
                        case when Quarter=4 then cantidad else 0 end as Q4,
                        from (
                            select
                            b.department, 
                            c.job,
                            EXTRACT(quarter FROM  date(a.datetime)) Quarter,
                            count(1) cantidad
                            from proyecto.hired_employees a
                            inner join
                            proyecto.departments b
                            on a.department_id=b.id
                            inner join
                            proyecto.jobs c
                            on c.id=a.job_id
                            where EXTRACT(Year FROM  date(a.datetime)) =2021
                            group by 
                            b.department, 
                            c.job,
                            a.datetime
                        )
                    ) group by 
                    department,
                    job
                    order by department,job asc

                
                
                """
            )
```
Se utiliza to_dataframe() para la conversión del resultado de la query en un dataframe que luego se convierte a un tipo de dato diccionario para que pueda ser retornado en ese formato al momento de la consulta en el api (método get)
  
```python 
        job_result = query_job.to_dataframe()  
        data = job_result.to_dict() 

        return {'data': data}, 200  
 ```  
  

## SECCION 2: Segundo Requerimiento
Hace referencia al segundo requerimiento de la Sección 2. End Point --> Requerimiento2

```python 
 
class Requerimiento2(Resource):
```
Se asigna la query al client para su ejecución en Bigquery
```python 
     
    def get(self):
        query_job = client.query(
                """
                with 
                    total as (
                        select
                        a.department_id,
                        b.department,
                        extract(year from date(a.datetime)) year,
                        count(1) cantidad
                        from proyecto.hired_employees a
                        inner join
                        proyecto.departments b
                        on a.department_id=b.id
                        group by
                        a.department_id,
                        b.department  ,
                        a.datetime
                    )
                    ,
                    datosacum as (
                        select Year,sum(cantidad) total_cont,count(distinct department) cantidad_dep,   sum(cantidad)/count(distinct department) as promedio
                        from total
                        group by Year
                    ),
                    promedio as (
                        select department_id,department, num_contrata from (
                                select department_id,department,sum(cantidad) num_contrata  , (select promedio from datosacum where year=2021) promedio
                                from total 
                                group by department_id,department
                                )
                                where num_contrata>promedio
                                order by num_contrata desc
                    )
                    select * from promedio

                
                
                """
            )
```
Se utiliza to_dataframe() para la conversión del resultado de la query en un dataframe que luego se convierte a un tipo de dato diccionario para que pueda ser retornado en ese formato al momento de la consulta en el api (método get)
```python
        job_result = query_job.to_dataframe()  
        data = job_result.to_dict()  
        return {'data': data}, 200  
       
```

Se añaden los end_points
```python
api.add_resource(Requerimiento1, '/Requerimiento1')   
api.add_resource(Requerimiento2, '/Requerimiento2')   
api.add_resource(Insertbatch, '/Insertbatch')  

 
    
```
```python
if __name__ == '__main__':
    app.run(debug=True)  
```