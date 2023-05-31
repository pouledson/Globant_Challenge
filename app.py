from flask import Flask,request
from flask_restful import Resource,Api, reqparse
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from werkzeug.utils import secure_filename
import credentials

credentials = service_account.Credentials.from_service_account_file(
    credentials.path_to_service_account_key_file, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
 
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)




app = Flask(__name__)
api = Api(app)

    

class Requerimiento1(Resource):

    # methods go here
    
    def get(self):
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
        job_result = query_job.to_dataframe()  # Waits for job to complete.
        data = job_result.to_dict()  # convert dataframe to dictionary
        return {'data': data}, 200  # return data and 200 OK code
    
class Requerimiento2(Resource):

    # methods go here
    
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
        job_result = query_job.to_dataframe()  # Waits for job to complete.
        data = job_result.to_dict()  # convert dataframe to dictionary
        return {'data': data}, 200  # return data and 200 OK code
       

class Insertbatch(Resource):
    
    def allowed_file(self, filename):
        ALLOWED_EXTENSIONS = {'csv'}
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
    def post(self):
        # Get Text type fields
        parser = reqparse.RequestParser()          
        parser.add_argument('table',type=str, required=True,location='args')
        args = parser.parse_args()  # parse arguments to dictionary

        table_id="proyectoglobant2905.proyecto."+str(args['table'])
        if 'file' not in request.files:
            return 'No file part'
        
        file = request.files.get("file")
        if file and self.allowed_file(file.filename):
            filename = secure_filename(file.filename)
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0, autodetect=False,field_delimiter=",",
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )

            job = client.load_table_from_file(file.stream, table_id, job_config=job_config)
                
            job.result()  # Waits for the job to complete.
             
            client.get_table(table_id)  # Make an API request.
             
            mensaje=    "Se insertaron los valores correctamente en la tabla  {}".format(
                     table_id
                )
             
            

        return mensaje

api.add_resource(Requerimiento1, '/Requerimiento1')   
api.add_resource(Requerimiento2, '/Requerimiento2')   
api.add_resource(Insertbatch, '/Insertbatch')  

 
    


if __name__ == '__main__':
    app.run(debug=True)  # run our Flask app