from app import app
import unittest
from unittest import mock
import io
from pathlib import Path
from pathlib import Path

class FlaskTest(unittest.TestCase):
  
    #Test consulta Get 
    def test_GetRequerimiento1(self):
        tester = app.test_client(self)

        response = tester.get("/Requerimiento1")

        statuscode = response.status_code
        self.assertEqual(statuscode, 200)

    def test_GetRequerimiento2(self):
        tester = app.test_client(self)

        response = tester.get("/Requerimiento2")

        statuscode = response.status_code
        self.assertEqual(statuscode, 200)
    
    #Si resultado del get es Json
    def test_Requerimiento1Json(self):
        tester = app.test_client(self)

        response = tester.get("/Requerimiento1")

        self.assertEqual(response.content_type, "application/json")
    def test_Requerimiento2Json(self):
        tester = app.test_client(self)

        response = tester.get("/Requerimiento2")

        self.assertEqual(response.content_type, "application/json")
    
    #Test API Post al adjuntar archivo
    
    def test_APIPostCSV(self):
        tester = app.test_client(self)
        resources = Path(__file__).parent / "data_challenge_files"
        print(resources / "jobs.csv")
        response = tester.post("/Insertbatch", data={
                                "table": "jobs",
                                "file": (resources / "jobs.csv").open( 'rb'),
        })

        statuscode = response.status_code
        print(response.data)
        self.assertEqual(statuscode, 200)
  
if __name__ == "__main__":
  unittest.main()