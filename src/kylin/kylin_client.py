import re
import requests
from requests.auth import HTTPBasicAuth


class KylinClient:
    def __init__(self, host: str = 'localhost', port: int = 7070) -> None:
        self.base_url = f'http://{host}:{port}/kylin/api'

    def authentication(self, user:str, password:str) -> dict:
        '''
            @Parametros
                user: String con nombre de usuario
                password: String con contraseña de usuario en Kylin
            @Retorna
                True | False: Segun autenticacion
        '''
        try:
            self.auth = HTTPBasicAuth(user, password)
            url = f'{self.base_url}/user/authentication'
            response = requests.post(url=url, auth=self.auth, verify=False)
            response.raise_for_status()
            # Obtener codigo de respuesta en JSON
            status_code = response.json()['code']

            if status_code == '000':
                self.user = user
                self.password = password
                return True
            else:
                return False

        except requests.RequestException:
            return None

    def get_projects(self):
        '''
            @Retorna
                response: Objeto diccionario data_projects 
        '''
        try:
            url = f"{self.base_url}/projects"
            response = requests.get(url=url, auth=self.auth, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            return None

    def get_projects_by_user(self):
        '''
            @Retorna
                response: Lista de projectos por el usuario 
        '''
        projects = self.get_projects()
        if projects:
            return [project['name'] for project in projects if project['status'] == 'ENABLED' and project['owner'] == self.user]
        return None

    def get_cubes(self) -> dict:
        '''
            @Retorna
                response: Lista de metadata de cubos 
        '''
        try:
            # Obtener todos los proyectos que son accesibles al usuario
            projectsName = self.get_projects_by_user()
            if projectsName:
                cubes = []
                url = f'{self.base_url}/cubes'
                # Iterar sobre proyectos para obetener cubos
                for projectName in projectsName:
                    params = {'projectName': projectName}
                    response = requests.get(url=url, auth=self.auth, params=params, verify=False)
                    response.raise_for_status()
                    # Se agrega metada cubo en lista
                    cubes += response.json()['data']['cubes']
                return cubes
        except requests.RequestException:
            return None

    def get_sql_from_cube(self, project_name: str, cube_name: str):
        '''
            @Parametros
                project_name: String nombre del proyecto en que estan cubos
                cube_name: String cnombre del cubo especifico
            @Retorna
                response: String sql de como relizar la consulta al cubo 
        '''
        try:
            url = f'{self.base_url}/cubes/{cube_name}/sql'
            body = {
                "project": project_name
            }
            response = requests.get(url=url, auth=self.auth, verify=False, json= body)
            response.raise_for_status()
            # Obtener raw sql
            sql = response.json()['data']['sql']
            # Obtener cosulta mas elegible
            clean_sql = re.sub(r'\s+as\s+"[^"]+"', '', sql, flags=re.IGNORECASE)

            return clean_sql
        except requests.RequestException:
            return None

    def execute_query(self, sql: str, project_name: str = None, limit: int = 2) -> dict:
        '''
            @Parametros
                sql: String sql del cubo
                project_name: String cnombre del proyecto
                limit: Int numero maximo de tuplas a obtener
            @Retorna
                response: Objeto diccionario con datos de consulta
        '''
        try:
            url = f"{self.base_url}/query"
            body = {
                "sql": sql,
                "limit": limit,
                "project": project_name
            }
            
            response = requests.post(url, auth=self.auth, json=body)
            response.raise_for_status()
            return response.json()
        except requests.RequestError as e:
            return None

