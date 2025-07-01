import itertools
import pandas as pd
from model.cube import Cube
from collections import OrderedDict
from kylin.kylin_client import KylinClient
from kylin.kylin_client import KylinClient
from olapy.core.mdx.parser import MdxParser
from olapy.core.mdx.parser import MdxParser
from olapy.core.mdx.executor.execute import MdxEngine

class KylinMdxEngine(MdxEngine):
    def __init__(self, kylin_client:KylinClient):
        # Todas estas variables son importantes ya que son usadas en clases, por ende 
        # deben estar embebidas si aun no son necesarias para el uso en este caso de Kylin
        self.cube = None
        self.measures = None
        self.sqla_engine = None
        self.cube_config = None
        self.tables_loaded = None
        self.facts: str = 'Facts'
        self.db_cubes: list = None
        self.selected_measures = None
        self.cubes_folder: str = None
        self.source_type: str = 'Kylin'
        self.csv_files_cubes: list = None
        self.star_schema_dataframe = pd.DataFrame()

        # Un parser de consultas mdx a tuplas y listas
        self.parser: MdxParser = MdxParser()
        
        self.kylin_client: KylinClient = kylin_client
        self.olapy_data_location: str = self.kylin_client.base_url
        
        # Obtener cubos en lista
        self.cubes = []
        # Tener referencia a instancia actual de cubo
        self.cube_instance = None

    '''
        ESTAS FUNCIONES SON NECESARIAS PARA QUE LA LIBERIA NO COLAPSE
    '''
    def load_cubes(self):
        # Cargar cubos
        cubes = self.kylin_client.get_cubes()

        # Agregar cubos lista
        for cube in cubes:
            self.cubes.append(
                Cube(
                    meta_data= cube,
                    kylin_client= self.kylin_client
                )
            )

    def get_cubes_names(self):
        '''
            @Retorna
                Lista: lista con nombres del cubo
        '''
        # Validar si existen datos de cubos
        if not self.cubes:
            ValueError('Do not load cubes')
        # Retornar lista con nombre de los cubos
        return [cube.get_cube_name() for cube in self.cubes]
        
    def load_cube(self, cube_name, **kwargs):
        '''
            @Parametros
                cube_name: String nombre del cubo actual
        '''
        # Determinar si el existen cubos
        if not self.cubes:
            ValueError('Do not load cubes')

        # Obtener instancia del cubo actual
        is_cube_loaded = False
        for cube in self.cubes:
            if cube_name == cube.get_cube_name():
                self.cube = cube_name
                self.cube_instance = cube
                is_cube_loaded = True
                break
        
        if not is_cube_loaded:
            ValueError('Do not load cube_instance')
        
        # Definir measures
        self.measures = self.get_measures()
        # selected_measures por defecto es la primera
        self.selected_measures = [self.measures[0]]
  

    def get_measures(self):
        '''
            @Retorna
                Lista: lista de nombre de medidas del cubo actual
        '''

        if not self.cube_instance:
            ValueError('Do not load cube_instance')

        return self.cube_instance.get_measures_name()


    def get_all_tables_names(self, ignore_fact=False):
        '''
            @Parametros
                ignore_fact: parametro no necesario
            @Retorna
                List: lista de nombres de la tablas que componen el cubo
        '''
        if not self.cube_instance:
            ValueError('Do not load cube_instance')

        return self.cube_instance.get_tables_name()

    def execute_mdx(self, mdx_query):
        '''
            @Parametros
                mdx_query: string con consulta de excel MDX
            @Retorna
                Dict: dicionario necesario para que el executor pueda ejecutar la consulta
        '''
        # Limpiar el mdx para obtener los paramatros necesarios
        query = self.clean_mdx_query(mdx_query)
        # Obtener medidas del campo where
        query_axes = self.parser.decorticate_query(query)
        
        # Cargar datos 
        self._load_data(query_axes= query_axes)
        
        if self.change_measures(query_axes["all"]):
            self.selected_measures = self.change_measures(query_axes["all"])


        tables_n_columns = self.get_tables_and_columns(query_axes)

        columns_to_keep = OrderedDict(
            (table, columns)
            for table, columns in tables_n_columns["all"].items()
            if table != self.facts
        )

        tuples_on_mdx_query = self._uniquefy_tuples(query_axes["all"])
        tuples_on_mdx_query.sort(key=lambda tupl: (tupl[0], len(tupl)))

        if tuples_on_mdx_query:

            if self.check_nested_select():
                df_to_fusion = self.nested_tuples_to_dataframes(columns_to_keep)
            else:
                df_to_fusion = self.tuples_to_dataframes(
                    tuples_on_mdx_query, columns_to_keep
                )
            df = self.fusion_dataframes(df_to_fusion)
            cols = []
            
            cols = list(itertools.chain.from_iterable(columns_to_keep.values()))
            sort = self.parser.hierarchized_tuples()

            result = df.groupby(cols, sort=sort).sum()[self.selected_measures]
            
        else:
            result = (
                self.star_schema_dataframe[self.selected_measures].sum().to_frame().T
            )

        return {"result": result, "columns_desc": tables_n_columns}
    
    '''
        ESTAS FUNCIONES SON AGREGACIONES NECESARIAS PARA EL CODIGO
    '''
    def _load_data(self, query_axes):
        '''
            @Parametros
                discover_request_handler: instancia de XmlaDiscoverReqHandlerKylin
                execute_request_handler: instancia de XmlaExecuteReqHandler
            @Retorna
                Application: instancia server wsgi
        '''
        measures = [data[-1] for data in query_axes['all'] if data[0] == 'Measures']
               
        if self.star_schema_dataframe.empty or not all(measure in self.star_schema_dataframe.columns for measure in measures): 
            # Cargar star schema
            self.star_schema_dataframe = self.cube_instance.get_raw_data(measures)
            # Cargar tablas de datos
            self.tables_loaded = self.cube_instance.get_raw_table_data(self.star_schema_dataframe)
        
            if not measures:
                self.star_schema_dataframe[self.selected_measures] = 1



