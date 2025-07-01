import pandas as pd
from  utils.utils import type_mapping
from kylin.kylin_client import KylinClient

class Cube:
    def __init__(self, meta_data:dict, kylin_client:KylinClient):
        self.meta_data:dict = meta_data
        self.kylin_client:KylinClient = kylin_client 

    def get_project_name(self):
        '''
            @Retorna
                List: lista de proyectos como objetos dict
        '''
        return self.meta_data['project']

    def get_cube_name(self):
        '''
            @Retorna
                String: nombre del cubo
        '''
        return self.meta_data['name']
    
    def get_tables_name(self):
        '''
            @Retorna
                Lista: lista de tablas que intervienen en el cubo
        '''
        # Obtener nombre de fact table
        fact_table_name = [self.meta_data['fact_table'].split('.')[-1]]
        # Obtener tablas joins
        lookups = [lookup['table'].split('.')[-1] for lookup in self.meta_data['lookups']]

        return fact_table_name + lookups
    
    def get_measures_name(self):
        '''
            @Retorna
                Lista: lista de todas las medidas en el cubo
        '''
        return [lookup['name'].split('.')[-1] for lookup in self.meta_data['all_measures']]
    
    def _get_raw_measure_from_measure_name(self, measure_name):
        '''
            @Parametros
                measure_name: string nombre de la medida
            @Retorna
                measure: objeto dict con metadata de la medida
        '''
        for measure in self.meta_data['all_measures']:
            if measure['name'] == measure_name:
                return measure

    def get_dimensions_from_table(self, table_name):
        '''
            @Parametros
                table_name: string nombre de la tabla
            @Retorna
                List: lista de medidas de acuerdo al nombre de la tabla
        '''        
        return [dimension['name'] for dimension in self.meta_data['simplified_dimensions'] if dimension['column'].split('.')[0] == table_name]
    
    def _get_default_dimension_name(self, dimension_name):
        '''
            @Parametros
                dimension_name: string nombre de la dimension seleccionada
            @Retorna
                String: nombre original de dimesion si es considerada como columna | columna en si
        '''  
        # Primero probar en las columnas
        for named_column in self.meta_data['all_named_columns']:
            if named_column['name'].lower() == dimension_name.lower():
                return named_column['name']
        
        # Ver si la columna agregada corresponse a measures
        for measure in self.meta_data['all_measures']:
            if measure['name'].lower() == dimension_name.lower():
                return measure['name']


    def _build_sql(self, measures:list):
        '''
            @Parametros
                measures: lista de nombre con las medidas del cubo
            @Retorna
                String: construccion del sql
        '''
        sql_lines = []

        # SELECT
        select_lines = []
        
        # Atributos (dimensiones)
        for table_name in self.get_tables_name():
            for dimension_name in self.get_dimensions_from_table(table_name):
                select_lines.append(f'{table_name}.{dimension_name} AS {dimension_name}')

        # Medidas
        measure_lines = []
        for measure_name in measures:#self.meta_data['all_measures']:
            measure_data = self._get_raw_measure_from_measure_name(measure_name)
            name = measure_data['name']
            func_expr = measure_data['function']['expression']
            parameters = measure_data['function']['parameters']
        
            sql_embedded_args = []

            for param in parameters:
                if param['type'] == 'column':
                    table, column = param['value'].split('.')
                    sql_embedded_args.append(f'{table}.{column}')
                elif param['type'] == 'constant':
                    sql_embedded_args.append(str(param['value']))

            args_str = ', '.join(sql_embedded_args)
            measure_lines.append(f'{func_expr}({args_str}) AS {name}')

        # Combina dimensiones + medidas
        all_select_lines = select_lines + measure_lines
        sql_lines.append('SELECT ' + ', '.join(all_select_lines))

        # FROM (tabla de hechos) de cargarse para poder realizar
        # los disntintos joins en las tablas
        data_base, fact_table = self.meta_data['fact_table'].split('.')
        sql_lines.append(f'FROM {data_base}.{fact_table}')

        # JOINS
        for lookup in self.meta_data['lookups']:
            db, table = lookup['table'].split('.')
            join_type = lookup['join']['type']

            join_clause = f'{join_type} JOIN {db}.{table} ON '
            join_conditions = []
            for pk, fk in zip(lookup['join']['primary_key'], lookup['join']['foreign_key']):
                table_pk, col_pk = pk.split('.')
                table_fk, col_fk = fk.split('.')
                join_conditions.append(f'{table_pk}.{col_pk} = {table_fk}.{col_fk}')

            join_clause += ' AND '.join(join_conditions)
            sql_lines.append(join_clause)

        # GROUP BY (si hay medidas y dimensiones)
        if measures:
            sql_embedded_args = []

            for table_name in self.get_tables_name():
                for dimension_name in self.get_dimensions_from_table(table_name):
                    sql_embedded_args.append(f'{table_name}.{dimension_name}')
            
            
            sql_lines.append('GROUP BY ' + ', '.join(sql_embedded_args))

        # Devuelve el SQL final como string
        return ' '.join(sql_lines)

 
    # Star Schema debe cargarse
    def get_raw_data(self, measures: list) -> pd.DataFrame: 
        '''
            @Parametros
                measures: lista de nombre con las medidas del cubo
            @Retorna
                DataFrame: data_frame con datos se denomina start schema
        '''
        # Obtener sql agregando las medidas necesarias
        sql = self._build_sql(measures)   

        # Realizar consulta acerca de datos
        raw_data = self.kylin_client.execute_query(project_name= self.get_project_name(), sql= sql)
        
        # La consula sql devulve valores de metadata en mayuscula debe
        # cambiarse a labels originales
        meta_data_columns = {}
        for column in raw_data['columnMetas']:
            dimension_name = self._get_default_dimension_name(column['name'])
            meta_data_columns[dimension_name] = column['columnTypeName']

        results = raw_data['results']

        data_frame = pd.DataFrame(columns= meta_data_columns.keys(), data= results)

        # Parsear tipo de datos
        for column, typ in meta_data_columns.items():
            target_type = type_mapping.get(typ.upper())
            if target_type:
                try:
                    if 'datetime' in target_type:
                        data_frame[column] = pd.to_datetime(data_frame[column], errors='coerce')
                    else:
                        data_frame[column] = data_frame[column].astype(target_type)
                except Exception as e:
                    ValueError(f"Error al convertir la columna {column} al tipo {target_type}: {e}")

        return data_frame
    
    # Tables loaded una vez cargada star schema
    def get_raw_table_data(self, raw_data:pd.DataFrame):
        '''
            @Parametros
                raw_data: data_frame start schema
            @Retorna
                {str: DataFrame}: data_frame con datos se denomina start schema
        '''
        tables_loaded = {}
        for table_name in self.get_tables_name():
            # Obtener lista de dimensiones de acuerdo a tabla
            dim_columns = self.get_dimensions_from_table(table_name)
            
            # Crear DataFrame con las columnas de dimensión como índices
            tables_loaded[table_name] = raw_data[dim_columns]
                    
        return tables_loaded



        
