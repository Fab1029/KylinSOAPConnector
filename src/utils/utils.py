# Utilidad necesaria para administrar y 
# parsear tipos de datos al obtener consulta de datos
type_mapping = {
    'VARCHAR': 'string',
    'STRING': 'string',
    'CHAR': 'string',
    'INT': 'Int64',  
    'BIGINT': 'Int64',
    'DOUBLE': 'float',
    'FLOAT': 'float',
    'DECIMAL': 'float',
    'DATE': 'datetime64[ns]',
    'DATETIME': 'datetime64[ns]',
    'TIMESTAMP': 'datetime64[ns]',
    'INTEGER': 'Int64'
}