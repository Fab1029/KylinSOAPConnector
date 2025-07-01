import xmlwitch
from olapy.core.services.xmla_discover_request_handler import XmlaDiscoverReqHandler

from olapy.core.services.xmla_discover_xsds import (
    mdschema_levels_xsd,
    mdschema_dimensions_xsd,
    mdschema_hierarchies_xsd,
)


class XmlaDiscoverReqHandlerKylin(XmlaDiscoverReqHandler):
    
    def change_cube(self, new_cube):
        '''
            @Parametros
                new_cube: String con nombre del cubo
        '''
        if self.selected_cube != new_cube:
            # Cambiar por el nombre del cubo seleccionado actualmente
            self.selected_cube = new_cube
            # Cargar cubo
            self.executor.load_cube(new_cube)

    
    def mdschema_dimensions_response(self, request):
        '''
            @Parametros
                request: Object request discover
            @Retorna
                xml: Object xml
        '''
        xml = xmlwitch.Builder()
        # Construccion del xml
        with xml["return"]:
            # Cabeceras necesarias para root
            with xml.root(
                xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                **{
                    "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                    "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                },
            ):
                # Obtener schema determinado para este discover request
                xml.write(mdschema_dimensions_xsd)
                if request.Restrictions.RestrictionList:
                    if (
                        request.Restrictions.RestrictionList.CUBE_NAME
                        == self.selected_cube
                        and request.Restrictions.RestrictionList.CATALOG_NAME
                        == self.selected_cube
                        and request.Properties.PropertyList.Catalog is not None
                    ):
                        # Actualizar apuntador a cubo
                        self.change_cube(request.Properties.PropertyList.Catalog)
                        ord = 1
                        # Obtener todas las dimensiones es decir tablas como primer nivel de jerarquia
                        for tables in self.executor.get_all_tables_names(
                            ignore_fact=True
                        ):
                            with xml.row:
                                xml.CATALOG_NAME(self.selected_cube)
                                xml.CUBE_NAME(self.selected_cube)
                                xml.DIMENSION_NAME(tables)
                                xml.DIMENSION_UNIQUE_NAME(f'[{tables}]')
                                xml.DIMENSION_CAPTION(tables)
                                xml.DIMENSION_ORDINAL(str(ord))
                                xml.DIMENSION_TYPE("3")
                                xml.DIMENSION_CARDINALITY("0") 
                                xml.DEFAULT_HIERARCHY(f'[{tables}].[{tables}]')
                                xml.IS_VIRTUAL("false")
                                xml.IS_READWRITE("false")
                                xml.DIMENSION_UNIQUE_SETTINGS("1")
                                xml.DIMENSION_IS_VISIBLE("true")
                            ord += 1

                        # Schema para medidas si no existen datos
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_cube)
                            xml.CUBE_NAME(self.selected_cube)
                            xml.DIMENSION_NAME("Measures")
                            xml.DIMENSION_UNIQUE_NAME("[Measures]")
                            xml.DIMENSION_CAPTION("Measures")
                            xml.DIMENSION_ORDINAL(str(ord))
                            xml.DIMENSION_TYPE("2")
                            xml.DIMENSION_CARDINALITY("0")
                            xml.DEFAULT_HIERARCHY("[Measures]")
                            xml.IS_VIRTUAL("false")
                            xml.IS_READWRITE("false")
                            xml.DIMENSION_UNIQUE_SETTINGS("1")
                            xml.DIMENSION_IS_VISIBLE("true")

        return str(xml)

    def mdschema_hierarchies_response(self, request):
        '''
            @Parametros
                request: Object request discover
            @Retorna
                xml: Object xml
        '''
        # Construccion del xml
        xml = xmlwitch.Builder()
        with xml["return"]:
            # Cabeceras necesarias para root
            with xml.root(
                xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                **{
                    "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                    "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                },
            ):
                # Obtener schema determinado para este discover request
                xml.write(mdschema_hierarchies_xsd)

                if request.Restrictions.RestrictionList:
                    if (
                        request.Restrictions.RestrictionList.CUBE_NAME
                        == self.selected_cube
                        and request.Properties.PropertyList.Catalog is not None
                    ):

                        # Actualizar apuntador a cubo
                        self.change_cube(request.Properties.PropertyList.Catalog)

                        for table_name in self.executor.get_all_tables_names():
                            if table_name == self.executor.facts:
                                continue

                            with xml.row:
                                xml.CATALOG_NAME(self.selected_cube)
                                xml.CUBE_NAME(self.selected_cube)
                                xml.DIMENSION_UNIQUE_NAME(f'[{table_name}]')
                                xml.HIERARCHY_NAME(table_name)
                                xml.HIERARCHY_UNIQUE_NAME(f'[{table_name}].[{table_name}]')
                                xml.HIERARCHY_CAPTION(table_name)
                                xml.DIMENSION_TYPE("3")
                                xml.HIERARCHY_CARDINALITY("0") 
                                xml.STRUCTURE("0")
                                xml.IS_VIRTUAL("false")
                                xml.IS_READWRITE("false")
                                xml.DIMENSION_UNIQUE_SETTINGS("1")
                                xml.DIMENSION_IS_VISIBLE("true")
                                xml.HIERARCHY_ORDINAL("1")
                                xml.DIMENSION_IS_SHARED("true")
                                xml.HIERARCHY_IS_VISIBLE("true")
                                xml.HIERARCHY_ORIGIN("1")
                                xml.INSTANCE_SELECTION("0")

                        # Schema para medidas si no existen datos
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_cube)
                            xml.CUBE_NAME(self.selected_cube)
                            xml.DIMENSION_UNIQUE_NAME("[Measures]")
                            xml.HIERARCHY_NAME("Measures")
                            xml.HIERARCHY_UNIQUE_NAME("[Measures]")
                            xml.HIERARCHY_CAPTION("Measures")
                            xml.DIMENSION_TYPE("2")
                            xml.HIERARCHY_CARDINALITY("0")
                            xml.DEFAULT_MEMBER(f"[Measures].[{self.executor.measures[0]}]")
                            xml.STRUCTURE("0")
                            xml.IS_VIRTUAL("false")
                            xml.IS_READWRITE("false")
                            xml.DIMENSION_UNIQUE_SETTINGS("1")
                            xml.DIMENSION_IS_VISIBLE("true")
                            xml.HIERARCHY_ORDINAL("1")
                            xml.DIMENSION_IS_SHARED("true")
                            xml.HIERARCHY_IS_VISIBLE("true")
                            xml.HIERARCHY_ORIGIN("1")
                            xml.INSTANCE_SELECTION("0")

        return str(xml)

    def mdschema_levels_response(self, request):
        '''
            @Parametros
                request: Object request discover
            @Retorna
                xml: Object xml
        '''
        # Construccion del xml
        xml = xmlwitch.Builder()

        with xml["return"]:
            # Cabeceras necesarias para root
            with xml.root(
                xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                **{
                    "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                    "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                },
            ):
                # Obtener schema determinado para este discover request
                xml.write(mdschema_levels_xsd)

                if request.Restrictions.RestrictionList:
                    if (
                        request.Restrictions.RestrictionList.CUBE_NAME
                        == self.selected_cube
                        and request.Properties.PropertyList.Catalog is not None
                    ):

                        # Actualizar apuntador a cubo
                        self.change_cube(request.Properties.PropertyList.Catalog)

                        for tables in self.executor.get_all_tables_names(
                            ignore_fact=True
                        ):
                            index = 0
                            for col in self.executor.cube_instance.get_dimensions_from_table(tables):
                                with xml.row:
                                    xml.CATALOG_NAME(self.selected_cube)
                                    xml.CUBE_NAME(self.selected_cube)
                                    xml.DIMENSION_UNIQUE_NAME(f'[{tables}]')
                                    xml.HIERARCHY_UNIQUE_NAME(f'[{tables}].[{tables}]')
                                    xml.LEVEL_NAME(str(col))
                                    xml.LEVEL_UNIQUE_NAME(f'[{tables}].[{tables}].[{col}]')
                                    xml.LEVEL_CAPTION(str(col))
                                    xml.LEVEL_NUMBER(str(index))
                                    xml.LEVEL_CARDINALITY("0")
                                    xml.LEVEL_TYPE("0")
                                    xml.CUSTOM_ROLLUP_SETTINGS("0")
                                    xml.LEVEL_UNIQUE_SETTINGS("0")
                                    xml.LEVEL_IS_VISIBLE("true")
                                    xml.LEVEL_DBTYPE("130")
                                    xml.LEVEL_KEY_CARDINALITY("1")
                                    xml.LEVEL_ORIGIN("2")
                                    xml.LEVEL_IS_FLAT("true")
                                index += 1

                        # Schema para medidas si no existen datos
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_cube)
                            xml.CUBE_NAME(self.selected_cube)
                            xml.DIMENSION_UNIQUE_NAME("[Measures]")
                            xml.HIERARCHY_UNIQUE_NAME("[Measures]")
                            xml.LEVEL_NAME("MeasuresLevel")
                            xml.LEVEL_UNIQUE_NAME("[Measures]")
                            xml.LEVEL_CAPTION("MeasuresLevel")
                            xml.LEVEL_NUMBER("0")
                            xml.LEVEL_CARDINALITY("0")
                            xml.LEVEL_TYPE("0")
                            xml.CUSTOM_ROLLUP_SETTINGS("0")
                            xml.LEVEL_UNIQUE_SETTINGS("0")
                            xml.LEVEL_IS_VISIBLE("true")
                            xml.LEVEL_DBTYPE("130")
                            xml.LEVEL_KEY_CARDINALITY("1")
                            xml.LEVEL_ORIGIN("1")
                            xml.LEVEL_IS_FLAT("true")

        return str(xml)

    