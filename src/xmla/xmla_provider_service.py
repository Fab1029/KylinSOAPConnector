import base64
from spyne import AnyXml, ServiceBase, rpc
from spyne.error import InvalidCredentialsError
from olapy.core.services.xmla_lib import XmlaProviderLib
from olapy.core.services.models import DiscoverRequest, ExecuteRequest, Session


class XmlaProviderService(ServiceBase, XmlaProviderLib):
    
    @rpc(DiscoverRequest, _returns=AnyXml, _body_style="bare", _out_header=Session, _throws=InvalidCredentialsError)
    def Discover(ctx, request):
        '''
            @Parametros
                ctx: Request obtenida por el servidor wsgi
                request: String request de excel
            @Retorna
                method: Ejecuccion del metodo discover
        '''      
        # Inicializar configuracion del handler discover
        discover_request_hanlder = ctx.app.config["discover_request_hanlder"]
        ctx.out_header = Session(SessionId=str(discover_request_hanlder.session_id))
        
        # Obtener configuracion si existe de cubo, esta parte no es necesaria
        config_parser = discover_request_hanlder.executor.cube_config

        # En esta seccion se realiza el auth del usuario de 
        # Kylin ademas la liberia tiene un bug el cual hay 
        # que cargar desde aqui la variable discover_request_hanlder.cubes
        if not XmlaProviderService.auth_against_kylin(
            discover_request_hanlder.executor.kylin_client,
            ctx.transport.req_env.get("HTTP_AUTHORIZATION")
        ): 
            raise InvalidCredentialsError(
                fault_string="You do not have permission to access this resource"
            ) 
        
        else:
            discover_request_hanlder.executor.load_cubes()
            discover_request_hanlder.cubes = discover_request_hanlder.executor.get_cubes_names()


        # Esta parte dejarla para autenticacion sin necesidad de usuarios y contraseña
        # autenticacion basica de windows
        if (
            config_parser
            and config_parser["xmla_authentication"]
            and ctx.transport.req_env["QUERY_STRING"] != "admin"
        ):
            raise InvalidCredentialsError(
                fault_string="You do not have permission to access this resource"
            )

        method_name = request.RequestType.lower() + "_response"
        method = getattr(discover_request_hanlder, method_name)

        if request.RequestType == "DISCOVER_DATASOURCES":
            return method()

        return method(request)


    @rpc(ExecuteRequest, _returns=AnyXml, _body_style="bare", _out_header=Session)
    def Execute(ctx, request):
        '''
            @Parametros
                ctx: Request obtenida por el servidor wsgi
                request: String request de excel
            @Retorna
                method: Ejecuccion del metodo discover
        ''' 
        # Usar el mismo session id para que excel no pierda consistencia de request y execute 
        ctx.out_header = Session(
            SessionId=str(ctx.app.config["discover_request_hanlder"].session_id)
        )
        # same executor instance as the discovery (not reloading the cube another time)
        mdx_query = request.Command.Statement.encode().decode("utf8")
        execute_request_hanlder = ctx.app.config["execute_request_hanlder"]

        # Jeraquia
        if all(
            key in mdx_query
            for key in ["WITH MEMBER", "strtomember", "[Measures].[XL_SD0]"]
        ):
            convert2formulas = True
        else:
            convert2formulas = False
        
        if (
            request.Properties
            and request.Properties.PropertyList.Catalog
            and not execute_request_hanlder.executor.cube
        ):
            # Cargar cubo con data
            execute_request_hanlder.executor.load_cube(
                request.Properties.PropertyList.Catalog
            )

        execute_request_hanlder.execute_mdx_query(mdx_query, convert2formulas)
        return execute_request_hanlder.generate_response()


    @staticmethod
    def auth_against_kylin(kylin_client, authorization):
        '''
            @Parametros
                kylin_client: cliente kylin
                authorization: HTTP autorization que vendria con credenciales Basic
            @Retorna
                True | False: Pudo autenticar con servicio Kylin
        ''' 

        # Si no obtiene autenticacion BASIC
        # devolver credenciales de None
        if not authorization.startswith("Basic "):
            return None, None
        
        auth_decoded = base64.b64decode(authorization.split(" ")[1]).decode("utf-8")
        username, password = auth_decoded.split(":", 1)
        
        return kylin_client.authentication(username, password)
        