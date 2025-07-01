import imp
import sys
import logging
from spyne import Application
from xmla.xmla_soap import XmlaSoap11
from kylin.kylin_client import KylinClient
from spyne.server.wsgi import WsgiApplication
from wsgiref.simple_server import make_server
from mdx.kylin_mdx_engine import KylinMdxEngine
from xmla.xmla_provider_service import XmlaProviderService
from xmla.xmla_discover_req_handler_kylin import XmlaDiscoverReqHandlerKylin
from olapy.core.services.xmla_execute_request_handler import XmlaExecuteReqHandler

def get_mdx_engine():
    '''
        @Retorna
            KylinMdxEngine: instancia del executor MDX
    '''
    # Se instancia el engine con el client de Kylin
    executor = KylinMdxEngine(kylin_client= KylinClient())
    return executor

def get_spyne_app(discover_request_hanlder, execute_request_hanlder):
    '''
        @Parametros
            discover_request_handler: instancia de XmlaDiscoverReqHandlerKylin
            execute_request_handler: instancia de XmlaExecuteReqHandler
        @Retorna
            Application: instancia server wsgi
    '''
    return Application(
        [XmlaProviderService],
        "urn:schemas-microsoft-com:xml-analysis",
        in_protocol=XmlaSoap11(validator="soft"),
        out_protocol=XmlaSoap11(validator="soft"),
        
        # Configuraciones necesarias de ejecutores de request
        config={
            "discover_request_hanlder": discover_request_hanlder,
            "execute_request_hanlder": execute_request_hanlder,
        },
    )

def get_wsgi_application(mdx_engine):
    '''
        @Parametros:
            mdx_engine: instancia del executor MDX
        @Retorna: Wsgi Application
    '''
    discover_request_hanlder = XmlaDiscoverReqHandlerKylin(mdx_engine)
    execute_request_hanlder = XmlaExecuteReqHandler(mdx_engine)
    application = get_spyne_app(discover_request_hanlder, execute_request_hanlder)

    return WsgiApplication(application)



def runserver (host, port):
    '''
        @Parametros:
            host: configuracion IP
            port: puerto
    '''
    try:
        imp.reload(sys)
        # reload(sys)  # Reload is a hack
        sys.setdefaultencoding("UTF8")
    except Exception:
        pass
    
    # Instanciar motor de mdx y wsgi aplicacion
    mdx_engine = get_mdx_engine ()
    wsgi_application = get_wsgi_application(mdx_engine)

    # Configurar debug basico
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("spyne.protocol.xml").setLevel(logging.DEBUG)
    
    server = make_server(host, port, wsgi_application)
    server.serve_forever()

if __name__ == '__main__':
    port = 8000
    host = '0.0.0.0'
    
    runserver(host= host, port= port)
    
