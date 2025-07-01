from spyne import Fault
from spyne.const.http import HTTP_200
from spyne.const.http import HTTP_401
from spyne.protocol.soap import Soap11
from spyne.server.http import HttpTransportContext

class XmlaSoap11(Soap11):

    def create_in_document(self, ctx, charset=None):
        '''
            @Parametros
                ctx: Request obtenida por el servidor wsgi
                charset: Codificación de caracteres utilizada (opcional)
            @Retorna
                Document
        '''
        if isinstance(ctx.transport, HttpTransportContext):
            http_verb = ctx.transport.get_request_method()
            if http_verb == "OPTIONS":
                ctx.transport.resp_headers["allow"] = "POST, OPTIONS"
                ctx.transport.respond(HTTP_200)
                raise Fault("")
            
            # Solicitar basic auth
            auth = ctx.transport.req_env.get("HTTP_AUTHORIZATION")
            if not auth:
                ctx.transport.resp_headers["WWW-Authenticate"] = 'Basic realm="XMLA"'
                ctx.transport.respond(HTTP_401)
                raise Fault("Unauthorized")

        return Soap11.create_in_document(self, ctx, charset)