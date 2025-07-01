# KylinSOAPConnector

**KylinSOAPConnector** es un conector personalizado que permite la integración entre **Microsoft Excel** y **Apache Kylin 5.x** mediante el protocolo **XMLA** (SOAP). El sistema actúa como un puente entre las consultas MDX enviadas por Excel y la API REST de Kylin, permitiendo explorar y consultar cubos OLAP desde Excel sin necesidad de Mondrian u otras herramientas.

---

## 🚀 Características

- Conexión directa entre Excel y Apache Kylin usando XMLA.
- Traducción automática de consultas MDX a llamadas REST.
- Respuesta XMLA compatible con Excel.
- Soporte inicial para `DISCOVER_PROPERTIES`, `DISCOVER_DATASOURCES` y `EXECUTE`.

---

## ⚙️ Requisitos

- Python 3.9
- Apache Kylin 5.x en funcionamiento y accesible vía API REST.
- Microsoft Excel con capacidad para conectarse a servicios XMLA.

---

## 📦 Instalación

> **⚠️ Es obligatorio utilizar un entorno virtual para evitar conflictos con librerías del sistema.**

1. **Clona el repositorio:**

```bash
git clone https://github.com/tuusuario/KylinSOAPConnector.git
cd KylinSOAPConnector
```
2. **Crea entorno virtual**
```bash
python3.9 -m venv .venv
# En Windows: .venv\Scripts\activate
source .venv/bin/activate
```
3. **Instala las dependencias desde requirements.txt**
```bash
pip install -r requirements.txt
```

4. **Ejecuta el conector**
```bash
python app.py
```
---
## 🐛 Bug conocido en Spyne
1. **Ruta del bug**
```bash
<entorno_virtual>/.venv/lib/python3.9/site-packages/spyne/server/wsgi.py
```

2. **Como deberia estar el constructor**
```bash
def __init__(self, parent, transport, req_env, content_type):
    super(WsgiTransportContext, self).__init__(parent, transport, req_env, content_type)
    self.req_env = req_env  # Necesario para evitar errores de entorno con WSGI
```

