# +==========================================================+
# |DATOS DEL PROGRAMA                                        |
# +==========================================================+
# |                                                          |
# |Proyecto:       Web Service CENTRAL-SAP                   |
# |Archivo:        WebService.py                             |
# |Descripcion:    Programa Principal de la Aplicacion       |
# |Empresa:        SICA Technologies                         |
# |Desarrollador:  Ing. Agustin Noguez Salazar               |
# |                                                          |
# +==========================================================+
# |CONTROL DE VERSIONES                                      |
# +==========================================================+
# |                                                          |
# |Version:       2.0.0                                      |
# |Fecha:         27-Septiembre-2016                         |
# +----------------------------------------------------------+

#=====> LIBRERIAS <=====#

import datetime                     # Libreria para manejo de Hora y Fecha
import logging                      # Libreria para manejo de LOG
import os                           # Libreria para manejo de carpetas
import pypyodbc                     # Libreria para manejo de Base de Datos
import requests                     # Libreria para manejo de request SOAP
import sys                          # Libreria para manejo de Sistema Operativo
import time                         # Libreria para manejo de Retardos
import xml.etree.ElementTree as ET  # Libreria para manejo de lectura de XML

#=====> VARIABLES GLOBALES <=====#

# -Create_Log_Folder-

log_folder = ''                     # string

# -Create_Year_Folder-

year_folder = ''                     # string

# -Create_Month_Folder-

month_folder = ''                    # string

# -Connection_Database-

connection = ''                      # string

# -First_Query-

cursor = ''                          # string

# -Number_Rows-

object_rows = []                     # Object
number_rows = 0                      # int

# -Process_Rows-

int_ID_TRANSACCION = 0               # int
str_CANAL_DE_DISTRIBUCION = ''       # char(2)
int_CLIENTE = 0                      # int
float_TOTAL_DE_INGRESO = 0.0         # float
str_FECHA_CONTABLE = ''              # char(15)
int_CANTIDAD_DE_SERVICIOS = 0        # int
str_REGISTRO_MENSAJE = ''            # char(200)
str_FECHA_RECEPCION = ''             # char(19)
int_ID_USUARIO_INGRESO_SISTEMA = 0   # int
str_ORGANIZACION_VENTAS = ''         # char(6)
str_SECTOR_EMPRESARIAL = ''          # char(3)
str_CAMPO_ESPECIAL = ''              # char(50)
str_MATERIAL_SAP = ''                # char(10)
str_CENTRO_BENEFICIO = ''            # char(15)
int_ACK_INTERNO = 0                  # int
str_FOLIO_RECEPCION = ''             # char(10)

str_ID_TRANSACCION = ''              # str
str_CLIENTE = ''                     # str
str_TOTAL_DE_INGRESO = ''            # str
str_CANTIDAD_DE_SERVICIOS = ''       # str

BSTNK = ''                           # str
VTWEG = ''                           # str
KUNNR = ''                           # str
NETWR = ''                           # str
BSTDK = ''                           # str
KWMENG = ''                          # str
VKORG = ''                           # str
SPART = ''                           # str
ZUONR = ''                           # str
MATNR = ''                           # str
PRCTR = ''                           # str
XBLNR = ''                           # str

int_counter = 0                      # int
str_counter = ''                     # string

# -Clear_Spaces-

soap_values = ''                     # string

# -SAP_connection-

environment = 'production'
body = ''
response = ''

# -Parse_XML-

tree = ''
DocSap = ''

# -Correct_Incorrect_Response-

insert_values = ''

#=====> FUNCIONES <=====#

#Imprimir Titulo de Funcion


def Print_Title(title_value):
    if title_value == 0:
        print("Start the Script")
    elif title_value == 1:
        print("Connection_Database")
    elif title_value == 2:
        print("First_Query")
    elif title_value == 3:
        print("Number_Rows")
    elif title_value == 4:
        print("Process_Rows")
    elif title_value == 5:
        print("Clear_Spaces")
    elif title_value == 6:
        print("SOAP_Connection")
    elif title_value == 7:
        print("Parse_XML")
    elif title_value == 8:
        print("Response Correct")
    elif title_value == 9:
        print("Response Incorrect")
    elif title_value == 10:
        print("Insert Values")

#Imprimir Acierto


def Handler_Info(info_value):
    if info_value == 0:
        print("")
        logging.info('Start the Script')
    elif info_value == 1:
        print("Database Connection OK\n")
        logging.info("Database Connection")
    elif info_value == 2:
        print("First Query OK\n")
        logging.info("First Query")
    elif info_value == 4:
        print("Process Rows OK\n")
        logging.info("Process Rows")
    elif info_value == 5:
        print("Clear Spaces OK\n")
        logging.info("Row: " + str_counter + "\n\n" + soap_values + "\n")
    elif info_value == 6:
        print("SOAP Connection OK\n")
        logging.info("SOAP Connection\n\n" + "Request:\n\n" + body + "\n\n" +
                     "Response:\n\n" + response + "\n")
    elif info_value == 7:
        print("\nParse XML OK\n")
        logging.info("Parse XML")
    elif info_value == 8:
        print("Response Correct OK\n")
        logging.info("Response Correct")
    elif info_value == 9:
        print("Response Incorrect OK\n")
        logging.info("Response Incorrect")
    elif info_value == 10:
        print("Insert Values OK\n")
        logging.info("Insert Values")

#Imprimir Error


def Handler_Error(error_value):
    if error_value == 1:
        print("Database Connection FAIL\n")
        logging.error("Database Connection")
    elif error_value == 2:
        print("First Query FAIL\n")
        logging.error("First Query")
        cursor.close()
        connection.close()
    elif error_value == 4:
        print("Process Rows FAIL\n")
        logging.error("Process Rows")
        cursor.close()
        connection.close()
    elif error_value == 5:
        print("Clear Spaces FAIL\n")
        logging.error("Clear Spaces")
        cursor.close()
        connection.close()
    elif error_value == 6:
        print("SOAP Connection FAIL\n")
        logging.error("SOAP Connection")
        cursor.close()
        connection.close()
    elif error_value == 7:
        print("Parse XML FAIL\n")
        logging.error("Parse XML")
        cursor.close()
        connection.close()
    elif error_value == 8:
        print("Response Correct FAIL\n")
        logging.error("Response Correct")
        cursor.close()
        connection.close()
    elif error_value == 9:
        print("Response Incorrect FAIL\n")
        logging.error("Response Incorrect")
        cursor.close()
        connection.close()
    elif error_value == 10:
        print("Insert Values FAIL\n")
        logging.error("Insert Values")
        cursor.close()
        connection.close()
    print("Stop the Script")
    Get_DateTime()
    logging.info('Stop the Script')
    sys.exit(1)

#Imprimir Informacion


def Handler_Debug(debug_value):
    if debug_value == 0:
        print("Number Rows OK\n")
        logging.debug("Rows For Processing: " + str(number_rows))
        cursor.close()
        connection.close()
        Get_DateTime()
        logging.info('Stop the Script')
        sys.exit(1)
    if debug_value == 1:
        print("Number Rows OK\n")
        logging.debug("Rows For Processing: " + str(number_rows))

#Buscar si la carpeta LOG existe


def Create_Log_Folder():

    #Variables globales usadas en esta funcion
    global log_folder

    #Process
    current_folder = os.getcwd()
    log_folder = os.path.join(current_folder, "LOGS")
    print(log_folder)
    log_folder_exists = os.path.exists(log_folder)
    if log_folder_exists is False:
        os.mkdir(log_folder)

#Buscar si la carpeta del año existe


def Create_Year_Folder():

    #Variables globales usadas en esta funcion
    global year_folder

    #Process
    year_value = datetime.datetime.now().strftime('%Y')
    year_folder = os.path.join(log_folder, year_value)
    print(year_folder)
    year_folder_exists = os.path.exists(year_folder)
    if year_folder_exists is False:
        os.mkdir(year_folder)

#Buscar si la carpeta del mes existe


def Create_Month_Folder():

    #Variables globales usadas en esta funcion
    global month_folder

    #Process
    str_month_value = datetime.datetime.now().strftime('%m')
    int_month_value = int(str_month_value)
    if int_month_value == 1:
        month_value = 'Enero'
    elif int_month_value == 2:
        month_value = 'Febrero'
    elif int_month_value == 3:
        month_value = 'Marzo'
    elif int_month_value == 4:
        month_value = 'Abril'
    elif int_month_value == 5:
        month_value = 'Mayo'
    elif int_month_value == 6:
        month_value = 'Junio'
    elif int_month_value == 7:
        month_value = 'Julio'
    elif int_month_value == 8:
        month_value = 'Agosto'
    elif int_month_value == 9:
        month_value = 'Septiembre'
    elif int_month_value == 10:
        month_value = 'Octubre'
    elif int_month_value == 11:
        month_value = 'Noviembre'
    elif int_month_value == 12:
        month_value = 'Diciembre'
    month_folder = os.path.join(year_folder, month_value)
    print(month_folder)
    month_folder_exists = os.path.exists(month_folder)
    if month_folder_exists is False:
        os.mkdir(month_folder)

#Crear el archivo log


def Create_Log_File():

    #Process
    log_file = datetime.datetime.now().strftime('%d-%m-%Y-%H%M%S.log')
    name_log_file = os.path.join(month_folder, log_file)
    print(name_log_file + '\n')
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%H:%M:%S',
                        filename=name_log_file,
                        filemode='w'
                        )
    Print_Title(0)
    Get_DateTime()
    Handler_Info(0)

#Obtener Fecha y Hora


def Get_DateTime():
    timestamp = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    print(timestamp)

#Conexion a SQL Server


def Connection_Database():

    #Variables globales usadas en esta funcion
    global connection

    #Funciones
    Print_Title(1)
    Get_DateTime()

    #Handler
    try:
#        connection = pypyodbc.connect('Driver={SQL Server};'
#                                      'Server=SQL-SERVER\SQLEXPRESS;'
#                                      'Database=CETRAM;'
#                                      'uid=sa;pwd=Cetram21376')
        connection = pypyodbc.connect('Driver={SQL Server};'
                                      'Server=CETRAMCENTRAL;'
                                      'Database=CENTRAL;'
                                      'uid=sa;pwd=Cetram1376')
        Handler_Info(1)
    except:
        Handler_Error(1)

#Consulta 1 a Base de Datos


def First_Query():

    #Variables globales usadas en esta funcion
    global cursor

    #Funciones
    Print_Title(2)
    Get_DateTime()

    #Handler
    try:
        cursor = connection.cursor()
        first_query = ("SELECT * FROM CUDI_ACTM WHERE FOLIO_RECEPCION IS NULL")
        cursor.execute(first_query)
        Handler_Info(2)
    except:
        Handler_Error(2)

#Obtener el numero de valores a procesar


def Number_Rows():

    #Variables globales usadas en esta funcion
    global object_rows
    global number_rows

    #Funciones
    Print_Title(3)
    Get_DateTime()

    #Handler
    object_rows = cursor.fetchall()
    number_rows = len(object_rows)
    if number_rows > 0:
        Handler_Debug(1)
    else:
        Handler_Debug(0)

#Procesar las filas de Valores Obtenidos


def Process_Rows():

    #Variables globales usadas en esta funcion
    global int_ID_TRANSACCION
    global str_CANAL_DE_DISTRIBUCION
    global int_CLIENTE
    global float_TOTAL_DE_INGRESO
    global str_FECHA_CONTABLE
    global int_CANTIDAD_DE_SERVICIOS
    global str_REGISTRO_MENSAJE
    global str_FECHA_RECEPCION
    global int_ID_USUARIO_INGRESO_SISTEMA
    global str_ORGANIZACION_VENTAS
    global str_SECTOR_EMPRESARIAL
    global str_CAMPO_ESPECIAL
    global str_MATERIAL_SAP
    global str_CENTRO_BENEFICIO
    global int_ACK_INTERNO
    global str_FOLIO_RECEPCION

    global str_ID_TRANSACCION
    global str_CLIENTE
    global str_TOTAL_DE_INGRESO
    global str_CANTIDAD_DE_SERVICIOS

    global int_counter
    global str_counter

    #Funciones
    Print_Title(4)
    Get_DateTime()

    #Valores de la consulta
    values_row = object_rows[int_counter]

    #Leer cada valor
    int_ID_TRANSACCION = values_row[0]
    str_CANAL_DE_DISTRIBUCION = values_row[1]
    int_CLIENTE = values_row[2]
    float_TOTAL_DE_INGRESO = values_row[3]
    str_FECHA_CONTABLE = values_row[4]
    int_CANTIDAD_DE_SERVICIOS = values_row[5]
    str_REGISTRO_MENSAJE = values_row[6]                # No se ocupa para SOAP
    str_FECHA_RECEPCION = values_row[7]                 # No se ocupa para SOAP
    int_ID_USUARIO_INGRESO_SISTEMA = values_row[8]      # No se ocupa para SOAP
    str_ORGANIZACION_VENTAS = values_row[9]
    str_SECTOR_EMPRESARIAL = values_row[10]
    str_CAMPO_ESPECIAL = values_row[11]
    str_MATERIAL_SAP = values_row[12]
    str_CENTRO_BENEFICIO = values_row[13]
    int_ACK_INTERNO = values_row[14]                    # No se ocupa para SOAP
    str_FOLIO_RECEPCION = values_row[15]                # No se ocupa para SOAP

    #Convertir los int en str

    str_ID_TRANSACCION = str(int_ID_TRANSACCION)
    str_CLIENTE = str(int_CLIENTE)
    float_TOTAL_DE_INGRESO_SIN_IVA = (float_TOTAL_DE_INGRESO) / (1.16)
    str_TOTAL_DE_INGRESO = str(float("%.2f" % float_TOTAL_DE_INGRESO_SIN_IVA))
    str_CANTIDAD_DE_SERVICIOS = str(int_CANTIDAD_DE_SERVICIOS)

    int_counter = int_counter + 1
    str_counter = str(int_counter)

    Handler_Info(4)

#Quitar espacios a los valores


def Clear_Spaces():

    #Variables globales usadas en esta funcion
    global BSTNK
    global VTWEG
    global KUNNR
    global NETWR
    global BSTDK
    global KWMENG
    global VKORG
    global SPART
    global ZUONR
    global MATNR
    global PRCTR
    global XBLNR

    global soap_values

    #Funciones
    Print_Title(5)
    Get_DateTime()

    BSTNK = str_ID_TRANSACCION.replace(' ', '')
    VTWEG = str_CANAL_DE_DISTRIBUCION.replace(' ', '')
    KUNNR = str_CLIENTE.replace(' ', '')
    NETWR = str_TOTAL_DE_INGRESO.replace(' ', '')
    BSTDK = str_FECHA_CONTABLE.replace(' ', '')
    KWMENG = str_CANTIDAD_DE_SERVICIOS.replace(' ', '')
    VKORG = str_ORGANIZACION_VENTAS.replace(' ', '')
    SPART = str_SECTOR_EMPRESARIAL.replace(' ', '')
    ZUONR = str_CAMPO_ESPECIAL.replace(' ', '')
    MATNR = str_MATERIAL_SAP.replace(' ', '')
    PRCTR = str_CENTRO_BENEFICIO.replace(' ', '')
    XBLNR = str_ID_TRANSACCION.replace(' ', '')

    soap_values = BSTNK + "\n" + VTWEG + "\n" + KUNNR + "\n" +\
                  NETWR + "\n" + BSTDK + "\n" + KWMENG + "\n" +\
                  VKORG + "\n" + SPART + "\n" + ZUONR + "\n" +\
                  MATNR + "\n" + PRCTR + "\n" + XBLNR

    Handler_Info(5)

#Realizar la conexion al servidor SAP


def SAP_Connection():

    #Variables globales usadas en esta funcion
    global environment
    global body
    global response

    #Funciones
    Print_Title(6)
    Get_DateTime()

# Servidor SAP IAMSA

    if environment == 'development':
        url = 'http://192.168.151.63:8005/sap/bc/srt/rfc/sap/' \
              'z_sd_interfase_ventas/700/z_sd_interfase_ventas/binding'
        headers = {
                  'content-type': "text/xml",
                  'authorization': "Basic QzA0LUlOVDAxOlRyQG5zZmVyMDI=",
                  'cache-control': "no-cache"
                  }
    elif environment == 'production':
        url = 'http://192.168.151.37:8002/sap/bc/srt/rfc/sap/' \
              'z_sd_interfase_ventas/700/z_sd_interfase_ventas/binding'
        headers = {
                  'content-type': "text/xml",
                  'authorization': "Basic QzA0LUlOVDAxOlRyQG5zZmVyMDE=",
                  'cache-control': "no-cache",
                  }
    else:
        url = 'Error'
        headers = 'Error'

    body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" \
           "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap" \
           "/envelope/\" xmlns:urn=\"urn:sap-com:document:sap:soap:functions:" \
           "mc-style\">\n" \
           "   <soapenv:Header/>\n" \
           "   <soapenv:Body>\n" \
           "      <urn:ZSdInterfaseVentas>\n" \
           "         <Vkorg>" + VKORG + "</Vkorg>\n" \
           "         <Vtweg>" + VTWEG + "</Vtweg>\n" \
           "         <Spart>" + SPART + "</Spart>\n" \
           "         <Kunnr>" + KUNNR + "</Kunnr>\n" \
           "         <Bstnk>" + BSTNK + "</Bstnk>\n" \
           "         <Bstdk>" + BSTDK + "</Bstdk>\n" \
           "         <Xblnr>" + XBLNR + "</Xblnr>\n" \
           "         <Zuonr>" + ZUONR + "</Zuonr>\n" \
           "         <EtDetalle>\n" \
           "            <item>\n" \
           "               <Matnr>" + MATNR + "</Matnr>\n" \
           "               <Kwmeng>" + KWMENG + "</Kwmeng>\n" \
           "               <Netwr>" + NETWR + "</Netwr>\n" \
           "               <Prctr>" + PRCTR + "</Prctr>\n" \
           "            </item>\n" \
           "         </EtDetalle>\n" \
           "         <EtError>\n" \
           "            <item>\n" \
           "               <Tdformat>?</Tdformat>\n" \
           "               <Tdline>?</Tdline>\n" \
           "            </item>\n" \
           "         </EtError>\n" \
           "      </urn:ZSdInterfaseVentas>\n" \
           "   </soapenv:Body>\n" \
           "</soapenv:Envelope>"

    print("\nRequest:\n")
    print(body + "\n")

    try:
        response = requests.post(url, headers=headers, data=body).text
        print("Response:\n")
        print(response + "\n")
        Handler_Info(6)
    except:
        Handler_Error(6)

#Leer valores del XML


def Parse_XML():

    #Variables globales usadas en esta funcion
    global tree
    global DocSap

    #Funciones
    Print_Title(7)
    Get_DateTime()

    #Handler
    try:
        tree = ET.fromstring(response)
        print("\nDocSap:")
        DocSap = tree.find('.//DocSap').text
        print(DocSap)
        if DocSap is None:
            Handler_Info(7)
            Response_Incorrect()
        else:
            Handler_Info(7)
            Response_Correct()
    except:
        Handler_Error(7)

#Si el Documento generado es correcto


def Response_Correct():

    #Variables globales usadas en esta funcion
    global insert_values

    #Funciones
    Print_Title(8)
    Get_DateTime()

    #Handler

    try:
        #Asignar valores
        str_REGISTRO_MENSAJE = 'PROCESADO'
        str_format = '%d-%m-%Y %H:%M:%S'
        str_FECHA_RECEPCION = datetime.datetime.now().strftime(str_format)
        str_FOLIO_RECEPCION = DocSap
        str_ID_TRANSACCION = BSTNK

        #Se construye el Query de Insert
        query_r1 = "UPDATE CUDI_ACTM SET "
        query_r2 = "REGISTRO_MENSAJE=" + "'" + str_REGISTRO_MENSAJE + "', "
        query_r3 = "FECHA_RECEPCION=" + "'" + str_FECHA_RECEPCION + "', "
        query_r4 = "FOLIO_RECEPCION=" + "'" + str_FOLIO_RECEPCION + "' "
        query_r5 = "WHERE ID_TRANSACCION=" + "'" + str_ID_TRANSACCION + "'"
        insert_values = query_r1 + query_r2 + query_r3 + query_r4 + query_r5
        Handler_Info(8)
    except:
        Handler_Error(8)

#Si el Documento generado es incorrecto


def Response_Incorrect():

    #Variables globales usadas en esta funcion
    global insert_values

    #Funciones
    Print_Title(9)
    Get_DateTime()

    #Handler

    try:
        #Leer errores
        valores = tree.findall(".//Tdline")
        print("\nErrores:\n")
        str_errores = ''
        for errores in valores:
            str_errores += str(errores.text) + "|"
        str_errores = str_errores[:-1]
        print(str_errores)
        print("")

        #Asignar valores
        str_REGISTRO_MENSAJE = str_errores
        str_format = '%d-%m-%Y %H:%M:%S'
        str_FECHA_RECEPCION = datetime.datetime.now().strftime(str_format)
        str_ID_TRANSACCION = BSTNK

        query_r1 = "UPDATE CUDI_ACTM SET "
        query_r2 = "REGISTRO_MENSAJE=" + "'" + str_REGISTRO_MENSAJE + "', "
        query_r3 = "FECHA_RECEPCION=" + "'" + str_FECHA_RECEPCION + "' "
        query_r4 = "WHERE ID_TRANSACCION=" + "'" + str_ID_TRANSACCION + "'"
        insert_values = query_r1 + query_r2 + query_r3 + query_r4
        Handler_Info(9)
    except:
        Handler_Error(9)

#Insertar los valores en la Base de Datos


def Insert_Values():

    #Variables globales usadas en esta funcion
    #None

    #Funciones
    Print_Title(10)
    Get_DateTime()

    try:
        cursor = connection.cursor()
        print(insert_values)
        cursor.execute(insert_values)
        cursor.commit()
        Handler_Info(10)
    except:
        Handler_Error(10)

# Funcion Principal


def Main_Function():

    #Variables globales usadas en esta funcion
    global number_rows

    #Funciones
    Process_Rows()
    Clear_Spaces()
    SAP_Connection()
    Parse_XML()
    Insert_Values()

    #Process
    time.sleep(0.1)
    number_rows = number_rows - 1
    print(number_rows)
    print("")
    if number_rows > 0:
        Main_Function()
    else:
        Handler_Debug(1)

#=====> PROGRAMA PRINCIPAL <=====#

Create_Log_Folder()
Create_Year_Folder()
Create_Month_Folder()
Create_Log_File()
Connection_Database()
First_Query()
Number_Rows()
Main_Function()