"""
Código de Exportación de Datos Contables
Autor: Bertoli Maricle
Fecha de Creación: 05-05-2023
Descripción: Este script genera archivos de exportación de datos contables.
"""


import mysql.connector
from datetime import date, timedelta, datetime
import logging as log
import pandas as pd
import os
from config import delta, dbconfig, beginning_date


def execute_query(query):
    """
    Ejecuta una consulta SQL en la base de datos y devuelve los resultados en un DataFrame.

    Args:
        query (str): La consulta SQL a ejecutar.

    Returns:
        pd.DataFrame: Un DataFrame que contiene los resultados de la consulta.
    """

    cnx = mysql.connector.connect(
        host= dbconfig['host'],
        user=dbconfig['user'],
        password=dbconfig['password'],
        database=dbconfig['database'],
        port=dbconfig['port']
    )

    if (not cnx):
        log.error('conexion refused:', cnx)
        print('error')
        return False

    # df = pd.read_sql(query, con=cnx)

    mycursor = cnx.cursor()
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    cnx.close()

    df = pd.DataFrame(myresult)
    df = df.fillna('')

    return df


def generateCabecera(date, route):
    """
    Genera archivos de cabecera para exportación de datos contables.

    Args:
        date (str): La fecha para la cual se generan los archivos.
        route (str): La ruta donde se guardarán los archivos.

    Returns:
        bool: True si los archivos se generaron con éxito, False en caso contrario.
    """

    query_header = """Select f.tipocomprobante, f.tipo as 'letra', f.puntodeventa_id  as 'pv', f.numero, f.fecha, c.numero_cliente as 'cod_cliente',
    concat(p.apellido,' ', p.nombre) as 'razon_social', p.tipo as 'tipo_doc', l.provincia_id as 'provincia', p.condicion_afip as 'sit_iva', p.numero as 'CUIT','' as 'IngBRUTO',
    f.created_by  as 'vendedor','' AS 'ZONA','' AS 'COD_CLIENTE', f.formasdepago_id as 'codicion_venta', '' AS 'COD_causaemision', f.fecha as 'vencimiento',
    f.total as 'importe_total','' as 'cod_descuento','' as 'cod_descuento_fin','' as 'cod_descuento_pie','' as 'app_contable',tc.numero as 'tipocliente',
    concat(p.calle,'' ,p.numero)  as 'direccion',v.codigo_postal, l.nombre as 'localidad', '' as 'clasif2','' as 'mensaje', '' as 'anulado', 'N' as 'actualiza_stock', ti.descripcion as 'clasif_add_1 ',
    ti.descripcion as 'clasif_ad_2',tc.tipo as 'desc_tipocliente', '' as 'desc_zona','' as 'desc_vendedor', '' as 'comp_preparado',  '0.00' as  'taza_descuento1', '0.00' as  'taza_descuento2',  '0.00' as  'taza_descuento3',
    '0.00' as  'taza_descuento_fin',   '0.00' as  'taza_descuento_pie', 'f.cae', 'f.vto_cae', if(f.puntodeventa_id <6, 'S', '') as 'controlador', p.email, p.telefono, p.celular, '' as 'contacto',
    '' as 'tipo_operacion', '' as 'nro_cuota', '0.00' as 'importe_cuota', '' as 'comp_en_cuotas', '1  ' as 'moneda', '' as 'tipodecambio', '1.0000' as 'cotizacion', f.fecha as 'fechaentrega', '' as 'grupo', '' as 'py', '' as 'lista_precio',
    '' as 'entrega', ''as 'autorizacion'
    from
    factura f inner join cliente c on f.cliente_id= c.id
    inner join persona p on c.persona_id= p.id
    left join localidad l on p.localidad_id= l.id
    left join provincia as v on l.provincia_id = v.id
    left join tipocliente as tc on c.tipocliente_id=tc.id
    left join tipodeingreso as ti on f.tipodeingreso_id= ti.id
    where f.puntodeventa_id >1
    and f.fecha = '{s1}'""".format(s1=date)

    data = execute_query(query_header)
    # print(type(data))
    # print(data)
    # exit()
    

    file = open(route+"/VCABECERA.txt", 'w')
    if (not data.empty):
        lines = len(data)
 
        log.debug('found {} items in FACTURAS'.format(lines))

        for index, row in data.iterrows():

            line = ''
            # begin 63 fields
            # 1  Tipo comprobante 3 str : ‘FC’, ‘ND’, ‘NC’
            line += str('FC').ljust(3)
            # 2 letra 1 str
            line += str(row[1]).strip()
            # 3 punto vta str 4
            line += str(row[2]).rjust(4, '0')
            # 4 nro comprobante
            line += str(row[3]).rjust(8, '0')
            # 5 nro hasta
            line += str('').ljust(8)
            # 6 fecha comprobante str 8
            line += str(row[4]).replace('-', '')
            # 7codig cliente str 6
            numcliente= int(row[5]) if  row[5] !='' else 0
            line += str(numcliente).rjust(6, '0')
            # 8 razon social str  40
            line += str(row[6]).ljust(40) if len(row[6]) < 40 else row[6][0:40]
            # 9 tipo documento str 2
            line += str(row[7]).rjust(6)
            # 10 provincia str 3
            line += str(row[8]).rjust(3, '0')
            # 11 situación ante IVA
            line += str(row[9])
            # 12 cuit numerico 11
            line += str(row[10]).ljust(6)
            # 13 Nro ingresos brutos str 15
            line += str(row[11]).ljust(15)
            # 14 código de vendedor string 4
            line += str(row[12]).ljust(4)
            # 15 código de zona str 4
            line += str(row[13]).ljust(4)
            # 16 código de clas adicional del cliente str 4
            line += str(row[14]).ljust(4)
            # 17 cond de venta str 2
            line += str(row[15]).rjust(2, '0')
            # 18 codigo de causa emision str 4
            line += str(row[16]).ljust(4)
            # 19 fecha vencimiento str 8
            line += str(row[17]).replace('-', '')
            # 20 Importe total  comprobante str 16
            line += str(row[18]).ljust(16)
            # 21 código de descuento comercial str 2
            line += str(row[19]).ljust(2)
            # 22 código de descuento financiero str 2
            line += str(row[20]).ljust(2)
            # 23 cod descuento al piedel comp.  str  2
            line += str(row[21]).ljust(2)
            # 24 apertura contable  str 4
            line += str(row[22]).ljust(4)
            # 25 tipo de cliente str 4
            line += str(row[23]).ljust(4)
            # 26 direccion string 30
            line += str(row[24]).ljust(30)
            # 27  código postal   str 8
            line += str(row[25]).ljust(8)
            # 28 localidad  str  25
            line += str(row[26]).ljust(25)
            # 29 cod. clasificación adicional 1 str 4
            line += str(row[27]).ljust(4)
            # 30 mensaje str 40
            line += str(row[28]).ljust(40)
            # 31 comp anulado str 1
            line += str(row[29]).ljust(1)
            # 32 actualiza stock  str 1
            line += str(row[30]).ljust(1)
            # 33 desc de clasific adicional 1
            line += str(row[31]).ljust(1)
            # 34 desc clasif adicional 2
            line += str(row[32]).ljust(2)
            # 35 desc de tipo de cliente str 15
            line += str(row[33]).ljust(15)
            # 36 descripcion de zona string 15
            line += str(row[34]).ljust(15)
            # 37 descripcion del vendedor str 25
            line += str(row[35]).ljust(25)
            # 38 comprobante preparado string 1
            line += str(row[36]).ljust(1)
            # 39 tasa descuento comercial 1 str 8
            line += str(row[37]).ljust(8)
            # 40 tasa descuento comercial 2 str 8
            line += str(row[38]).ljust(8)
            # 41 tasa descuento comercial 3 str 8
            line += str(row[39]).ljust(8)
            # 42 tasa descuento financiero str 8
            line += str(row[40]).ljust(8)
            # 43 tasa descuento vari o al pie str 8
            line += str(row[41]).ljust(8)
            # 44 Nro de CAI 1 str 15
            line += str(row[42]).ljust(15)
            # 45 fecha vto de CAI str 8
            line += str(row[43]).replace('-', '')
            # 46 Controlador fiscal str 1
            line += str(row[44]).ljust(1)
            # 47 email cliente str 50
            line += str(row[45]).ljust(50)
            # 48 telefono cliente 1 str 30
            line += str(row[46]).ljust(30)
            # 49 fax cliente str 30
            line += str(row[47]).ljust(30)
            # 50 contacto/observaciones  str 50
            line += str(row[48]).ljust(50)
            # 51 Tipo de operacion str 4
            line += str(row[49]).ljust(4)
            # 52 Nro de cuota 1 str 3
            line += str(row[50]).ljust(3)
            # 53 importe cuota str 16
            line += str(row[51]).ljust(16)
            # 54 comprobante en cuotras str 1
            line += str(row[52]).ljust(1)
            # 55 moneda del comprobante str 3
            line += str(row[53]).ljust(3)
            # 56 tipo de cambio str 1
            line += str(row[54]).ljust(3)
            # 57 cotizacion str 8
            line += str(row[55]).ljust(8)
            # 58 fecha de entrega num 8
            line += str(row[56]).replace('-', '')
            # 59 grupo str 15
            line += str(row[57]).ljust(15)
            # 60 proyecto str 15
            line += str(row[58]).ljust(15)
            # 61  lista de precios str 3
            line += str(row[59]).ljust(3)
            # 62  lugar de entrega str 3
            line += str(row[60]).ljust(3)
            # 63 marca autorizacion  str 1
            line += str(row[61]).ljust(1)

            file.write(line + "\n")
            # file.write(''.join([str(val) for val in row])+ os.linesep)

        file.close()

        log.debug('%s lines registered' % (lines))
        return True
    else:
        log.debug('found 0 items in FACTURAS')

        file.close()
        return False


def generateItems(date, route):

    query_header = """SELECT f.tipocomprobante, f.tipo as 'letra', f.puntodeventa_id  as 'pv', f.numero, f.fecha, c.numero_cliente as 'cod_cliente',
	'C' as'tipo_item', concat(ta.id,' ', ta.descripcion) as 'cod_concepto_art',l.cantidad, '0.0' as 'cantidad_2', ta.descripcion as 'descripcion',
    precio,f.iva,'0.0'  as 'ivaNI', '0.0'  as 'importeIva','0.0'  as 'importeIvaNO', cantidad * precio as'total sin iva', '0.0'  as 'dto_comercial','0.0'  as 'dtoFinanciero','0.0'  as 'dtoAlPie',
    '0.0'  as 'importeIvaNO', '' as 'cod_concepto_no_grabado','0.0'  as 'importeIvaNOGravado', '1' as 'tipodeiva','' as 'cod_descuento_linea','0.0' as 'importe_descuento_linea', '' as 'deposito',
    '' as 'partida', '' as 'tasa_desc-item', cantidad*precio as 'importe_renglon', '' as 'clase_art', '' as 'def1Articulo',  '' as 'def1Articulo',  '' as 'desc_def1_Articulo',  '' as 'def2Articulo', 
    '' as 'desc_def2_Articulo', '' as 'cod_proveedor', '' as 'razon_social_proovedor', '' as 'desc 1', '' as 'desc 2', '' as 'desc 3', '' as 'cod_barra', '' as 'fecha entrega'
    from 
    factura f inner join cliente c on f.cliente_id= c.id
    inner join persona p on c.persona_id= p.id   
    left join factura_x_aviso as l on l.factura_id=f.id
    left join tipodeaviso as ta  on ta.id= l.id
    where f.puntodeventa_id >1  
    and f.fecha = '{s1}'""".format(s1=date)

    data = execute_query(query_header)
    
    

    file = open(route+"/VITEM.txt".format(date), 'w')
    if (not data.empty):
        lines = len(data)
        log.debug('found {} items in items get items line 214'.format(lines))
        try:
            for index, row in data.iterrows():

                line = ''
                # begin 63 fields
                # 1  Tipo comprobante 3 str : ‘FC’, ‘ND’, ‘NC’
                line += str('FC').ljust(3)
                # 2 letra 1 str
                line +=   str(row[1]).strip()
                # 3 punto vta str 4
                line += str(row[2]).rjust(4, '0')
                # 4 nro comprobante
                line += str(row[3]).rjust(8, '0')
                # 5 nro hasta
                line += str('').ljust(8)
                # 6 fecha comprobante str 8
                line += str(row[4]).replace('-', '')
                # 7 codig cliente str 6
                numcliente= int(row[5]) if row[5] != '' else '0'
                line += str(numcliente).rjust(6, '0')
                # 8 tipoItem L o C
                line += str(row[6]).ljust(1)
                # 9 cod de concepto de articulo
                line += str(row[7]).ljust(23)
                # 10 cantidad en unidad de medida
                line += str(row[8]).rjust(16)
                # 11cantidad en unidad 2
                line += str(row[9]).rjust(16)
                # 12 descripcion articulo
                line += str(row[10]).ljust(50)
                # 13 precio unitario
                line += str(row[11]).rjust(16)
                # 14 tasa iva inscripto
                line += str(row[12]).rjust(8)
                # 15 tasa iva no inscripto
                line += str(row[13]).rjust(8)
                # # 16 importe iva inscripto
                line += str(row[14]).rjust(16)
                # 17 importe iva no inscripto
                line += str(row[15]).rjust(16)
                # 18 importe Total sin iva luego de aplicar descuentos(debo calcular los decuentos)
                line += str(row[16]).rjust(16)
                # 19 importe de descuento comercial
                line += str(row[17]).rjust(16)
                # 20 importe de descuento financiero
                line += str(row[18]).rjust(16)
                # 21 importe de descuento al pie
                line += str(row[19]).rjust(16)
                # 22 cod comprobante no gravado
                line += str(row[20]).rjust(4)
                # 23 importe no gravado
                line += str(row[21]).rjust(16)
                # 24 tipo de iva
                line += str(row[22]).rjust(1)
                # 25 cod descuento por linea
                line += str(row[23]).rjust(2)
                # 26 importe descuento por linea
                line += str(row[24]).rjust(16)
                # 27 deposito
                line += str(row[25]).rjust(3)
                # 28 Partida
                line += str(row[26]).rjust(26)
                # 29 tasa de descuento por item
                line += str(row[27]).rjust(8)
                # 30 Importe del renglón
                line += str(row[28]).rjust(16)
                # 31 clase del artículo
                line += str(row[29]).rjust(3)
                # 32 definible 1 de articulos
                line += str(row[30]).ljust(4)
                # 33 desc definible 1 de articulos
                line += str(row[31]).ljust(25)
                # 34 definible 12 de articulos
                line += str(row[32]).ljust(4)
                # 35 desc definible 2 de articulos
                line += str(row[33]).ljust(25)
                # 36 cod proveedor
                line += str(row[34]).rjust(6)
                # 37  razon social proveedor
                line += str(row[35]).ljust(40)
                # 38 descripcion elento N1
                line += str(row[36]).ljust(15)
                # 39 descripcion elento N2
                line += str(row[37]).ljust(15)
                # 40 descripcion elento N3
                line += str(row[38]).ljust(15)
                # 41 codigo de barras
                line += str(row[39]).ljust(20)
                # 42 fecha entrega
                line += str(row[40]).ljust(8)

                file.write(line + "\n")
                # file.write(''.join([str(val) for val in row])+ os.linesep)
        except Exception as e:
            # log.debug('ERROR getItems()', e)
            print(e)

        
        file.close()

        log.debug('%s lines registered' % (lines))
        return True
    else:
        print( 'found 0 items')

        file.close()
        log.debug('found 0 items in items ' )
       
        return False


def generatePagos( date, route):
    # make query to get payments from receipts
    query_header = """SELECT 'RC' as 'tipocomprobante','' as  'letra' , '' as  'puntodeventa' ,r.id as  'nro' ,  '' as 'nro hasta', r.fecha, c.numero_cliente  as 'codcliente',
                    '1' as ' medio de pago', '1' as 'moneda', 'UNI' as 'tipod e cambio','CEN' as 'caja', '' as 'tipo',  '' as 'fecha vto cheque', r.valor as 'importe',
                    '' as 'nro cheque','' as  'cod banco', '' as 'sucursal bco', '' as 'clearing', '' as 'origen',  '' as 'cod cta bancaria', '' as 'nro tarjeta',
                    '' as 'nro autorizacion', concat(p.nombre, ' ', p.apellido)  as 'nombre librador', concat(p.calle,' ' ,p.nro) as 'direccion',  v.codigo_postal, l.provincia_id as 'provincia',
                    l.nombre as 'localidad',p.telefono, valor
                    FROM diariolm.recibo r 
                    inner join cliente c on c.id=r.cliente_id 
                    inner join persona p on p.id = c.persona_id 
                    left join localidad l on p.localidad_id= l.id
                    left join provincia as v on l.provincia_id = v.id
                    where fecha ='{s1}'""".format(s1=date)

    data1 = execute_query(query_header) 

    # make query for cash payment 
    query2_header = """SELECT 'RC' as 'tipocomprobante','X' as  'letra' , '' as  'puntodeventa' ,x.id as  'nro' ,  '' as 'nro hasta', f.fecha, c.numero_cliente  as 'codcliente',
                    if(x.tipodepago_id=3 ,'1', if(x.tipodepago_id=2, '4',if((x.tipodepago_id=1 or x.tipodepago_id>=4), 2,''))) as ' medio de pago', '1' as 'moneda', 'UNI' as 'tipod e cambio',
                    '' as 'caja', '' as 'tipo',  '' as 'fecha vto cheque', x.total as 'importe',
                    if(x.tipodepago_id=1, x.numero, '') as 'nro cheque',x.banco as  'cod banco', '' as 'sucursal bco', '' as 'clearing', '' as 'origen',  '' as 'cod cta bancaria', '' as 'nro tarjeta',
                    '' as 'nro autorizacion', concat(p.nombre, ' ',p.apellido)  as 'nombre librador', concat(p.calle,' ' ,p.nro) as 'direccion',  v.codigo_postal, l.provincia_id as 'provincia',
                    l.nombre as 'localidad',p.telefono, x.total
                    FROM diariolm.pago x
                    inner join factura f on f.id=x.factura_id
                    inner join cliente c on c.id=f.cliente_id
                    inner join persona p on c.persona_id=p.id
                    left join localidad l on p.localidad_id= l.id
                    left join provincia as v on l.provincia_id = v.id
                    where  date(x.created_at)='{s1}'""".format(s1=date)
    
    data2 = execute_query(query2_header) 

    file = open(route+"/VMEDPAGO.txt", 'w')

    data= pd.concat([data1, data2])

    if (not data.empty):
        lines = len(data)
        
        log.debug('found {} pagos'.format(lines))
        for index, row in data.iterrows():

            line = ''
            # begin 63 fields
            # 1  Tipo comprobante 3 str : ‘FC’, ‘ND’, ‘NC’
            line += str(row[0]).ljust(3)
            # 2 letra 1 str
            line += str(row[1]).strip()
            # 3 punto vta str 4
            line += str(row[2]).rjust(4, '')
            # 4 nro comprobante
            line += str(row[3]).rjust(8, '0')
            # 5 nro hasta
            line += str(row[4]).ljust(8)
            # 6 fecha comprobante str 8
            line += str(row[5]).replace('-', '')
            # 7 codig cliente str 6
            line += str(row[6]).rjust(6, '0')
            # 8  medio de pago
            line += str(row[7]).ljust(1)
            # 9 moneda
            line += str(row[8]).rjust(3)
            # 10 tipo cambio 
            line += str(row[9]).rjust(3)
            # 11 caja str 3
            line += str(row[10]).rjust(3)
            # 12 tipo str 3
            line += str(row[11]).rjust(3)
            # 13 fecah vto
            line += str(row[12]).rjust(8)
            # 14 importe num 16
            line += str(row[13]).rjust(16)
            # 15 nro cheque
            line += str(row[14]).rjust(8)
            # 16 codigo banco
            line += str(row[15]).rjust(3)
            # 17 sucursal
            line += str(row[16]).rjust(4)
            # 18 clearing
            line += str(row[17]).rjust(3)
            # 19 origen
            line += str(row[18]).rjust(1)
            # 20 cod cuenta  bancaria
            line += str(row[19]).ljust(15)
            # 21 nro tarjeta
            line += str(row[20]).rjust(25)
            # 22 nro autorizacion
            line += str(row[21]).rjust(25)
            # 23 nombre 
            line += str(row[22]).ljust(30)
            # 24 direccion
            line += str(row[23]).ljust(30)
            # 25 cod postal 
            line += str(row[24]).rjust(8)
            # 26 provincia
            line += str(row[25]).ljust(3)
            # 27 localidad
            line += str(row[26]).ljust(25)
            # 28 telefono
            line += str(row[27]).ljust(30)
            # 29 importe en moneda local
            line += str(row[28]).rjust(16)            
            
            
            file.write(line + "\n")
            # file.write(''.join([str(val) for val in row])+ os.linesep)

        file.close()
    else:
        log.debug('found 0 pagos')
        file.close()
    return True


def main():
    # today = date.today()
    today = beginning_date.split('-')
    now = datetime.now().strftime("%d-%m-%Y %H %M %S")

    log.basicConfig( filename='logs/export_{}.log'.format(now), encoding='utf-8', level=log.DEBUG)

    
    log.debug("hello begerman {}".format(beginning_date))
    print('begin daily process')

    start_date = datetime.strptime(beginning_date, '%Y-%m-%d') - timedelta(days=delta)
    print('date:', start_date)
    # I process dates from de start day to end date
    files_generated = 0
    for i in range(delta):
        date_to_process = start_date + timedelta(days=i)
        try:
            date_formated = date_to_process.strftime("%Y-%m-%d")
           
            date_arr= str(date_formated).split('-')
            route = 'exports/' + date_arr[0] + '/' + date_arr[1] + '/'  + date_arr[2]
            if not os.path.exists(route):
                # if the directory is not present
                # then create it.
                os.makedirs(route)
 
            # generate Cabecera files
            print('generate header: ' + date_formated)
            

            generateCabecera(date_formated, route)
            # log.debug(f"File generated, date= {date_to_process}")
          
            # generate items
            print('generate items: ' + date_formated)
            generateItems(date_formated, route)
            log.debug("items generated, date= {}".format(date_to_process))
 
            print('generate pagos: ' + date_formated)
            generatePagos(date_formated, route)
            log.debug("payments generated")

            files_generated += 1
        except Exception as e:
            print(e)
            log.error(e)

    print( 'finished {0} files generated fron {1} to {2}'.format( files_generated,start_date,today))


if __name__ == "__main__":
    main()
