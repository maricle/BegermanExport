SELECT f.tipocomprobante, f.tipo as 'letra', f.puntodeventa_id  as 'pv', f.numero, f.fecha, c.numero_cliente as 'cod_cliente',
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
    and f.fecha >= '2022-10-03'