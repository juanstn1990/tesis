df_transform = """
organiza este dataframe, crea la siguientes columnas item, Descripcion, Concentracion, laboratorio , valor
y forma farmaceutica (tableta, capsula, jarave, solucion oral, solucion inyectable, crema, gel, polvo, suspension, gotas, ovulo, supositorio, inyectable, inhalador, parche, shampoo, locion, enema, pasta, aerosol, liquido,crema, polvo para solucion oral, polvo para suspension oral)
, si no existe el dato para la columna concentracion intenta extraerlo de la descripcion,

si no existe el dato para la columna presentacion intenta extraerlo de la descripcion,pero manten la descripcion con el nombre original 
para el valor manten que sea un dato numerico

Hazlo tu no me entregues codigo {dataframe}

requiero que la salida tenga esta estructura , si o si siempre la debe tener, haz todos los items

    [
        {
            'item': '1',
            'Descripcion': 'Acetaminofen',
            'Concentracion': '500mg',
            'Forma_farmaceutica': 'Tableta',
            'Laboratorio': 'Genfar',
            'Valor_Unitario': 125
        },
        {
            'item': '2',
            'Descripcion': 'Amoxicilina',
            'Concentracion': '250mg',
            'Forma_farmaceutica': 'Capsula',
            'Laboratorio': 'Aust',
            'Valor_Unitario': 90
        }
    }
    ]

    si el dataframe no correspode a medicamentos solo devuelve blano
"""
