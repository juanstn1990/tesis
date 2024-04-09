df_transform = """
organiza este dataframe, crea la siguientes columnas item, Descripcion (para la descripcion confronta el nombre contra los listados de medicamentos de tu base de datos y toma el tuyo), Concentracion, laboratorio , valor
y forma farmaceutica (tableta, capsula, jarave, solucion oral, solucion inyectable, crema, gel, polvo, suspension, gotas, ovulo, supositorio, inyectable, inhalador, parche, shampoo, locion, enema, pasta, aerosol, liquido,crema, polvo para solucion oral, polvo para suspension oral)
, si no existe el dato para la columna concentracion intenta extraerlo de la descripcion,

si no existe el dato para la columna presentacion intenta extraerlo de la descripcion,pero manten la descripcion con el nombre original 
para el valor manten que sea un dato numerico si no encuentras un valor pon 0, ten encuenta que algunas veces el valor trae una , o un . pero no significa que sea un flotante

Hazlo tu no me entregues codigo {dataframe}

requiero que la salida tenga esta estructura , si o si siempre la debe tener, haz todos los items
debe tener la estructura de lista de diccionarios por lo cual no pueden haber comillas entre los valores

    [
        {{
            'Item': '1',
            'Descripcion': 'Acetaminofen',
            'Concentracion': '500mg',
            'Forma_farmaceutica': 'Tableta',
            'Laboratorio': 'Genfar',
            'Valor_Unitario': 125
        }},
        {{
            'Item': '2',
            'Descripcion': 'Amoxicilina',
            'Concentracion': '250mg',
            'Forma_farmaceutica': 'Capsula',
            'Laboratorio': 'Aust',
            'Valor_Unitario': 90
        }}
    }}
    ]

    si el dataframe no correspode a medicamentos solo devuelve {{}}
"""