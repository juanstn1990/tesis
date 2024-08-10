{% macro normalize_text() %}
CREATE OR REPLACE FUNCTION `artful-sled-419501`.secop.normalize_text (input STRING)
RETURNS STRING
LANGUAGE js AS """
  // Definir las cadenas de caracteres a reemplazar
  var fromChars = 'ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ';
  var toChars = 'SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyy';
  
  // Crear un mapa de reemplazo
  var translateMap = {};
  for (var i = 0; i < fromChars.length; i++) {
    translateMap[fromChars.charAt(i)] = toChars.charAt(i);
  }
  
  // Realizar la traducción de caracteres
  var output = '';
  for (var i = 0; i < input.length; i++) {
    var char = input.charAt(i);
    output += translateMap[char] || char;
  }
  
  // Convertir el resultado a mayúsculas
  return output.toUpperCase();
""";
{% endmacro %}