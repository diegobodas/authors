package com.app.countpapers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Recibo: LongWritable del texto (posición byte)
//Recibo como valor: la línea del texto (Text)
//Emito como clave: el autor (una emisión por autor)
//Emito como valor: 1

public class MapperCountPapers extends
 Mapper<LongWritable, Text, Text, IntWritable> {

  private Text AuthorAsText = new Text();
  private IntWritable theOne = new IntWritable(1);

  @Override
  protected void map(LongWritable key, Text textLine, Context context)
      throws IOException, InterruptedException {

    // Descomposición de la línea de texto en una lista de Strings
    List<String> stringList =
        Arrays.asList(textLine.toString().toLowerCase().split(":::"));

    // Lista de autores
    List<String> authorsList = new ArrayList<String>();
    // Claro que, ahora mismo no tenemos un control de errores activado
    // OJO, el final es tamaño -1 PORQUE NO COGE EL ÚLTIMO ÍNDICE
    authorsList = stringList.subList(1, stringList.size() - 1);
    // Nos recorremos la lista con iteradores
    Iterator<String> authorIterator = authorsList.iterator();
    while (authorIterator.hasNext()) {
      // Obtenemos el valor del autor y lo emitimos junto un 1
      AuthorAsText.set(authorIterator.next());
      context.write(AuthorAsText, theOne);
    }
  }
}
