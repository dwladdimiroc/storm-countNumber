package citiaps.countApp.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Palabras {
	public boolean contiene(List<String> palabras, String texto) {
		for (String palabra : palabras) {
			String patron = ".*\\b" + palabra + "\\b.*";
			if (texto.toLowerCase().matches(patron))
				return true;
		}
		return false;
	}

	public String eliminar(List<String> palabras, String texto) {

		for (String palabra : palabras) {
			String patron = "\\b" + palabra + "\\b";
			texto = texto.replaceAll(patron, "");
		}

		return texto;
	}

	public List<String> palabraContenidas(List<String> palabras, String texto) {
		List<String> palabrasCont = new ArrayList<String>();
		for (String palabra : palabras) {
			String patron = ".*\\b" + palabra + "\\b.*";
			if (texto.toLowerCase().matches(patron))
				palabrasCont.add(palabra);
		}
		return palabrasCont;
	}

	public int numPalabrasContenidas(List<String> palabras, String texto) {
		int cont = 0;
		for (String palabra : palabras) {
			String patron = ".*\\b" + palabra + "\\b.*";
			if (texto.toLowerCase().matches(patron))
				cont++;
		}
		return cont;
	}

	public List<String> leerDiccionario(String ruta) {
		List<String> palabras = new ArrayList<String>();

		String nombreArchivo = ruta;
		String linea = null;

		try {
			FileReader fileReader = new FileReader(nombreArchivo);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((linea = bufferedReader.readLine()) != null) {
				palabras.add(linea);
			}

			bufferedReader.close();
			fileReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("No se puede abrir el archivo '" + nombreArchivo + "'");
		} catch (IOException ex) {
			System.out.println("Error al leer el archivo '" + nombreArchivo + "'");
		}

		return palabras;
	}
}
