package eu.profinit.manta.graphplayground.model.manta.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.codec.binary.Base64;

/**
 * Pomocna trida pro kodovani objektu a retezcu v Base64 pro ulozeni do csv. 
 * @author Pavel Jaromersky.
 */
public final class Base64AttributeCodingHelper {

    private static final String PREFIX = "B64:";

    /**
     * Default konstruktor pro zamezeni instanciace.
     */
    private Base64AttributeCodingHelper() {
    }

    /**
     * @param attributeValue Vstupni hodnota, kterou je potreba zakodovat. 
     * @return Vrati attVal, pokud attVal je typu String, jinak se pokusi prevest attVal do streamu, zakodovat v Base64,
     *  a pridat predponu '{@value #PREFIX}'.
     * @throws IOException 
     */
    public static String encodeAttribute(Object attributeValue) throws IOException {
        if (attributeValue == null) {
            // Pokud je vstup null, tak vratit take null primo.
            return null;
        } else if (attributeValue instanceof String) {
            // Pokud vstupem je String, tak vratit String primo. 
            return (String) attributeValue;
        } else {
            // Na vstupu je objekt, a neni to String. Serializovat a zakodovat do Base64.
            byte[] outputBytes = serializeObject(attributeValue);
            return PREFIX + Base64.encodeBase64String(outputBytes);
        }
    }

    /**
     * Provede serializaci objektu - prevede objekt na pole bytu reprezentujici objekt. 
     * @param objectToSerialize objektk  serializaci
     * @return serializovaný objekt
     * @throws IOException chyba během serializace
     */
    public static byte[] serializeObject(Object objectToSerialize) throws IOException {
        ObjectOutputStream objectOut = null;
        ByteArrayOutputStream memOut = new ByteArrayOutputStream();
        try {
            objectOut = new ObjectOutputStream(memOut);
            try {
                objectOut.writeObject(objectToSerialize);
            } finally {
                objectOut.close();
            }
        } finally {
            memOut.close();
        }
        byte[] outputBytes = memOut.toByteArray();
        return outputBytes;
    }

    /**
     * Dekodovat nactenou String hodnotu, a zjistit, zdali nekoduje binarni objekt v Base64.
     * @param serializedObject Vstupni String, u ktereho je potreba zjistit, 
     * zdali koduje binarni objekt, nebo bezny String. Pokud koduje objekt, tak ho zrekonstruovat zpet. 
     * @return deserializovaný objekt nebo null při chybě
     * @throws IOException chyba při načítání serializovaného objektu
     * @throws ClassNotFoundException serializovaný objekt má neznámou třídu
     */
    public static Object decodeAttribute(String serializedObject) throws IOException, ClassNotFoundException {
        // Docasne, abych videl, zdali je problem v titanu.
        if (serializedObject == null) {
            // Pokud je vstup null, tak vratit take null. 
            return null;
        } else if (serializedObject.startsWith(PREFIX)) {
            // Prefix oznacujici kodovani BASE64.
            byte[] objBytes = Base64.decodeBase64(serializedObject.substring(PREFIX.length()));
            Object desObject = deserializeObject(objBytes);
            return desObject;
        } else {
            // Jedna se o retezec, ktery nekoduje objekt. 
            return serializedObject;
        }
    }

    /**
     * Provede deserializaci objektu - objBytes prevede na instanci objektu. 
     * @param objBytes serializovaný objekt
     * @return  Instance objektu odpovidajici objBytes. 
     * @throws IOException chyba při načítání serializovaného objektu
     * @throws ClassNotFoundException serializovaný objekt má neznámou třídu
     */
    public static Object deserializeObject(byte[] objBytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream memInput = new ByteArrayInputStream(objBytes);
        ObjectInputStream objInputStream = null;
        try {
            objInputStream = new ObjectInputStream(memInput);
            try {
                return objInputStream.readObject();
            } finally {
                objInputStream.close();
            }
        } finally {
            memInput.close();
        }
    }
}
