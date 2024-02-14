package gr.imsi.athenarc.visual.middleware.util.io;

import java.io.*;


/**
 *
 * @author gap2
 */

public class SerializationUtilities {

	public static Object loadSerializedObject(String fileName) {
		Object object = null;
		try {
			InputStream file = new FileInputStream(fileName);
			InputStream buffer = new BufferedInputStream(file);
			ObjectInput input = new ObjectInputStream(buffer);
			try {
				object = input.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException ioex) {
			System.err.println(fileName);
			ioex.printStackTrace();
		}

		return object;
	}

	public static Object loadSerializedObject(InputStream file) {
		Object object = null;
		try {
			InputStream buffer = new BufferedInputStream(file);
			ObjectInput input = new ObjectInputStream(buffer);
			try {
				object = input.readObject();
			} finally {
				input.close();
			}
		} catch (ClassNotFoundException cnfEx) {
			cnfEx.printStackTrace();
		} catch (IOException ioex) {
			ioex.printStackTrace();
		}
		
		
		return object;
	}
	


	
	public static Object loadSerializedObjectWithExceptions(String fileName) throws Exception {
		InputStream file = new FileInputStream(fileName);
		InputStream buffer = new BufferedInputStream(file);
		ObjectInput input = new ObjectInputStream(buffer);
		Object object = input.readObject();
		input.close();

		return object;
	}

	public static void storeSerializedObject(Object object, String outputPath) {
		try {
			OutputStream file = new FileOutputStream(outputPath);
			OutputStream buffer = new BufferedOutputStream(file);
			ObjectOutput output = new ObjectOutputStream(buffer);
			try {
				output.writeObject(object);
			} finally {
				output.close();
			}
		} catch (IOException ioex) {
			ioex.printStackTrace();
		}

	}
}