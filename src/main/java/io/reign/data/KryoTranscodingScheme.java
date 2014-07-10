package io.reign.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Use Kryo as the serialization scheme.
 * 
 * @author ypai
 * 
 */
public class KryoTranscodingScheme implements TranscodingScheme {

	/**
	 * Kryo is not thread-safe.
	 */
	private static final ThreadLocal<Kryo> KRYO_THREAD_LOCAL = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			return kryo;
		}

	};

	public KryoTranscodingScheme() {
	}

	@Override
	public byte[] toBytes(Object value) {
		if (value == null) {
			return null;
		}

		Kryo kryo = KRYO_THREAD_LOCAL.get();
		Output out = new Output(new ByteArrayOutputStream());
		kryo.writeObject(out, value);
		byte[] bytes = out.toBytes();
		out.close();
		return bytes;
	}

	@Override
	public <T> T fromBytes(byte[] bytes, Class clazz) {
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		Kryo kryo = KRYO_THREAD_LOCAL.get();
		Input input = new Input(new ByteArrayInputStream(bytes));
		T value = (T) kryo.readObject(input, clazz);
		input.close();
		return value;
	}

	public void register(Class clazz, Serializer serializer) {
		Kryo kryo = KRYO_THREAD_LOCAL.get();
		kryo.register(clazz, serializer);
	}
}
