

package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Tuple  implements WritableComparable<Tuple>  {
	public static final byte BYTE = 0;
	public static final byte BOOLEAN = 1;
	public static final byte INT = 2;
	public static final byte LONG = 3;
	public static final byte FLOAT = 4;
	public static final byte DOUBLE = 5;
	public static final byte STRING = 6;
	public static final byte BYTE_ARRAY = 7;
	
	private List<Object> fields;
	private String delim = ",";
	
	public Tuple() {
		fields = new ArrayList<Object>();
	}
	
	public Tuple(List<Object> fields) {
		this.fields = fields;
	}

	public void initialize() {
		fields.clear();
	}
	
	public int getSize() {
		return fields.size();
	}
	
	public void add(Object...  fieldList) {
		for (Object field :  fieldList) {
			fields.add(field);
		}
	}

	public void prepend(Object field) {
		fields.add(0, field);
	}

	public void add(byte[] types, String[] fields) {
		for (int i = 0; i <  fields.length; ++i) {
			add(types[i],  fields[i]) ;
		}
	}
	
	public void add(byte type, String field) {
		Object typedField = null;
		
		if (type ==  BYTE ) {
			typedField = Byte.decode(field);
		} else if (type ==  BOOLEAN ) {
			typedField = Boolean.parseBoolean(field);
		} else if (type ==  INT ) {
			typedField = Integer.parseInt(field);
		}  else if (type ==  LONG ) {
			typedField =  Long.parseLong(field);
		}  else if (type ==  FLOAT ) {
			typedField = Float.parseFloat(field);
		} else if (type ==  DOUBLE ) {
			typedField = Double.parseDouble(field);
		} else if (type ==  STRING) {
			typedField = field;
		} else if (type ==  BYTE_ARRAY) {
			try {
				typedField = field.getBytes("utf-8");
			} catch (UnsupportedEncodingException e) {
				throw new IllegalArgumentException("Failed adding element to tuple, unknown element type");
			}
		}  else {
			throw new IllegalArgumentException("Failed adding element to tuple, unknown element type");
		}
		
		if (null != typedField){
			fields.add(typedField);
		}
	}

	public void set(int index, Object field) {
		fields.add(index, field);
	}
	
	public Object get(int index) {
		return fields.get(index);
	}
	
	public String getString(int index) {
		return (String)fields.get(index);
	}

	public int getInt(int index) {
		return (Integer)fields.get(index);
	}

	public long getLong(int index) {
		return (Long)fields.get(index);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		initialize();
		int numFields = in.readInt();
		
		for(int i = 0;  i < numFields;  ++i) {
			byte type = in.readByte();
			
			if (type ==  BYTE ) {
				fields.add(in.readByte());
			} else if (type ==  BOOLEAN ) {
				fields.add(in.readBoolean());
			} else if (type ==  INT ) {
				fields.add(in.readInt());
			}  else if (type ==  LONG ) {
				fields.add(in.readLong());
			}  else if (type ==  FLOAT ) {
				fields.add(in.readFloat());
			} else if (type ==  DOUBLE ) {
				fields.add(in.readDouble());
			} else if (type ==  STRING) {
				fields.add(in.readUTF());
			} else if (type ==  BYTE_ARRAY) {
				int  len = in.readShort();
				byte[] bytes = new byte[len];
				in.readFully(bytes);
				fields.add(bytes);
			} else {
				throw new IllegalArgumentException("Failed encoding, unknown element type in stream");
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(fields.size());
		
		for(Object field : fields) {
			if (field instanceof Byte){
				out.writeByte(BYTE);	
				out.writeByte((Byte)field);
			} else if (field instanceof Boolean){
				out.writeByte(BOOLEAN);	
				out.writeBoolean((Boolean)field);
			} else if (field instanceof Integer){
				out.writeByte(INT);	
				out.writeInt((Integer)field);
			} else if (field instanceof Long){
				out.writeByte(LONG);	
				out.writeLong((Long)field);
			} else if (field instanceof Float){
				out.writeByte(FLOAT);	
				out.writeFloat((Float)field);
			} else if (field instanceof Double){
				out.writeByte(DOUBLE);	
				out.writeDouble((Double)field);
			} else if (field instanceof String){
				out.writeByte(STRING);	
				out.writeUTF((String)field);
			} else if (field instanceof byte[]){
				byte[] bytes = (byte[])field;
				out.writeByte(BYTE_ARRAY);
				out.writeShort(bytes.length);
				out.write(bytes);
			} else {
				throw new IllegalArgumentException("Failed encoding, unknown element type in tuple");
			}
		}
	}

	public int hashCode() {
		return fields.hashCode();
	}
	
	public boolean equals(Object obj ) {
		boolean isEqual = false;
		if (null != obj && obj instanceof Tuple){
			isEqual =  ((Tuple)obj).fields.equals(fields);
		}
		return isEqual;
	}

	@Override
	public int compareTo(Tuple that) {
		int compared = 0;
		if (fields.size() == that.fields.size()) {
			for(int i = 0; i <  fields.size() && compared == 0; ++i) {
				Object field = fields.get(i);
				if (field instanceof Byte){
					compared = ((Byte)field).compareTo((Byte)that.fields.get(i));	
				} else if (field instanceof Boolean){
					compared = ((Boolean)field).compareTo((Boolean)that.fields.get(i));	
				} else if (field instanceof Integer){
					compared = ((Integer)field).compareTo((Integer)that.fields.get(i));	
				} else if (field instanceof Long){
					compared = ((Long)field).compareTo((Long)that.fields.get(i));	
				} else if (field instanceof Float){
					compared = ((Float)field).compareTo((Float)that.fields.get(i));	
				} else if (field instanceof Double){
					compared = ((Double)field).compareTo((Double)that.fields.get(i));	
				} else if (field instanceof String){
					compared = ((String)field).compareTo((String)that.fields.get(i));	
				}  else {
					throw new IllegalArgumentException("Failed in compare, unknown element type in tuple  ");
				}
			}
		} else {
			throw new IllegalArgumentException("Can not compare tuples of unequal length this:"  + 
					fields.size() + " that:" +  that.fields.size());
		}
		return compared;
	}
	
	public int compareToBase(Tuple other) {
		Tuple subThis = new Tuple(fields.subList(0,fields.size()-1));
		Tuple subThat = new Tuple(other.fields.subList(0,other.fields.size()-1));
		return subThis.compareTo(subThat);
	}
	
	public int hashCodeBase() {
		Tuple subThis = new Tuple(fields.subList(0,fields.size()-1));
		return subThis.hashCode();
	}
	
	public void setDelim(String delim) {
		this.delim = delim;
	}

	public String toString() {
		StringBuilder stBld = new  StringBuilder();
		for(int i = 0; i <  fields.size() ; ++i) {
			if (i == 0){
				stBld.append(fields.get(i).toString());
			} else {
				stBld.append(delim).append(fields.get(i).toString());
			}
		}		
		return stBld.toString();
	}
	
	
}
