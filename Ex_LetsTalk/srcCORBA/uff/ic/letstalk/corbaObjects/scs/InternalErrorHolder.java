package uff.ic.letstalk.corbaObjects.scs;

/**
 * corbaObjects/scs/InternalErrorHolder.java . Generated by the IDL-to-Java
 * compiler (portable), version "3.2" from scs.idl Sexta-feira, 9 de Dezembro de
 * 2005 18h26min26s BRST
 */

public final class InternalErrorHolder implements
		org.omg.CORBA.portable.Streamable {
	public uff.ic.letstalk.corbaObjects.scs.InternalError value = null;

	public InternalErrorHolder() {
	}

	public InternalErrorHolder(uff.ic.letstalk.corbaObjects.scs.InternalError initialValue) {
		value = initialValue;
	}

	public void _read(org.omg.CORBA.portable.InputStream i) {
		value = uff.ic.letstalk.corbaObjects.scs.InternalErrorHelper.read(i);
	}

	public void _write(org.omg.CORBA.portable.OutputStream o) {
		uff.ic.letstalk.corbaObjects.scs.InternalErrorHelper.write(o, value);
	}

	public org.omg.CORBA.TypeCode _type() {
		return uff.ic.letstalk.corbaObjects.scs.InternalErrorHelper.type();
	}

}
