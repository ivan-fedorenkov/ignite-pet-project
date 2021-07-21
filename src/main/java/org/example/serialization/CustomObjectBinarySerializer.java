package org.example.serialization;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryWriter;
import org.example.domain.CustomObject;

public class CustomObjectBinarySerializer implements BinarySerializer {
    private static final String NAME_FIELD = "name";

    @Override
    public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
        writer.writeString(NAME_FIELD, ((CustomObject) obj).getName());
    }

    @Override
    public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
        ((CustomObject) obj).setName(reader.readString(NAME_FIELD));
    }
}
