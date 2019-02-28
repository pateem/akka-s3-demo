package com.ibm.sap;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

public class AvroFileReader {

    /**
     *
     * @param absoluteAvroPath - Absolute path to location of an Avro file
     * @return
     * @throws IOException
     */
    public static FileReader get(String absoluteAvroPath) throws IOException {
        File file = Paths.get(absoluteAvroPath).toFile();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        return new DataFileReader<>(file, datumReader);
    }

    public static DataFileStream getStream(String absoluteAvroPath) throws IOException {
        InputStream is = new FileInputStream(absoluteAvroPath);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        return new DataFileStream<>(is, datumReader);

    }

}
