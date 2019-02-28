package com.ibm.sap;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.impl.util.JavaMapping;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.file.javadsl.Directory;
import akka.stream.alpakka.s3.ListBucketResultContents;
import akka.stream.alpakka.s3.MultipartUploadResult;
import akka.stream.alpakka.s3.javadsl.S3;
import akka.stream.javadsl.Compression;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import scala.Option;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;


public class App 
{


    public static final String AVRO_ROOT_DIR = "src\\main\\resources\\samples";
    public static final String AVRO_FILE_EXT = ".avro";
    public static final Integer GROUP_SIZE = 10;
    public static final String BUCKET_NAME = "demo-8000";

    final ActorSystem system = ActorSystem.create("demo");
    final Materializer materializer = ActorMaterializer.create(system);


    public static void main( String[] args )
    {
        App app = new App();

        app.listBucket();

        app.uploadAvroFiles();

    }


    public  void listBucket() {
        final Source<ListBucketResultContents, NotUsed> keySource = S3.listBucket(BUCKET_NAME, Option.empty());
        keySource.runWith(Sink.foreach((i)-> System.out.println(i)), materializer);
    }

    public void uploadAvroFiles() {
        AtomicInteger ID = new AtomicInteger(1);

        final Source<Path, NotUsed> source = Directory.walk(Paths.get(AVRO_ROOT_DIR).toAbsolutePath());

        Source<ByteString, NotUsed> bytes  = source.map(file -> file)
                .filter(file-> file.getFileName().toString().endsWith(AVRO_FILE_EXT))
                .<DataFileStream<GenericRecord>>map(avro -> AvroFileReader.getStream(avro.toAbsolutePath().toString()))
                .mapConcat(avro -> avro)
                .grouped(GROUP_SIZE)
                .map(a -> ByteString.fromString(a.toString()))
                .via(Compression.gzip());

        final Sink<ByteString, Source<MultipartUploadResult, NotUsed>> s3sink =
                S3.multipartUpload(BUCKET_NAME, String.valueOf(ID.getAndIncrement()) + ".json.gz");

        bytes.runWith(s3sink, materializer).runWith(Sink.ignore(), materializer).toCompletableFuture();

    }

}
