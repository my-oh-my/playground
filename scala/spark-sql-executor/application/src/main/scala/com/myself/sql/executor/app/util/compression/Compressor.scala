package com.myself.sql.executor.app.util.compression

import java.io.IOException;

trait Compressor {

    @throws(classOf[IOException])
    def compress(data: String): String

    @throws(classOf[IOException])
    def compress(data: Array[Byte]): String

    @throws(classOf[IOException])
    def decompress(data: Array[Byte]): Array[Byte]

    @throws(classOf[IOException])
    def decompress(data: String): String

}