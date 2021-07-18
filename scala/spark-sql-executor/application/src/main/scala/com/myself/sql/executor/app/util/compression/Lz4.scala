package com.myself.sql.executor.app.util.compression

import java.nio.charset.StandardCharsets
import java.util.Base64

import net.jpountz.lz4._

object Lz4 extends Compressor {

  val factory: LZ4Factory = LZ4Factory.safeInstance()

  override def compress(data: String): String = {

    compress(data.getBytes( StandardCharsets.UTF_8))
  }

  override def compress(data: Array[Byte]): String = {

    val compressor: LZ4Compressor = factory.highCompressor()
    val compressorWL: LZ4CompressorWithLength = new LZ4CompressorWithLength(compressor)

    Base64.getEncoder().encodeToString(compressorWL.compress(data))
  }

  override def decompress(data: Array[Byte]): Array[Byte] = {

    val decompressor: LZ4SafeDecompressor = factory.safeDecompressor()
    val decompressorWL: LZ4DecompressorWithLength = new LZ4DecompressorWithLength(decompressor)

    decompressorWL.decompress(data)
  }

  override def decompress(data: String): String = {

    val bytes: Array[Byte] = Base64.getDecoder()decode(data)

    new String(bytes, StandardCharsets.UTF_8)
  }

}
