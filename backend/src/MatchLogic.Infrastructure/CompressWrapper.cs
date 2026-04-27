using System;
using System.IO;
using System.IO.Compression;

namespace MatchLogic.Compression
{
    public class CompressWrapper
    {
        private const Int32 BufferLength = 65536;

        /// <summary>
        /// Zipping input stream
        /// </summary>
        /// <param name="inputStream">stream, that will be compressed</param>
        /// <returns>compressed data</returns>
        public static MemoryStream Compress(Stream inputStream)
        {
            inputStream.Position = 0L;
            Byte[] buffer = new Byte[BufferLength];
            using (MemoryStream memoryStream = new MemoryStream())
            {
                using (GZipStream gzipStream = new GZipStream(memoryStream, CompressionMode.Compress, true))
                {
                    while (true)
                    {
                        Int32 count = inputStream.Read(buffer, 0, BufferLength);
                        if (count != 0)
                        {
                            gzipStream.Write(buffer, 0, count);
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                return new MemoryStream(memoryStream.ToArray());
            }
        }

        /// <summary>
        /// Unzipping input stream
        /// </summary>
        /// <param name="inputStream">stream, that will be decompressed</param>
        /// <returns>decompressed data</returns>
        public static MemoryStream Decompress(Stream inputStream)
        {
            inputStream.Position = 0L;
            using (MemoryStream memoryStream = new MemoryStream())
            {
                using (GZipStream gzipStream = new GZipStream(inputStream, CompressionMode.Decompress, false))
                {
                    Byte[] buffer = new Byte[BufferLength];
                    while (true)
                    {
                        Int32 count = gzipStream.Read(buffer, 0, BufferLength);
                        if (count != 0)
                        {
                            memoryStream.Write(buffer, 0, count);
                        }
                        else
                        {
                            break;
                        }
                    }

                    return new MemoryStream(memoryStream.ToArray());
                }
            }
        }
    }
}