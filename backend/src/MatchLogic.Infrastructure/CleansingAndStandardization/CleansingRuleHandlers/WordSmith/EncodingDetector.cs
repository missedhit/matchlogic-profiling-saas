using System.IO;
using System.Text;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;

public static class EncodingDetector
{
    private const int SampleSize = 4096;

    public static Encoding Detect(string filePath)
    {
        using var fs = File.OpenRead(filePath);
        var buffer = new byte[System.Math.Min(SampleSize, fs.Length)];
        var read = fs.Read(buffer, 0, buffer.Length);

        if (read >= 3 && buffer[0] == 0xEF && buffer[1] == 0xBB && buffer[2] == 0xBF)
            return new UTF8Encoding(encoderShouldEmitUTF8Identifier: true);
        if (read >= 2 && buffer[0] == 0xFF && buffer[1] == 0xFE)
            return Encoding.Unicode;
        if (read >= 2 && buffer[0] == 0xFE && buffer[1] == 0xFF)
            return Encoding.BigEndianUnicode;

        // UTF-16 LE without BOM: ASCII-heavy text has zero bytes at odd positions
        if (read > 20 && CountNullsAtOddIndex(buffer, read) * 2.5 > read)
            return Encoding.Unicode;

        try
        {
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true)
                .GetString(buffer, 0, read);
            return new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        }
        catch (DecoderFallbackException)
        {
            return Encoding.Unicode;
        }
    }

    private static int CountNullsAtOddIndex(byte[] buffer, int length)
    {
        int count = 0;
        for (int i = 1; i < length; i += 2)
            if (buffer[i] == 0) count++;
        return count;
    }
}
