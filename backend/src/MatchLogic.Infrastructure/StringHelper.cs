using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using MatchLogic.Core.Security;

namespace MatchLogic.StringHelper;

public static class StringHelper
{
    private static Char[] whitespaceChars;

    public static Char[] GetAllWhiteSpaces()
    {
        if (whitespaceChars == null)
        {
            StringBuilder stringBuilder = new StringBuilder();

            for (Char someChar = Char.MinValue; someChar < UInt16.MaxValue; ++someChar)
            {
                if (Char.IsWhiteSpace(someChar))
                {
                    stringBuilder.Append(someChar);
                }
            }

            stringBuilder.Append(',');
            stringBuilder.Append(';');
            whitespaceChars = new Char[stringBuilder.Length];

            for (Int32 index = 0; index < stringBuilder.Length; ++index)
            {
                whitespaceChars[index] = stringBuilder[index];
            }
        }

        return whitespaceChars;
    }

    public static String GetNonPrintableCharacters()
    {
        String str = String.Empty;

        for (Int32 index = 0; index < UInt16.MaxValue; ++index)
        {
            Char c = (Char)index;

            if (Char.IsWhiteSpace(c) && c != 32)
            {
                str += c.ToString();
            }
        }

        return str;
    }

    public static String Reverse(String str)
    {
        StringBuilder stringBuilder = new StringBuilder(str.Length);
        Int32 length = str.Length;
        for (Int32 index = 0; index < length; ++index)
        {
            stringBuilder.Append(str[length - 1 - index]);
        }
        return stringBuilder.ToString();
    }

    /// <summary>
    /// String to hex codes of all chars
    /// </summary>
    /// <remarks>
    /// "20" -> "3230"
    /// "Hello" -> "48656C6C6F"
    /// </remarks>
    /// <param name="value"></param>
    /// <returns></returns>
    public static String StringToHex(String value)
    {
        StringBuilder stringBuilder = new StringBuilder(value.Length * 2);

        for (Int32 index = 0; index < value.Length; ++index)
        {
            stringBuilder.Append(((Int32)value[index]).ToString("X2"));
        }

        return stringBuilder.ToString();
    }

    /// <summary>
    /// Hex ASCII code of char to this char
    /// </summary>
    /// <remarks> 
    /// '31' -> '1'
    /// '20' -> ' '
    /// '3130' -> '10'
    /// </remarks>
    /// <param name="value"></param>
    /// <returns></returns>
    public static String HexToString(String value)
    {
        StringBuilder stringBuilder = new StringBuilder(value.Length / 2);
        Int32 index = 0;
        while (index < value.Length)
        {
            Int32 int32 = Convert.ToInt32(Char.ToString(value[index + 1]), 16);
            Char ch = Convert.ToChar((Convert.ToInt32(Char.ToString(value[index]), 16) << 4) + int32);
            stringBuilder.Append(ch);
            index += 2;
        }

        return stringBuilder.ToString();
    }

    public static Byte[] ToBytes(String s)
    {
        Byte[] numArray = new Byte[s.Length];
        for (Int32 index = 0; index < numArray.Length; ++index)
        {
            numArray[index] = (Byte)s[index];
        }
        return numArray;
    }

    public static String FromBytes(Byte[] buffer)
    {
        Char[] chArray = new Char[buffer.Length];
        for (Int32 index = 0; index < buffer.Length; ++index)
        {
            chArray[index] = (Char)buffer[index];
        }
        return new String(chArray);
    }

    public static Byte[] SerizalizeString(String value)
    {
        Int32 length = value.Length;
        Byte[] numArray = new Byte[length * 2];
        Buffer.BlockCopy(value.ToCharArray(), 0, numArray, 0, length * 2);
        return numArray;
    }

    public static String DeserizalizeString(Stream stream, Int32 dataLength, Byte[] serializedData)
    {
        Char[] chArray = new Char[dataLength / 2];
        stream.Read(serializedData, 0, dataLength);
        Buffer.BlockCopy(serializedData, 0, chArray, 0, dataLength);
        return new String(chArray);
    }

    /// <summary>
    /// Find substring that is arounded with spaces or limiters.
    /// Find 'whole' word
    /// </summary>
    /// <param name="target">whole string</param>
    /// <param name="value">substring</param>
    /// <returns></returns>
    public static Int32 IndexOfWholeWord(String target, String value)
    {
        Int32 index;
        for (Int32 startIndex = 0;
            startIndex < target.Length && (index = target.IndexOf(value, startIndex)) != -1;
            startIndex = index + 1)
        {
            Boolean correctStart = true;
            if (index > 0)
            {
                correctStart = !Char.IsLetterOrDigit(target[index - 1]);
            }

            Boolean correctFinish = true;
            if (index + value.Length < target.Length)
            {
                correctFinish = !Char.IsLetterOrDigit(target[index + value.Length]);
            }

            if (correctStart & correctFinish)
            {
                return index;
            }
        }

        return -1;
    }

    /// <summary>
    /// Find 'whole' world
    /// </summary>
    /// <param name="target">text</param>
    /// <param name="value">searched word</param>
    /// <param name="separators">allowed limiters</param>
    /// <returns></returns>
    public static Int32 IndexOfWholeWord(String target, String value, Char[] separators)
    {
        Int32 index;
        for (Int32 startIndex = 0;
            startIndex < target.Length && (index = target.IndexOf(value, startIndex)) != -1;
            startIndex = index + 1)
        {
            Boolean correctStart = true;
            if (index > 0)
            {
                correctStart = separators.Contains(target[index - 1]);
            }

            Boolean correctFinish = true;
            if (index + value.Length < target.Length)
            {
                correctFinish = separators.Contains(target[index + value.Length]);
            }

            if (correctStart & correctFinish)
            {
                return index;
            }
        }

        return -1;
    }

    /// <summary>
    /// Repeat a char a few times
    /// ('a',5 -> "aaaaa" )
    /// </summary>
    /// <param name="ch">repeated char</param>
    /// <param name="capacity">how many times to repeat this char</param>
    /// <returns></returns>
    public static String Replicate(Char ch, Int32 capacity)
    {
        StringBuilder stringBuilder = new StringBuilder(capacity);
        for (Int32 index = 0; index < capacity; ++index)
        {
            stringBuilder.Append(ch);
        }
        return stringBuilder.ToString();
    }

    /// <summary>
    /// Returns all the collocations based on separator set and max number of words.
    /// </summary>
    /// <remarks>   
    /// For "Quick fox jumps over lazy dog" with limitations 2 and a single whitespace separator
    /// the output is: 
    /// [Quick; fox; jumps; over; lazy; dog; Quick fox; fox jumps; jumps over; over lazy; lazy dog]
    /// </remarks>
    /// <param name="text">Input data.</param>
    /// <param name="maxWordCount">Maximum count of words in a collocation.</param>
    /// <param name="separators">Set of separators to apply.</param>
    /// <param name="includeFullText">Says whether to add the input string to the output set.</param>
    /// <returns>Array of collocations.</returns>
    public static String[] GetCombinedWords(String text, Int32 maxWordCount, ref Char[] separators,
        Boolean includeFullText)
    {
        if (separators == null || separators.Length == 0)
        {
            return new String[] { text };
        }

        String[] words = text.Split(separators, StringSplitOptions.RemoveEmptyEntries);

        if (words.Length <= 1)
        {
            return words;
        }

        Int32 finalLength = words.Length;

        for (Int32 collocationLength = 2; collocationLength <= maxWordCount; ++collocationLength)
        {
            if (collocationLength <= words.Length)
            {
                finalLength += words.Length - collocationLength + 1;
            }
        }

        if (includeFullText && words.Length > maxWordCount)
        {
            ++finalLength;
        }

        String[] combinedWords = new String[finalLength];

        if (includeFullText)
        {
            combinedWords[finalLength - 1] = text;
        }

        Array.Copy(words, combinedWords, words.Length);

        Int32 emptyIndex = words.Length;

        for (Int32 collocationLength = 2; collocationLength <= maxWordCount; ++collocationLength)
        {
            for (Int32 firstWord = 0; firstWord <= words.Length - collocationLength; ++firstWord)
            {
                StringBuilder collocation = new StringBuilder();

                for (Int32 wordInCollocation = 0; wordInCollocation < collocationLength; ++wordInCollocation)
                {
                    collocation.Append(words[firstWord + wordInCollocation]);

                    if (wordInCollocation < collocationLength - 1)
                    {
                        collocation.Append(" ");
                    }
                }

                combinedWords[emptyIndex] = collocation.ToString();
                ++emptyIndex;
            }
        }

        return combinedWords;
    }

    /// <summary>
    /// Parse USA date-time format string into DateTime
    /// </summary>
    /// <param name="dateStr"></param>
    /// <returns></returns>
    public static DateTime ConvertToDateTimeEnUs(String dateStr)
    {
        CultureInfo cultureInfo = new CultureInfo("en-US");
        return Convert.ToDateTime(dateStr, cultureInfo);
    }

    public static String RemoveLineBreaks(this String source)
    {
        source = source.Replace("\r\n", " ").Replace("\r", " ").Replace("\n", " ");
        return source;
    }
    /// <summary>
    /// Read all lines from file. 
    /// </summary>
    /// <remarks>
    /// This method break lines only with limiter '\r\n'. Standard method can use also '\r'
    /// </remarks>
    /// <param name="fn">filename</param>
    /// <returns>array of lines</returns>
    public static string[] ReadFile(string fn)
    {
        List<string> res = new List<string>();
        string newline = "\r\n";

        try
        {
            using (StreamReader stream = new StreamReader(PathValidator.Simple.SanitizePath(fn)))
            {
                int bufferSize = 4096;
                int cnt = 0;
                int newlinePointer = 0;
                char[] buffer = new char[bufferSize];

                StringBuilder sb = new StringBuilder();
                while ((cnt = stream.Read(buffer, 0, bufferSize)) != 0)
                {
                    for (int i = 0; i < cnt; i++)
                    {
                        sb.Append(buffer[i]);
                        if (buffer[i] == newline[newlinePointer])
                        {
                            if (newlinePointer == newline.Length - 1)
                            {
                                string str = sb.Remove(sb.Length - newline.Length, newline.Length).ToString();
                                res.Add(str);
                                newlinePointer = 0;
                                sb = new StringBuilder();
                            }
                            else
                                newlinePointer++;
                        }
                        else
                            newlinePointer = 0;
                    }
                }
                res.Add(sb.ToString());
            }
        }
        catch (FileNotFoundException)
        {
            return new String[0];
        }

        return res.ToArray();
    }
}