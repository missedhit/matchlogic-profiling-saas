using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace MatchLogic.EncryptDecrypt;

public class EncryptDecryptString
{
    private static String PassPhrase => "this is any string";

    private static String SaltValue => "@ny str1ng t00";

    private static String InitVector => "0$^cQ!,+9J`kp0*&";

    private static String HashAlgorithm => "SHA1";

    private static Int32 PasswordIterations => 2;

    private static Int32 KeySize => 256;

    public static Byte[] Encrypt(Byte[] plainBytes, String passPhrase, String saltValue, String hashAlgorithm,
        Int32 passwordIterations, String initVector, Int32 keySize)
    {
        Byte[] bytes1 = Encoding.ASCII.GetBytes(initVector);
        Byte[] bytes2 = Encoding.ASCII.GetBytes(saltValue);
        Byte[] bytes3 = new PasswordDeriveBytes(passPhrase, bytes2, hashAlgorithm, passwordIterations).GetBytes(keySize / 8);

        AesCryptoServiceProvider cryptoServiceProvider = new AesCryptoServiceProvider
        {
            Mode = CipherMode.CBC
        };

        ICryptoTransform encryptor = cryptoServiceProvider.CreateEncryptor(bytes3, bytes1);

        using (MemoryStream memoryStream = new MemoryStream())
        using (CryptoStream cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
        {
            cryptoStream.Write(plainBytes, 0, plainBytes.Length);
            cryptoStream.FlushFinalBlock();
            Byte[] array = memoryStream.ToArray();

            return array;
        }
    }

    public static String Encrypt(String plainText, String passPhrase, String saltValue, String hashAlgorithm,
        Int32 passwordIterations, String initVector, Int32 keySize)
    {
        Byte[] numArray = Encrypt(Encoding.UTF8.GetBytes(plainText), passPhrase, saltValue, hashAlgorithm,
            passwordIterations, initVector, keySize);

        StringBuilder stringBuilder = new StringBuilder(numArray.Length);

        for (Int32 i = 0; i < numArray.Length; ++i)
        {
            stringBuilder.Append((Char)numArray[i]);
        }

        return stringBuilder.ToString();
    }

    public static Byte[] Decrypt(Byte[] cipherBytes, String passPhrase, String saltValue, String hashAlgorithm,
        Int32 passwordIterations, String initVector, Int32 keySize)
    {
        if (cipherBytes.Length == 0)
        {
            return new Byte[0];
        }

        Byte[] bytes1 = Encoding.ASCII.GetBytes(initVector);
        Byte[] bytes2 = Encoding.ASCII.GetBytes(saltValue);
        Byte[] bytes3 =
            new PasswordDeriveBytes(passPhrase, bytes2, hashAlgorithm, passwordIterations).GetBytes(keySize / 8);

        AesCryptoServiceProvider cryptoServiceProvider = new AesCryptoServiceProvider
        {
            Mode = CipherMode.CBC
        };

        ICryptoTransform decryptor = cryptoServiceProvider.CreateDecryptor(bytes3, bytes1);

        using (MemoryStream memoryStream = new MemoryStream(cipherBytes))
        using (CryptoStream cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read))
        {
            Byte[] buffer = new Byte[cipherBytes.Length];
            cryptoStream.Read(buffer, 0, buffer.Length);

            return buffer;
        }
    }

    public static String Decrypt(String cipherText, String passPhrase, String saltValue, String hashAlgorithm,
        Int32 passwordIterations, String initVector, Int32 keySize)
    {
        Byte[] cipherBytes = new Byte[cipherText.Length];

        for (Int32 index = 0; index < cipherText.Length; ++index)
        {
            cipherBytes[index] = (Byte)cipherText[index];
        }

        Byte[] bytes = Decrypt(cipherBytes, passPhrase, saltValue, hashAlgorithm, passwordIterations, initVector,
            keySize);
        Int32 count = bytes.Length;

        for (Int32 index = 0; index < bytes.Length; ++index)
        {
            if (bytes[index] == 0)
            {
                count = index;

                break;
            }
        }

        return Encoding.UTF8.GetString(bytes, 0, count);
    }

    public static MemoryStream EncryptStream(Stream inputStream)
    {
        Byte[] numArray = new Byte[inputStream.Length];
        inputStream.Position = 0L;
        inputStream.Read(numArray, 0, numArray.Length);

        return new MemoryStream(Encrypt(numArray));
    }

    public static MemoryStream DecryptStream(Stream inputStream)
    {
        Byte[] numArray = new Byte[inputStream.Length];
        inputStream.Position = 0L;
        inputStream.Read(numArray, 0, numArray.Length);

        return new MemoryStream(Decrypt(numArray));
    }

    public static Byte[] Encrypt(Byte[] plainBytes)
    {
        return Encrypt(plainBytes, PassPhrase, SaltValue, HashAlgorithm, PasswordIterations, InitVector, KeySize);
    }

    public static String Encrypt(String plainText)
    {
        return Encrypt(plainText, PassPhrase, SaltValue, HashAlgorithm, PasswordIterations, InitVector, KeySize);
    }

    private static String smethod_0(String string_0)
    {
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(string_0));
    }

    private static String smethod_1(String string_0)
    {
        return Encoding.UTF8.GetString(Convert.FromBase64String(string_0));
    }

    public static String EncryptToBase64(String plainText)
    {
        return smethod_0(Encrypt(plainText, PassPhrase, SaltValue, HashAlgorithm, PasswordIterations, InitVector,
            KeySize));
    }

    public static String DecryptFromBase64(String plainText)
    {
        return Decrypt(smethod_1(plainText), PassPhrase, SaltValue, HashAlgorithm, PasswordIterations, InitVector,
                KeySize).Replace("\0", "").Replace("\\n", "\n").Replace("\\r", "\r").Replace("\\\"", "\"")
            .Replace("\\a", "\a").Replace("\\b", "\b").Replace("\\t", "\t").Replace("\\r", "\r")
            .Replace("\\f", "\f");
    }

    public static Byte[] Decrypt(Byte[] cipherBytes)
    {
        return Decrypt(cipherBytes, PassPhrase, SaltValue, HashAlgorithm, PasswordIterations, InitVector, KeySize);
    }

    public static String Decrypt(String cipherText)
    {
        return Decrypt(cipherText, PassPhrase, SaltValue, HashAlgorithm, PasswordIterations, InitVector, KeySize);
    }

    public static String EncryptXOR(String inString, Int32 startKey, Int32 multKey, Int32 addKey)
    {
        return StringHelper.StringHelper.FromBytes(EncryptXOR(StringHelper.StringHelper.ToBytes(inString), startKey,
            multKey, addKey));
    }

    public static Byte[] EncryptXOR(Byte[] inBytes, Int32 startKey, Int32 multKey, Int32 addKey)
    {
        Byte[] numArray = new Byte[inBytes.Length];

        for (Int32 index = 0; index < inBytes.Length; ++index)
        {
            numArray[index] = (Byte)(inBytes[index] ^ (UInt32)(startKey >> 8));
            startKey = (numArray[index] + startKey) * multKey + addKey;
        }

        return numArray;
    }

    public static String DecryptXOR(String inString, Int32 startKey, Int32 multKey, Int32 addKey)
    {
        return StringHelper.StringHelper.FromBytes(DecryptXOR(StringHelper.StringHelper.ToBytes(inString), startKey,
            multKey, addKey));
    }

    public static Byte[] DecryptXOR(Byte[] inBytes, Int32 startKey, Int32 multKey, Int32 addKey)
    {
        Byte[] numArray = new Byte[inBytes.Length];

        for (Int32 index = 0; index < inBytes.Length; ++index)
        {
            numArray[index] = (Byte)(inBytes[index] ^ (UInt32)(startKey >> 8));
            startKey = (inBytes[index] + startKey) * multKey + addKey;
        }

        return numArray;
    }
}