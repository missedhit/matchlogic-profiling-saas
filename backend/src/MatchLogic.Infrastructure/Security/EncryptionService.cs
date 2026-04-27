using MatchLogic.Application.Interfaces.Security;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Security
{
    public class EncryptionService : IEncryptionService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<EncryptionService> _logger;
        private readonly string _masterKey;

        public EncryptionService(IConfiguration configuration, ILogger<EncryptionService> logger)
        {
            _configuration = configuration;
            _logger = logger;
            _masterKey = _configuration["Security:MasterKey"] ?? throw new InvalidOperationException("Master key not configured");
            
            if(string.IsNullOrWhiteSpace(_masterKey)) throw new InvalidOperationException("Master key cannot be empty or null");
        }

        public async Task<string> EncryptAsync(string plainText, string keyId = null)
        {
            if (string.IsNullOrEmpty(plainText))
                return plainText;

            try
            {
                using var aes = Aes.Create();
                var key = DeriveKey(_masterKey, keyId ?? "default");
                aes.Key = key;
                aes.GenerateIV();

                using var encryptor = aes.CreateEncryptor();
                using var msEncrypt = new MemoryStream();
                using var csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write);
                using var swEncrypt = new StreamWriter(csEncrypt);

                await swEncrypt.WriteAsync(plainText);
                swEncrypt.Close();

                var iv = aes.IV;
                var encryptedContent = msEncrypt.ToArray();
                var result = new byte[iv.Length + encryptedContent.Length];

                Buffer.BlockCopy(iv, 0, result, 0, iv.Length);
                Buffer.BlockCopy(encryptedContent, 0, result, iv.Length, encryptedContent.Length);

                return Convert.ToBase64String(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error encrypting data");
                throw;
            }
        }
        
        public async Task<string> DecryptAsync(string encryptedText, string keyId = null)
        {
            if (string.IsNullOrEmpty(encryptedText))
                return encryptedText;
            try
            {
                var fullCipher = Convert.FromBase64String(encryptedText);

                using var aes = Aes.Create();
                var key = DeriveKey(_masterKey, keyId ?? "default");
                aes.Key = key;

                var iv = new byte[aes.BlockSize / 8];
                var cipher = new byte[fullCipher.Length - iv.Length];

                Buffer.BlockCopy(fullCipher, 0, iv, 0, iv.Length);
                Buffer.BlockCopy(fullCipher, iv.Length, cipher, 0, cipher.Length);

                aes.IV = iv;

                using var decryptor = aes.CreateDecryptor();
                using var msDecrypt = new MemoryStream(cipher);
                using var csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read);
                using var srDecrypt = new StreamReader(csDecrypt);

                return await srDecrypt.ReadToEndAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error decrypting data");
                throw;
            }
        }
        private bool IsBase64String(string s)
        {
            if (string.IsNullOrEmpty(s) || s.Length % 4 != 0)
                return false;
            Span<byte> buffer = stackalloc byte[s.Length];
            return Convert.TryFromBase64String(s, buffer, out _);
        }
        public string GenerateKeyId()
        {
            return Guid.NewGuid().ToString("N")[..16];
        }

        private static byte[] DeriveKey(string masterKey, string keyId)
        {
            using var rfc2898 = new Rfc2898DeriveBytes(masterKey, Encoding.UTF8.GetBytes(keyId), 10000, HashAlgorithmName.SHA256);
            return rfc2898.GetBytes(32); // 256-bit key
        }
    }
}