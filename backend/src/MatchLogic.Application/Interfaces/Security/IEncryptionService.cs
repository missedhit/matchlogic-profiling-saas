using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Security
{
    public interface IEncryptionService
    {
        Task<string> EncryptAsync(string plainText, string keyId = null);
        Task<string> DecryptAsync(string encryptedText, string keyId = null);
        string GenerateKeyId();
    }
}