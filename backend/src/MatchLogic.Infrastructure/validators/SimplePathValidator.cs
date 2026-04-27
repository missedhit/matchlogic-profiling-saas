using MatchLogic.EncryptDecrypt;

namespace MatchLogic.Core.Security;

public class SimplePathValidator : PathValidator
{
    internal SimplePathValidator() { }

    public override string SanitizePath(string path)
    {
        string pathEncrypted = EncryptDecryptString.Encrypt(path);

        return EncryptDecryptString.Decrypt(pathEncrypted);
    }
}
