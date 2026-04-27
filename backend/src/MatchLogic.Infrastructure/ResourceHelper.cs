using MatchLogic.Compression;
using MatchLogic.EncryptDecrypt;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure;

internal class ResourceHelper
{
    /// <summary>
    /// Loads DataTable from resources.
    /// </summary>
    /// <param name="table"></param>
    /// <param name="resourcesName"></param>
    /// <returns></returns>
    internal static Boolean LoadTableFromResources(DataTable table, String resourcesName)
    {

        var assembly = System.Reflection.Assembly.GetExecutingAssembly();

        string[] resources = assembly.GetManifestResourceNames();
        if (!resources.Contains(resourcesName))
        {
            return false;
        }

        Stream encryptedCompressedStream = assembly.GetManifestResourceStream(resourcesName);
        MemoryStream decryptedCompressedStream = EncryptDecryptString.DecryptStream(encryptedCompressedStream);
        MemoryStream decompressedStream = CompressWrapper.Decompress(decryptedCompressedStream);
        encryptedCompressedStream.Close();
        table.ReadXml(decompressedStream);

        return true;
    }

    internal static Boolean LoadTableFromResourcesXml(DataTable table, String resourcesName)
    {
        var assembly = System.Reflection.Assembly.GetExecutingAssembly();
        string[] resources = assembly.GetManifestResourceNames();
        if (!resources.Contains(resourcesName))
        {
            return false;
        }
        Stream xmlStream = assembly.GetManifestResourceStream(resourcesName);
        table.ReadXml(xmlStream);
        xmlStream.Close();
        return true;
    }
}
