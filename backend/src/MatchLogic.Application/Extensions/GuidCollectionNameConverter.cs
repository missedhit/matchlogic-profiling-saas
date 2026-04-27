using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Extensions;
public static class GuidCollectionNameConverter
{
    public static string ToValidCollectionName(Guid guid)
    {
        // Convert GUID to a base64 string
        string base64 = Convert.ToBase64String(guid.ToByteArray());

        // Remove any non-alphanumeric characters
        string alphanumeric = new string(base64.Where(c => char.IsLetterOrDigit(c)).ToArray());

        // Ensure the name starts with a letter (LiteDB requirement)
        if (char.IsDigit(alphanumeric[0]))
        {
            alphanumeric = "C" + alphanumeric;
        }

        // Truncate to 128 characters if necessary (LiteDB limit)
        if (alphanumeric.Length > 128)
        {
            alphanumeric = alphanumeric.Substring(0, 128);
        }

        return alphanumeric;
    }
}
