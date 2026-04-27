using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Core.Security;

public abstract class PathValidator
{
    public static SimplePathValidator Simple = new SimplePathValidator();

    public abstract string SanitizePath(string path);
}
