using MatchLogic.Application.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Persistence;
/// <summary>
/// Resolves which StoreType a given repository class should use.
///
/// Resolution priority (highest → lowest):
///   1. StoreSettings:Overrides  in appsettings (by short class name)
///   2. [UseStore] attribute on the repository class
///   3. StoreSettings:Default    in appsettings
///   4. StoreType.MongoDB        (hardcoded last-resort fallback)
/// </summary>
public interface IStoreTypeResolver
{
    StoreType Resolve(Type repositoryType);
}
