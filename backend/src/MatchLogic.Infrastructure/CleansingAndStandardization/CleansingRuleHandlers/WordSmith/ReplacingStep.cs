using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;

/// <summary>
/// because of priority replacing is done from the highest (0) priority and
/// steps are cached to this class and then sorted by priority...
/// </summary>
//public class ReplacingStep
//{
//    public String Words = String.Empty;
//    public String Replacement = String.Empty;
//    public String NewColumnName = String.Empty;
//    public Int32 Priority = 0;
//    public Boolean ToDelete = false;

//    public ReplacingStep Clone()
//    {
//        return (ReplacingStep)MemberwiseClone();
//    }
//}
