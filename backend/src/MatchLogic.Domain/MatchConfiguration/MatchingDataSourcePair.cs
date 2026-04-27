using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.MatchConfiguration
{
    public class MatchingDataSourcePair : IEntity,IEquatable<MatchingDataSourcePair> 
    {
        public Guid DataSourceA { get; private set; }
        public Guid DataSourceB { get; private set; }

        public MatchingDataSourcePair(Guid dataSourceA, Guid dataSourceB)
        {
            DataSourceA = dataSourceA;
            DataSourceB = dataSourceB;
        }
        
        public override String ToString() => $"[{DataSourceA}, {DataSourceB}]";
        
        public Boolean Equals(MatchingDataSourcePair other)
        {
            if (other == null)
            {
                return false;
            }

            return (this.DataSourceA.Equals(other.DataSourceA) && this.DataSourceB.Equals(other.DataSourceB))
                   || (this.DataSourceB.Equals(other.DataSourceA) && this.DataSourceA.Equals(other.DataSourceB));
        }
    }
}
