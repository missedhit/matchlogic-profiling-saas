using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.MatchConfiguration
{
    public class MatchingDataSourcePairs: AuditableEntity
    {
        public MatchingDataSourcePairs()
        {
            
        }
        public Guid ProjectId { get; set; }

        private List<MatchingDataSourcePair> _pairs = new List<MatchingDataSourcePair>();
        public MatchingDataSourcePairs(List<MatchingDataSourcePair> pairs)
        {
            _pairs = pairs;
        }
        public List<MatchingDataSourcePair> Pairs {
            get
            {
                return _pairs;
            } 
            set 
            {
                _pairs = value;
            } 
        }
        public MatchingDataSourcePair this[Int32 index] => _pairs[index];
        public Int32 Count => _pairs.Count;
        public Boolean Add(Guid dataSourceA, Guid dataSourceB)
        {
            Boolean isAdded = false;

            if (!Contains(dataSourceA, dataSourceB))
            {
                _pairs.Add(new MatchingDataSourcePair(dataSourceA, dataSourceB));
                isAdded = true;
            }

            return isAdded;
        }
        public Boolean Remove(Guid dataSourceA, Guid dataSourceB)
        {
            MatchingDataSourcePair pairToRemove = new MatchingDataSourcePair(dataSourceA, dataSourceB);

            return _pairs.RemoveAll(pair => pair.Equals(pairToRemove)) > 0;
        }

        public void Clear()
        {
            _pairs.Clear();
        }
        
        public Boolean Contains(Guid dataSourceA, Guid dataSourceB)
        {
            MatchingDataSourcePair pairToSeek = new MatchingDataSourcePair(dataSourceA, dataSourceB);

            return _pairs.Contains(pairToSeek);
        }
        
        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();

            for (Int32 i = 0; i < Count; i++)
            {
                sb.AppendLine(_pairs[i].ToString());
            }

            return sb.ToString();
        }
    }
}
