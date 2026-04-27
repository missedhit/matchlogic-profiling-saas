using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchResult.Get;
public class MatchResultResponse
{
    public Guid JobId { get; set; }
    public Guid Id { get; set; }
    public string Name { get; set; }

    public List<MatchCriteriaResponse> Criteria { get; set; } = new List<MatchCriteriaResponse>();
}
public class MatchCriteriaResponse
{
    public string FieldName { get; set; }
    //public bool Include { get; set; }
    public MatchingType MatchingType { get; set; }
    public CriteriaDataType DataType { get; set; }
    //public double Weight { get; set; }
    public Dictionary<ArgsValue, string> Arguments { get; set; }
}