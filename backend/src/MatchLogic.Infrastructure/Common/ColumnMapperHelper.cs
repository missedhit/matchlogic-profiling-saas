using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Common;
public static class ColumnMapperHelper
{

    /// <summary>  
    /// Maps columns from a source dictionary to a target dictionary by matching keys.  
    /// If a key in the source dictionary exists in the target dictionary,  
    /// the corresponding value from the target dictionary is added to the result.  
    /// </summary>  
    /// <param name="sourceColumns">The source dictionary containing column mappings.</param>  
    /// <param name="targetColumns">The target dictionary containing column mappings.</param>  
    /// <returns>A dictionary containing the mapped columns where keys match between source and target.</returns>  
    public static Dictionary<string, string> MapColumns(Dictionary<string, string> sourceColumns, Dictionary<string, string> targetColumns)
    {
        var mappedColumns = new Dictionary<string, string>();
        foreach (var sourceColumn in sourceColumns)
        {
            if (targetColumns.TryGetValue(sourceColumn.Key, out var targetColumn))
            {
                mappedColumns[sourceColumn.Key] = targetColumn;
            }
        }
        return mappedColumns;
    }
       
    ///<summary>
    /// Handles duplicate column headers by appending a numeric suffix to the header if it already exists in the dictionary.
    /// If the header is encountered for the first time, it is added to the dictionary with an initial count of 0.
    /// </summary>
    /// <param name="header">The column header to process.</param>
    /// <param name="sameColumns">A reference to a dictionary that tracks the count of duplicate headers.</param>
    /// <returns>The processed header, with a numeric suffix appended if it is a duplicate.</returns>
    public static string HandleDuplicateHeaders(string header, ref Dictionary<string, int> sameColumns)
    {
        string headerResult = header;
        if (sameColumns.TryGetValue(header, out int count))
        {
            count++;
            sameColumns[header] = count;
            headerResult = $"{header}_{count}";
        }
        else
        {
            sameColumns[header] = 0;
        }
        return headerResult;
    }
}
